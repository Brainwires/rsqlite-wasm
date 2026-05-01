use rsqlite_storage::btree::{BTreeCursor, CursorRow};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::literal_to_value;
use crate::planner::{ColumnRef, PlanExpr, ProjectionItem, UnaryOp};
use crate::types::QueryResult;

pub(super) fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => false,
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Real(x), Value::Real(y)) => x == y,
        (Value::Integer(x), Value::Real(y)) => (*x as f64) == *y,
        (Value::Real(x), Value::Integer(y)) => *x == (*y as f64),
        (Value::Text(x), Value::Text(y)) => x == y,
        (Value::Blob(x), Value::Blob(y)) => x == y,
        _ => false,
    }
}

pub(super) fn value_to_sql_literal(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Blob(b) => format!(
            "X'{}'",
            b.iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
    }
}

pub(super) fn map_query_row_to_insert(
    query_values: &[Value],
    table_columns: &[ColumnRef],
    target_columns: &Option<Vec<String>>,
) -> Result<(Vec<Value>, Vec<bool>)> {
    let num_table_cols = table_columns.len();
    let mut values = vec![Value::Null; num_table_cols];
    let mut explicitly_set = vec![false; num_table_cols];

    if let Some(targets) = target_columns {
        for (i, col_name) in targets.iter().enumerate() {
            let idx = table_columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| Error::Other(format!("unknown column: {col_name}")))?;
            values[idx] = query_values.get(i).cloned().unwrap_or(Value::Null);
            explicitly_set[idx] = true;
        }
    } else {
        for (i, val) in query_values.iter().enumerate() {
            if i < num_table_cols {
                values[i] = val.clone();
                explicitly_set[i] = true;
            }
        }
    }

    Ok((values, explicitly_set))
}

pub(super) fn read_row_by_rowid(
    pager: &mut Pager,
    root_page: u32,
    target_rowid: i64,
    table_columns: &[ColumnRef],
) -> Result<Vec<Value>> {
    let mut cursor = BTreeCursor::new(pager, root_page);
    let mut has_row = cursor.first().map_err(|e| Error::Other(e.to_string()))?;
    while has_row {
        let row = cursor.current().map_err(|e| Error::Other(e.to_string()))?;
        if row.rowid == target_rowid {
            let mut values = Vec::with_capacity(table_columns.len());
            for col in table_columns {
                if col.is_rowid_alias {
                    values.push(Value::Integer(row.rowid));
                } else {
                    values.push(
                        row.record
                            .values
                            .get(col.column_index)
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                }
            }
            return Ok(values);
        }
        if row.rowid > target_rowid {
            break;
        }
        has_row = cursor.next().map_err(|e| Error::Other(e.to_string()))?;
    }
    Err(Error::Other(format!("row not found: rowid={target_rowid}")))
}

pub(super) fn row_values_for_rowid(
    btree_rows: &[CursorRow],
    rowid: i64,
    table_columns: &[ColumnRef],
) -> Vec<Value> {
    for row in btree_rows {
        if row.rowid == rowid {
            let mut values = Vec::with_capacity(table_columns.len());
            for col in table_columns {
                if col.is_rowid_alias {
                    values.push(Value::Integer(row.rowid));
                } else {
                    values.push(
                        row.record
                            .values
                            .get(col.column_index)
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                }
            }
            return values;
        }
    }
    vec![Value::Null; table_columns.len()]
}

pub(super) fn get_table_indexes(catalog: &Catalog, table_name: &str) -> Vec<(u32, Vec<usize>)> {
    get_table_indexes_with_predicates(catalog, table_name)
        .into_iter()
        .map(|(root, cols, _)| (root, cols))
        .collect()
}

/// Like `get_table_indexes` but also returns each index's WHERE predicate
/// (for partial indexes). The predicate is the source-form WHERE clause;
/// callers re-parse it to skip rows that don't match.
pub(super) fn get_table_indexes_with_predicates(
    catalog: &Catalog,
    table_name: &str,
) -> Vec<(u32, Vec<usize>, Option<String>)> {
    let table_def = match catalog.get_table(table_name) {
        Some(t) => t,
        None => return vec![],
    };

    catalog
        .indexes
        .values()
        .filter(|idx| idx.table_name.eq_ignore_ascii_case(table_name) && !idx.columns.is_empty())
        .filter_map(|idx| {
            let col_indices: Vec<usize> = idx
                .columns
                .iter()
                .filter_map(|col_name| {
                    table_def
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(col_name))
                })
                .collect();
            if col_indices.len() == idx.columns.len() {
                Some((idx.root_page, col_indices, idx.predicate.clone()))
            } else {
                None
            }
        })
        .collect()
}

/// Evaluate a partial-index predicate (in source form) against a row of
/// values from the table. Returns true if the predicate is missing or
/// truthy — i.e. the row belongs in the index.
pub(super) fn index_predicate_matches(
    predicate: Option<&str>,
    values: &[Value],
    table_columns: &[ColumnRef],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<bool> {
    let s = match predicate {
        None => return Ok(true),
        Some(s) => s,
    };
    let parsed = match rsqlite_parser::parse::parse_sql(&format!("SELECT 1 WHERE {s}")) {
        Ok(p) => p,
        Err(_) => return Ok(true),
    };
    let stmt = match parsed.into_iter().next() {
        Some(s) => s,
        None => return Ok(true),
    };
    let expr = match stmt {
        sqlparser::ast::Statement::Query(q) => match *q.body {
            sqlparser::ast::SetExpr::Select(sel) => sel.selection,
            _ => None,
        },
        _ => None,
    };
    let expr = match expr {
        Some(e) => e,
        None => return Ok(true),
    };
    let plan_expr = match crate::planner::plan_expr(&expr, table_columns, catalog) {
        Ok(pe) => pe,
        Err(_) => return Ok(true),
    };
    let row = crate::types::Row { values: values.to_vec(), rowid: None };
    let col_names: Vec<String> = table_columns.iter().map(|c| c.name.clone()).collect();
    let v = super::eval::eval_expr(&plan_expr, &row, &col_names, pager, catalog)?;
    Ok(crate::eval_helpers::is_truthy(&v))
}

pub(super) fn build_index_key(
    values: &[Value],
    col_indices: &[usize],
    table_columns: &[ColumnRef],
    rowid: i64,
) -> Record {
    let mut key_values: Vec<Value> = Vec::new();
    for &col_idx in col_indices {
        if table_columns[col_idx].is_rowid_alias {
            key_values.push(Value::Integer(rowid));
        } else {
            key_values.push(values.get(col_idx).cloned().unwrap_or(Value::Null));
        }
    }
    key_values.push(Value::Integer(rowid));
    Record { values: key_values }
}

pub(super) fn eval_insert_row(
    row_exprs: &[PlanExpr],
    table_columns: &[ColumnRef],
    target_columns: &Option<Vec<String>>,
) -> Result<(Vec<Value>, Vec<bool>)> {
    match target_columns {
        None => {
            let mut values = Vec::with_capacity(table_columns.len());
            let mut explicitly_set = Vec::with_capacity(table_columns.len());
            for (i, _col) in table_columns.iter().enumerate() {
                if i < row_exprs.len() {
                    values.push(eval_literal(&row_exprs[i])?);
                    explicitly_set.push(true);
                } else {
                    values.push(Value::Null);
                    explicitly_set.push(false);
                }
            }
            Ok((values, explicitly_set))
        }
        Some(target_cols) => {
            let mut values = vec![Value::Null; table_columns.len()];
            let mut explicitly_set = vec![false; table_columns.len()];
            for (i, target_name) in target_cols.iter().enumerate() {
                let col_idx = table_columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(target_name))
                    .ok_or_else(|| Error::Other(format!("unknown column: {target_name}")))?;
                if i < row_exprs.len() {
                    values[col_idx] = eval_literal(&row_exprs[i])?;
                    explicitly_set[col_idx] = true;
                }
            }
            Ok((values, explicitly_set))
        }
    }
}

/// Evaluate a list of RETURNING projections against each row in `affected`
/// and produce a QueryResult. Each row in `affected` must align column-wise
/// with `table_columns` (rowid alias columns should already carry the rowid).
pub(super) fn build_returning_result(
    returning: &[ProjectionItem],
    affected: &[Vec<crate::types::Value>],
    table_columns: &[ColumnRef],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let columns: Vec<String> = returning.iter().map(|p| p.alias.clone()).collect();
    let col_names: Vec<String> = table_columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::with_capacity(affected.len());
    for values in affected {
        let row = crate::types::Row { values: values.clone(), rowid: None };
        let mut out_values = Vec::with_capacity(returning.len());
        for proj in returning {
            let v = super::eval::eval_expr(&proj.expr, &row, &col_names, pager, catalog)?;
            out_values.push(v);
        }
        rows.push(crate::types::Row::new(out_values));
    }
    Ok(QueryResult { columns, rows })
}

/// Compute GENERATED ALWAYS AS (...) column values for the row. Generated
/// columns must not have been set explicitly — that would error before this
/// runs. Both STORED and VIRTUAL flavors are computed here; the VIRTUAL
/// distinction would only matter once we read directly from the btree
/// without re-deriving, which we don't yet do.
pub(super) fn apply_generated_columns(
    values: &mut [Value],
    table_name: &str,
    table_columns: &[ColumnRef],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    let table_def = match catalog.get_table(table_name) {
        Some(td) => td,
        None => return Ok(()),
    };
    let col_names: Vec<String> = table_columns.iter().map(|c| c.name.clone()).collect();

    for (i, col) in table_columns.iter().enumerate() {
        let cat_col = match table_def
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&col.name))
        {
            Some(c) => c,
            None => continue,
        };
        let gen_col = match &cat_col.generated {
            Some(g) => g,
            None => continue,
        };

        let parsed = match rsqlite_parser::parse::parse_sql(&format!("SELECT {}", gen_col.expr)) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let expr_ast = parsed.into_iter().next().and_then(|stmt| {
            if let sqlparser::ast::Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
                    sel.projection.into_iter().next().and_then(|item| {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(e) = item {
                            Some(e)
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            } else {
                None
            }
        });
        let expr_ast = match expr_ast {
            Some(e) => e,
            None => continue,
        };
        let plan_expr = match crate::planner::plan_expr(&expr_ast, table_columns, catalog) {
            Ok(pe) => pe,
            Err(_) => continue,
        };
        // Build a row from the values computed so far so the generated expr
        // can reference sibling columns. Generated columns reading other
        // generated columns aren't well-defined; SQLite forbids forward
        // references but here we just use whatever values are populated.
        let row = crate::types::Row { values: values.to_vec(), rowid: None };
        if let Ok(v) = super::eval::eval_expr(&plan_expr, &row, &col_names, pager, catalog) {
            values[i] = v;
        }
    }
    Ok(())
}

pub(super) fn apply_column_defaults(
    values: &mut [Value],
    explicitly_set: &[bool],
    table_name: &str,
    table_columns: &[ColumnRef],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    let table_def = match catalog.get_table(table_name) {
        Some(td) => td,
        None => return Ok(()),
    };

    let col_names: Vec<String> = table_columns.iter().map(|c| c.name.clone()).collect();
    let placeholder_row = crate::types::Row { values: values.to_vec(), rowid: None };

    for (i, col) in table_columns.iter().enumerate() {
        if explicitly_set[i] {
            continue;
        }
        let cat_col = match table_def
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&col.name))
        {
            Some(c) => c,
            None => continue,
        };
        let default_sql = match &cat_col.default_expr {
            Some(s) => s,
            None => continue,
        };

        let parsed = match rsqlite_parser::parse::parse_sql(&format!("SELECT {default_sql}")) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let expr_ast = parsed.into_iter().next().and_then(|stmt| {
            if let sqlparser::ast::Statement::Query(q) = stmt {
                if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
                    sel.projection.into_iter().next().and_then(|item| {
                        if let sqlparser::ast::SelectItem::UnnamedExpr(e) = item {
                            Some(e)
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            } else {
                None
            }
        });
        let expr_ast = match expr_ast {
            Some(e) => e,
            None => continue,
        };
        let plan_expr = match crate::planner::plan_expr(&expr_ast, table_columns, catalog) {
            Ok(pe) => pe,
            Err(_) => continue,
        };
        match super::eval::eval_expr(&plan_expr, &placeholder_row, &col_names, pager, catalog) {
            Ok(v) => values[i] = v,
            Err(_) => continue,
        }
    }
    Ok(())
}

pub(super) fn eval_literal(expr: &PlanExpr) -> Result<Value> {
    match expr {
        PlanExpr::Literal(lit) => Ok(literal_to_value(lit)),
        PlanExpr::Param(index) => Ok(super::state::get_param(*index)),
        PlanExpr::UnaryOp {
            op: UnaryOp::Neg,
            operand,
        } => {
            let v = eval_literal(operand)?;
            match v {
                Value::Integer(n) => Ok(Value::Integer(-n)),
                Value::Real(f) => Ok(Value::Real(-f)),
                _ => Ok(Value::Integer(0)),
            }
        }
        _ => Err(Error::Other(
            "only literal values are supported in INSERT".to_string(),
        )),
    }
}

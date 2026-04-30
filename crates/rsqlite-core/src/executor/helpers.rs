use rsqlite_storage::btree::{BTreeCursor, CursorRow};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::literal_to_value;
use crate::planner::{ColumnRef, PlanExpr, UnaryOp};

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
        Value::Blob(b) => format!("X'{}'", b.iter().map(|byte| format!("{byte:02x}")).collect::<String>()),
    }
}

pub(super) fn map_query_row_to_insert(
    query_values: &[Value],
    table_columns: &[ColumnRef],
    target_columns: &Option<Vec<String>>,
) -> Result<Vec<Value>> {
    let num_table_cols = table_columns.len();
    let mut values = vec![Value::Null; num_table_cols];

    if let Some(targets) = target_columns {
        for (i, col_name) in targets.iter().enumerate() {
            let idx = table_columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| Error::Other(format!("unknown column: {col_name}")))?;
            values[idx] = query_values.get(i).cloned().unwrap_or(Value::Null);
        }
    } else {
        for (i, val) in query_values.iter().enumerate() {
            if i < num_table_cols {
                values[i] = val.clone();
            }
        }
    }

    Ok(values)
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
                Some((idx.root_page, col_indices))
            } else {
                None
            }
        })
        .collect()
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
) -> Result<Vec<Value>> {
    match target_columns {
        None => {
            let mut values = Vec::with_capacity(table_columns.len());
            for (i, col) in table_columns.iter().enumerate() {
                if i < row_exprs.len() {
                    values.push(eval_literal(&row_exprs[i])?);
                } else if col.is_rowid_alias {
                    values.push(Value::Null);
                } else {
                    values.push(Value::Null);
                }
            }
            Ok(values)
        }
        Some(target_cols) => {
            let mut values = vec![Value::Null; table_columns.len()];
            for (i, target_name) in target_cols.iter().enumerate() {
                let col_idx = table_columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(target_name))
                    .ok_or_else(|| {
                        Error::Other(format!("unknown column: {target_name}"))
                    })?;
                if i < row_exprs.len() {
                    values[col_idx] = eval_literal(&row_exprs[i])?;
                }
            }
            Ok(values)
        }
    }
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

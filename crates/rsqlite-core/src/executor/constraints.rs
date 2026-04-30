use rsqlite_storage::btree::BTreeCursor;
use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::{compare, is_truthy};
use crate::planner::ColumnRef;
use crate::types::Row;

pub(super) fn check_check_constraints(
    values: &[Value],
    columns: &[ColumnRef],
    table_name: &str,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    let table_def = match catalog.get_table(table_name) {
        Some(td) => td,
        None => return Ok(()),
    };
    if table_def.check_constraints.is_empty() {
        return Ok(());
    }

    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let row = Row { values: values.to_vec() };

    for check_sql in &table_def.check_constraints {
        let parsed = rsqlite_parser::parse::parse_sql(&format!("SELECT {check_sql}"));
        let expr = match parsed {
            Ok(stmts) => {
                if let Some(sqlparser::ast::Statement::Query(q)) = stmts.into_iter().next() {
                    if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
                        if let Some(item) = sel.projection.into_iter().next() {
                            if let sqlparser::ast::SelectItem::UnnamedExpr(e) = item {
                                Some(e)
                            } else { None }
                        } else { None }
                    } else { None }
                } else { None }
            }
            Err(_) => None,
        };
        let expr = match expr {
            Some(e) => e,
            None => continue,
        };
        let plan_expr = match crate::planner::plan_expr(&expr, columns, catalog) {
            Ok(pe) => pe,
            Err(_) => continue,
        };
        let val = super::eval::eval_expr(&plan_expr, &row, &col_names, pager, catalog)?;
        if !is_truthy(&val) && !matches!(val, Value::Null) {
            return Err(Error::Other(format!(
                "CHECK constraint failed: {table_name}"
            )));
        }
    }
    Ok(())
}

pub(super) fn check_not_null_constraints(values: &[Value], columns: &[ColumnRef], table_name: &str) -> Result<()> {
    for (i, col) in columns.iter().enumerate() {
        if !col.nullable && !col.is_rowid_alias {
            if let Some(Value::Null) = values.get(i) {
                return Err(Error::Other(format!(
                    "NOT NULL constraint failed: {}.{}", table_name, col.name
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn check_unique_constraints(
    values: &[Value],
    columns: &[ColumnRef],
    table_name: &str,
    pager: &mut Pager,
    root_page: u32,
    exclude_rowid: Option<i64>,
) -> Result<()> {
    let unique_cols: Vec<(usize, &str)> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_unique && !c.is_rowid_alias)
        .map(|(i, c)| (i, c.name.as_str()))
        .collect();

    if unique_cols.is_empty() {
        return Ok(());
    }

    let mut cursor = BTreeCursor::new(pager, root_page);
    let rows = cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;

    for (col_idx, col_name) in &unique_cols {
        let new_val = &values[*col_idx];
        if matches!(new_val, Value::Null) {
            continue;
        }
        for row in &rows {
            if let Some(exclude) = exclude_rowid {
                if row.rowid == exclude {
                    continue;
                }
            }
            if let Some(existing) = row.record.values.get(*col_idx) {
                if compare(existing, new_val) == 0 {
                    return Err(Error::Other(format!(
                        "UNIQUE constraint failed: {}.{}", table_name, col_name
                    )));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn find_unique_conflict_rowid(
    values: &[Value],
    columns: &[ColumnRef],
    pager: &mut Pager,
    root_page: u32,
) -> Result<Option<i64>> {
    let unique_cols: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| (c.is_unique || c.is_primary_key) && !c.is_rowid_alias)
        .map(|(i, _)| i)
        .collect();

    if unique_cols.is_empty() {
        return Ok(None);
    }

    let mut cursor = BTreeCursor::new(pager, root_page);
    let rows = cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;

    for col_idx in &unique_cols {
        let new_val = &values[*col_idx];
        if matches!(new_val, Value::Null) {
            continue;
        }
        for row in &rows {
            if let Some(existing) = row.record.values.get(*col_idx) {
                if compare(existing, new_val) == 0 {
                    return Ok(Some(row.rowid));
                }
            }
        }
    }
    Ok(None)
}

pub(super) fn check_foreign_key_insert(
    values: &[Value],
    columns: &[ColumnRef],
    table_name: &str,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    if !super::state::foreign_keys_enabled() {
        return Ok(());
    }
    let table_def = match catalog.get_table(table_name) {
        Some(td) => td,
        None => return Ok(()),
    };
    if table_def.foreign_keys.is_empty() {
        return Ok(());
    }

    for fk in &table_def.foreign_keys {
        let fk_values: Vec<Value> = fk.from_columns.iter().map(|fc| {
            columns.iter().position(|c| c.name.eq_ignore_ascii_case(fc))
                .map(|i| values[i].clone())
                .unwrap_or(Value::Null)
        }).collect();

        if fk_values.iter().all(|v| matches!(v, Value::Null)) {
            continue;
        }

        let parent = match catalog.get_table(&fk.to_table) {
            Some(t) => t,
            None => return Err(Error::Other(format!("FOREIGN KEY constraint failed: parent table '{}' not found", fk.to_table))),
        };

        let parent_col_indices: Vec<usize> = fk.to_columns.iter().map(|tc| {
            parent.columns.iter().position(|c| c.name.eq_ignore_ascii_case(tc)).unwrap_or(0)
        }).collect();

        let mut cursor = BTreeCursor::new(pager, parent.root_page);
        let rows = cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;
        let mut found = false;
        for row in &rows {
            let match_all = parent_col_indices.iter().zip(fk_values.iter()).all(|(&ci, fk_val)| {
                let parent_col = &parent.columns[ci];
                let parent_val = if parent_col.is_rowid_alias {
                    Value::Integer(row.rowid)
                } else {
                    row.record.values.get(ci).cloned().unwrap_or(Value::Null)
                };
                super::helpers::values_equal(&parent_val, fk_val)
            });
            if match_all {
                found = true;
                break;
            }
        }
        if !found {
            return Err(Error::Other(format!(
                "FOREIGN KEY constraint failed: {}.({}) -> {}.({}){}",
                table_name,
                fk.from_columns.join(", "),
                fk.to_table,
                fk.to_columns.join(", "),
                fk_values.iter().map(|v| format!(" {v:?}")).collect::<String>(),
            )));
        }
    }
    Ok(())
}

pub(super) fn check_foreign_key_delete(
    deleted_rowid: i64,
    deleted_values: &[Value],
    table_name: &str,
    table_columns: &[crate::catalog::ColumnDef],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    if !super::state::foreign_keys_enabled() {
        return Ok(());
    }
    for child_table in catalog.all_tables() {
        for fk in &child_table.foreign_keys {
            if !fk.to_table.eq_ignore_ascii_case(table_name) {
                continue;
            }
            let parent_col_indices: Vec<usize> = fk.to_columns.iter().map(|tc| {
                table_columns.iter().position(|c| c.name.eq_ignore_ascii_case(tc)).unwrap_or(0)
            }).collect();

            let parent_vals: Vec<Value> = parent_col_indices.iter().map(|&ci| {
                let col = &table_columns[ci];
                if col.is_rowid_alias {
                    Value::Integer(deleted_rowid)
                } else {
                    deleted_values.get(ci).cloned().unwrap_or(Value::Null)
                }
            }).collect();

            let child_col_indices: Vec<usize> = fk.from_columns.iter().map(|fc| {
                child_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(fc)).unwrap_or(0)
            }).collect();

            let mut cursor = BTreeCursor::new(pager, child_table.root_page);
            let rows = cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;
            for row in &rows {
                let match_all = child_col_indices.iter().zip(parent_vals.iter()).all(|(&ci, pv)| {
                    let child_val = row.record.values.get(ci).cloned().unwrap_or(Value::Null);
                    super::helpers::values_equal(&child_val, pv)
                });
                if match_all {
                    return Err(Error::Other(format!(
                        "FOREIGN KEY constraint failed: cannot delete from '{}' — referenced by '{}'",
                        table_name, child_table.name
                    )));
                }
            }
        }
    }
    Ok(())
}

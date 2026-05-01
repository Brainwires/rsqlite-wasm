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
    let row = Row { values: values.to_vec(), rowid: None };

    for check_sql in &table_def.check_constraints {
        let parsed = rsqlite_parser::parse::parse_sql(&format!("SELECT {check_sql}"));
        let expr = match parsed {
            Ok(stmts) => {
                if let Some(sqlparser::ast::Statement::Query(q)) = stmts.into_iter().next() {
                    if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
                        if let Some(item) = sel.projection.into_iter().next() {
                            if let sqlparser::ast::SelectItem::UnnamedExpr(e) = item {
                                Some(e)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
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

pub(super) fn check_not_null_constraints(
    values: &[Value],
    columns: &[ColumnRef],
    table_name: &str,
) -> Result<()> {
    for (i, col) in columns.iter().enumerate() {
        if !col.nullable && !col.is_rowid_alias {
            if let Some(Value::Null) = values.get(i) {
                return Err(Error::Other(format!(
                    "NOT NULL constraint failed: {}.{}",
                    table_name, col.name
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
    catalog: &Catalog,
) -> Result<()> {
    let unique_cols: Vec<(usize, &str)> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_unique && !c.is_rowid_alias)
        .map(|(i, c)| (i, c.name.as_str()))
        .collect();

    // Composite PK indices (lowercased name → column position in `columns`).
    // Empty if the table has a single-column PK or no PK at all — those
    // cases are covered by the per-column `is_unique` check above.
    let composite_pk: Vec<(usize, &str)> = match catalog.get_table(table_name) {
        Some(td) if td.pk_columns.len() > 1 => td
            .pk_columns
            .iter()
            .filter_map(|name| {
                columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(name))
                    .map(|i| (i, columns[i].name.as_str()))
            })
            .collect(),
        _ => Vec::new(),
    };

    if unique_cols.is_empty() && composite_pk.is_empty() {
        return Ok(());
    }

    let mut cursor = BTreeCursor::new(pager, root_page);
    let rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

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
                        "UNIQUE constraint failed: {}.{}",
                        table_name, col_name
                    )));
                }
            }
        }
    }

    // Composite-PK tuple check: a row conflicts only when *every* PK
    // column matches simultaneously. Per SQLite semantics any NULL in
    // the new tuple disables the conflict (NULLs are distinct under
    // UNIQUE), matching the behavior of an SQLite composite UNIQUE
    // index over the same columns.
    if !composite_pk.is_empty() {
        let any_null = composite_pk
            .iter()
            .any(|(idx, _)| matches!(values[*idx], Value::Null));
        if !any_null {
            for row in &rows {
                if let Some(exclude) = exclude_rowid {
                    if row.rowid == exclude {
                        continue;
                    }
                }
                let all_match = composite_pk.iter().all(|(idx, _)| {
                    row.record
                        .values
                        .get(*idx)
                        .map(|existing| compare(existing, &values[*idx]) == 0)
                        .unwrap_or(false)
                });
                if all_match {
                    let names = composite_pk
                        .iter()
                        .map(|(_, name)| format!("{table_name}.{name}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(Error::Other(format!("UNIQUE constraint failed: {names}")));
                }
            }
        }
    }

    Ok(())
}

/// Find a row whose values for the given column names equal the values being
/// inserted. Returns the rowid of the conflicting row, or None.
pub(super) fn find_conflict_by_columns(
    values: &[Value],
    conflict_columns: &[String],
    columns: &[ColumnRef],
    pager: &mut Pager,
    root_page: u32,
) -> Result<Option<i64>> {
    let col_indices: Vec<usize> = conflict_columns
        .iter()
        .map(|name| {
            columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| Error::Other(format!("unknown conflict column: {name}")))
        })
        .collect::<Result<_>>()?;

    let mut cursor = BTreeCursor::new(pager, root_page);
    let rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

    for row in &rows {
        let all_match = col_indices.iter().all(|&ci| {
            let new_val = &values[ci];
            let existing = if columns[ci].is_rowid_alias {
                Value::Integer(row.rowid)
            } else {
                row.record.values.get(ci).cloned().unwrap_or(Value::Null)
            };
            // NULL never matches NULL for conflict purposes.
            if matches!(new_val, Value::Null) || matches!(existing, Value::Null) {
                false
            } else {
                compare(&existing, new_val) == 0
            }
        });
        if all_match {
            return Ok(Some(row.rowid));
        }
    }
    Ok(None)
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
    let rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

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
        let fk_values: Vec<Value> = fk
            .from_columns
            .iter()
            .map(|fc| {
                columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(fc))
                    .map(|i| values[i].clone())
                    .unwrap_or(Value::Null)
            })
            .collect();

        if fk_values.iter().all(|v| matches!(v, Value::Null)) {
            continue;
        }

        let parent = match catalog.get_table(&fk.to_table) {
            Some(t) => t,
            None => {
                return Err(Error::Other(format!(
                    "FOREIGN KEY constraint failed: parent table '{}' not found",
                    fk.to_table
                )));
            }
        };

        let parent_col_indices: Vec<usize> = fk
            .to_columns
            .iter()
            .map(|tc| {
                parent
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(tc))
                    .unwrap_or(0)
            })
            .collect();

        let mut cursor = BTreeCursor::new(pager, parent.root_page);
        let rows = cursor
            .collect_all()
            .map_err(|e| Error::Other(e.to_string()))?;
        let mut found = false;
        for row in &rows {
            let match_all = parent_col_indices
                .iter()
                .zip(fk_values.iter())
                .all(|(&ci, fk_val)| {
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
                fk_values
                    .iter()
                    .map(|v| format!(" {v:?}"))
                    .collect::<String>(),
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
    apply_foreign_key_delete_actions(
        deleted_rowid,
        deleted_values,
        table_name,
        table_columns,
        pager,
        catalog,
    )
}

/// Apply the configured ON DELETE action for every child FK referencing the
/// parent table when the row at `deleted_rowid` is being removed.
///
/// - NoAction / Restrict: error if any child row references this parent.
/// - Cascade: delete the matching child rows (recurse for further cascades).
/// - SetNull / SetDefault: rewrite the FK columns to NULL or the defaults.
pub(super) fn apply_foreign_key_delete_actions(
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

    // Snapshot the child tables to avoid holding a borrow on catalog while we
    // mutate the pager (and to allow recursive cascade through clones).
    let child_tables: Vec<crate::catalog::TableDef> = catalog
        .all_tables()
        .filter(|t| {
            t.foreign_keys
                .iter()
                .any(|fk| fk.to_table.eq_ignore_ascii_case(table_name))
        })
        .cloned()
        .collect();

    for child_table in child_tables {
        for fk in &child_table.foreign_keys {
            if !fk.to_table.eq_ignore_ascii_case(table_name) {
                continue;
            }
            let parent_col_indices: Vec<usize> = fk
                .to_columns
                .iter()
                .map(|tc| {
                    table_columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(tc))
                        .unwrap_or(0)
                })
                .collect();

            let parent_vals: Vec<Value> = parent_col_indices
                .iter()
                .map(|&ci| {
                    let col = &table_columns[ci];
                    if col.is_rowid_alias {
                        Value::Integer(deleted_rowid)
                    } else {
                        deleted_values.get(ci).cloned().unwrap_or(Value::Null)
                    }
                })
                .collect();

            let child_col_indices: Vec<usize> = fk
                .from_columns
                .iter()
                .map(|fc| {
                    child_table
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(fc))
                        .unwrap_or(0)
                })
                .collect();

            let mut cursor = BTreeCursor::new(pager, child_table.root_page);
            let rows = cursor
                .collect_all()
                .map_err(|e| Error::Other(e.to_string()))?;
            let matching_rowids: Vec<i64> = rows
                .iter()
                .filter(|row| {
                    child_col_indices
                        .iter()
                        .zip(parent_vals.iter())
                        .all(|(&ci, pv)| {
                            let child_val =
                                row.record.values.get(ci).cloned().unwrap_or(Value::Null);
                            super::helpers::values_equal(&child_val, pv)
                        })
                })
                .map(|r| r.rowid)
                .collect();

            if matching_rowids.is_empty() {
                continue;
            }

            match fk.on_delete {
                crate::catalog::ReferentialAction::NoAction
                | crate::catalog::ReferentialAction::Restrict => {
                    return Err(Error::Other(format!(
                        "FOREIGN KEY constraint failed: cannot delete from '{}' — referenced by '{}'",
                        table_name, child_table.name
                    )));
                }
                crate::catalog::ReferentialAction::Cascade => {
                    cascade_delete_child_rows(&child_table, &matching_rowids, pager, catalog)?;
                }
                crate::catalog::ReferentialAction::SetNull => {
                    set_child_fk_columns(
                        &child_table,
                        &matching_rowids,
                        &child_col_indices,
                        &vec![Value::Null; child_col_indices.len()],
                        pager,
                    )?;
                }
                crate::catalog::ReferentialAction::SetDefault => {
                    let defaults =
                        evaluate_column_defaults(&child_table, &child_col_indices, pager, catalog)?;
                    set_child_fk_columns(
                        &child_table,
                        &matching_rowids,
                        &child_col_indices,
                        &defaults,
                        pager,
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Mirror of [`apply_foreign_key_delete_actions`] for parent-key updates.
/// Fires the FK's `ON UPDATE` action whenever a referenced parent column
/// actually changes value.
pub(super) fn apply_foreign_key_update_actions(
    updated_rowid: i64,
    old_values: &[Value],
    new_values: &[Value],
    table_name: &str,
    table_columns: &[crate::catalog::ColumnDef],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    if !super::state::foreign_keys_enabled() {
        return Ok(());
    }

    let child_tables: Vec<crate::catalog::TableDef> = catalog
        .all_tables()
        .filter(|t| {
            t.foreign_keys
                .iter()
                .any(|fk| fk.to_table.eq_ignore_ascii_case(table_name))
        })
        .cloned()
        .collect();

    for child_table in child_tables {
        for fk in &child_table.foreign_keys {
            if !fk.to_table.eq_ignore_ascii_case(table_name) {
                continue;
            }
            let parent_col_indices: Vec<usize> = fk
                .to_columns
                .iter()
                .map(|tc| {
                    table_columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(tc))
                        .unwrap_or(0)
                })
                .collect();

            let value_for = |vals: &[Value], idx: usize| -> Value {
                let col = &table_columns[idx];
                if col.is_rowid_alias {
                    Value::Integer(updated_rowid)
                } else {
                    vals.get(idx).cloned().unwrap_or(Value::Null)
                }
            };

            let old_parent: Vec<Value> = parent_col_indices
                .iter()
                .map(|&ci| value_for(old_values, ci))
                .collect();
            let new_parent: Vec<Value> = parent_col_indices
                .iter()
                .map(|&ci| value_for(new_values, ci))
                .collect();

            // Skip if none of the referenced columns actually changed —
            // then no child action is required.
            if old_parent
                .iter()
                .zip(new_parent.iter())
                .all(|(o, n)| super::helpers::values_equal(o, n))
            {
                continue;
            }

            let child_col_indices: Vec<usize> = fk
                .from_columns
                .iter()
                .map(|fc| {
                    child_table
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(fc))
                        .unwrap_or(0)
                })
                .collect();

            let mut cursor = BTreeCursor::new(pager, child_table.root_page);
            let rows = cursor
                .collect_all()
                .map_err(|e| Error::Other(e.to_string()))?;
            let matching_rowids: Vec<i64> = rows
                .iter()
                .filter(|row| {
                    child_col_indices
                        .iter()
                        .zip(old_parent.iter())
                        .all(|(&ci, pv)| {
                            let child_val =
                                row.record.values.get(ci).cloned().unwrap_or(Value::Null);
                            super::helpers::values_equal(&child_val, pv)
                        })
                })
                .map(|r| r.rowid)
                .collect();

            if matching_rowids.is_empty() {
                continue;
            }

            match fk.on_update {
                crate::catalog::ReferentialAction::NoAction
                | crate::catalog::ReferentialAction::Restrict => {
                    return Err(Error::Other(format!(
                        "FOREIGN KEY constraint failed: cannot update '{}' \
                         — referenced by '{}'",
                        table_name, child_table.name
                    )));
                }
                crate::catalog::ReferentialAction::Cascade => {
                    set_child_fk_columns(
                        &child_table,
                        &matching_rowids,
                        &child_col_indices,
                        &new_parent,
                        pager,
                    )?;
                }
                crate::catalog::ReferentialAction::SetNull => {
                    set_child_fk_columns(
                        &child_table,
                        &matching_rowids,
                        &child_col_indices,
                        &vec![Value::Null; child_col_indices.len()],
                        pager,
                    )?;
                }
                crate::catalog::ReferentialAction::SetDefault => {
                    let defaults = evaluate_column_defaults(
                        &child_table,
                        &child_col_indices,
                        pager,
                        catalog,
                    )?;
                    set_child_fk_columns(
                        &child_table,
                        &matching_rowids,
                        &child_col_indices,
                        &defaults,
                        pager,
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Delete the given child rows, then recursively apply ON DELETE actions for
/// any FKs referencing this child table.
fn cascade_delete_child_rows(
    child_table: &crate::catalog::TableDef,
    rowids: &[i64],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    use rsqlite_storage::btree::{btree_delete, btree_index_delete};

    // Snapshot child rows for recursion before deleting.
    let mut cursor = BTreeCursor::new(pager, child_table.root_page);
    let rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;
    let snapshots: Vec<(i64, Vec<Value>)> = rows
        .iter()
        .filter(|r| rowids.contains(&r.rowid))
        .map(|r| (r.rowid, r.record.values.clone()))
        .collect();

    let table_indexes = super::helpers::get_table_indexes(catalog, &child_table.name);
    let plan_columns: Vec<crate::planner::ColumnRef> = child_table
        .columns
        .iter()
        .map(|c| crate::planner::ColumnRef {
            name: c.name.clone(),
            column_index: c.column_index,
            is_rowid_alias: c.is_rowid_alias,
            table: None,
            nullable: c.nullable,
            is_primary_key: c.is_primary_key,
            is_unique: c.is_unique,
        })
        .collect();

    for (rowid, values) in &snapshots {
        for (idx_root, idx_col_indices) in &table_indexes {
            let key =
                super::helpers::build_index_key(values, idx_col_indices, &plan_columns, pager, catalog, *rowid)?;
            let _ = btree_index_delete(pager, *idx_root, &key);
        }
        btree_delete(pager, child_table.root_page, *rowid)
            .map_err(|e| Error::Other(e.to_string()))?;
    }

    // Recursively cascade: this child may itself be a parent.
    for (rowid, values) in &snapshots {
        apply_foreign_key_delete_actions(
            *rowid,
            values,
            &child_table.name,
            &child_table.columns,
            pager,
            catalog,
        )?;
    }
    Ok(())
}

fn set_child_fk_columns(
    child_table: &crate::catalog::TableDef,
    rowids: &[i64],
    fk_col_indices: &[usize],
    new_values: &[Value],
    pager: &mut Pager,
) -> Result<()> {
    use rsqlite_storage::btree::{btree_delete, btree_insert};
    use rsqlite_storage::codec::Record;

    let mut cursor = BTreeCursor::new(pager, child_table.root_page);
    let rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;
    let to_update: Vec<(i64, Vec<Value>)> = rows
        .iter()
        .filter(|r| rowids.contains(&r.rowid))
        .map(|r| {
            let mut updated = r.record.values.clone();
            for (k, &ci) in fk_col_indices.iter().enumerate() {
                if ci < updated.len() {
                    updated[ci] = new_values.get(k).cloned().unwrap_or(Value::Null);
                }
            }
            (r.rowid, updated)
        })
        .collect();

    for (rowid, updated) in to_update {
        btree_delete(pager, child_table.root_page, rowid)
            .map_err(|e| Error::Other(e.to_string()))?;
        btree_insert(
            pager,
            child_table.root_page,
            rowid,
            &Record { values: updated },
        )
        .map_err(|e| Error::Other(e.to_string()))?;
    }
    Ok(())
}

/// Evaluate the static DEFAULT values for the given FK columns of a child
/// table. Columns without a DEFAULT (or whose DEFAULT can't be evaluated
/// statically) fall back to NULL.
fn evaluate_column_defaults(
    child_table: &crate::catalog::TableDef,
    col_indices: &[usize],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(col_indices.len());
    for &ci in col_indices {
        let col = &child_table.columns[ci];
        let default = match &col.default_expr {
            Some(s) => s.clone(),
            None => {
                out.push(Value::Null);
                continue;
            }
        };
        // Best-effort: parse and evaluate the default expression.
        let parsed = match rsqlite_parser::parse::parse_sql(&format!("SELECT {default}")) {
            Ok(s) => s,
            Err(_) => {
                out.push(Value::Null);
                continue;
            }
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
            None => {
                out.push(Value::Null);
                continue;
            }
        };
        let plan_columns: Vec<crate::planner::ColumnRef> = child_table
            .columns
            .iter()
            .map(|c| crate::planner::ColumnRef {
                name: c.name.clone(),
                column_index: c.column_index,
                is_rowid_alias: c.is_rowid_alias,
                table: None,
                nullable: c.nullable,
                is_primary_key: c.is_primary_key,
                is_unique: c.is_unique,
            })
            .collect();
        let plan_expr = match crate::planner::plan_expr(&expr_ast, &plan_columns, catalog) {
            Ok(pe) => pe,
            Err(_) => {
                out.push(Value::Null);
                continue;
            }
        };
        let placeholder_row = crate::types::Row { values: vec![Value::Null; child_table.columns.len()], rowid: None };
        let col_names: Vec<String> = child_table.columns.iter().map(|c| c.name.clone()).collect();
        match super::eval::eval_expr(&plan_expr, &placeholder_row, &col_names, pager, catalog) {
            Ok(v) => out.push(v),
            Err(_) => out.push(Value::Null),
        }
    }
    Ok(out)
}

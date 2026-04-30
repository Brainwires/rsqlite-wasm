use rsqlite_storage::btree::{
    btree_delete, btree_index_delete, btree_index_insert, btree_insert, btree_max_rowid,
    btree_row_exists,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::planner::{InsertPlan, OnConflictPlan};
use crate::types::Row;

use super::autoincrement::{compute_autoincrement_rowid, update_autoincrement_seq};
use super::constraints::{
    check_check_constraints, check_foreign_key_insert, check_not_null_constraints,
    check_unique_constraints, find_unique_conflict_rowid,
};
use super::eval::eval_expr;
use super::helpers::{
    apply_column_defaults, build_index_key, eval_insert_row, get_table_indexes,
    map_query_row_to_insert, read_row_by_rowid,
};
use super::state::{set_changes, set_last_insert_rowid};
use super::trigger::fire_triggers;
use super::ExecResult;

pub(super) fn execute_insert(plan: &InsertPlan, pager: &mut Pager, catalog: &Catalog) -> Result<ExecResult> {
    let table_indexes = get_table_indexes(catalog, &plan.table_name);
    let mut rows_affected = 0u64;
    let mut current_root = plan.root_page;
    let is_autoincrement = catalog
        .get_table(&plan.table_name)
        .is_some_and(|t| t.has_autoincrement);
    let mut max_rowid_inserted = 0i64;

    if let Some(source) = &plan.source_query {
        let query_result = super::execute(source, pager, catalog)?;
        for row in &query_result.rows {
            let (mut values, explicitly_set) = map_query_row_to_insert(
                &row.values,
                &plan.table_columns,
                &plan.target_columns,
            )?;
            apply_column_defaults(
                &mut values,
                &explicitly_set,
                &plan.table_name,
                &plan.table_columns,
                pager,
                catalog,
            )?;

            let mut rowid = None;
            for (i, col) in plan.table_columns.iter().enumerate() {
                if col.is_rowid_alias {
                    if let Value::Integer(id) = &values[i] {
                        rowid = Some(*id);
                    }
                }
            }
            let rowid = match rowid {
                Some(id) => id,
                None if is_autoincrement => compute_autoincrement_rowid(pager, catalog, &plan.table_name, current_root)?,
                None => btree_max_rowid(pager, current_root)? + 1,
            };

            check_not_null_constraints(&values, &plan.table_columns, &plan.table_name)?;
            check_unique_constraints(&values, &plan.table_columns, &plan.table_name, pager, current_root, None)?;
            check_check_constraints(&values, &plan.table_columns, &plan.table_name, pager, catalog)?;
            check_foreign_key_insert(&values, &plan.table_columns, &plan.table_name, pager, catalog)?;
            let record = Record { values: values.clone() };
            current_root = btree_insert(pager, current_root, rowid, &record)?;
            if rowid > max_rowid_inserted {
                max_rowid_inserted = rowid;
            }
            for (idx_root, idx_col_indices) in &table_indexes {
                let key = build_index_key(&values, idx_col_indices, &plan.table_columns, rowid);
                btree_index_insert(pager, *idx_root, &key)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
            rows_affected += 1;
        }

        if is_autoincrement && max_rowid_inserted > 0 {
            update_autoincrement_seq(pager, catalog, &plan.table_name, max_rowid_inserted)?;
        }

        if !pager.in_transaction() {
            pager.flush()?;
        }
        set_changes(rows_affected as i64);
        return Ok(ExecResult { rows_affected });
    }

    let mut last_rowid = 0i64;
    for row_exprs in &plan.rows {
        let (mut values, explicitly_set) =
            eval_insert_row(row_exprs, &plan.table_columns, &plan.target_columns)?;
        apply_column_defaults(
            &mut values,
            &explicitly_set,
            &plan.table_name,
            &plan.table_columns,
            pager,
            catalog,
        )?;

        let mut rowid = None;
        for (i, col) in plan.table_columns.iter().enumerate() {
            if col.is_rowid_alias {
                if let Value::Integer(id) = &values[i] {
                    rowid = Some(*id);
                }
            }
        }

        let rowid = match rowid {
            Some(id) => id,
            None if is_autoincrement => compute_autoincrement_rowid(pager, catalog, &plan.table_name, current_root)?,
            None => btree_max_rowid(pager, current_root)? + 1,
        };

        if plan.or_replace && btree_row_exists(pager, current_root, rowid)? {
            let old_values =
                read_row_by_rowid(pager, current_root, rowid, &plan.table_columns)?;
            for (idx_root, idx_col_indices) in &table_indexes {
                let old_key = build_index_key(
                    &old_values,
                    idx_col_indices,
                    &plan.table_columns,
                    rowid,
                );
                let _ = btree_index_delete(pager, *idx_root, &old_key);
            }
            btree_delete(pager, current_root, rowid)
                .map_err(|e| Error::Other(e.to_string()))?;
        } else if plan.or_replace {
            let conflict_rowid = find_unique_conflict_rowid(
                &values,
                &plan.table_columns,
                pager,
                current_root,
            )?;
            if let Some(existing_rowid) = conflict_rowid {
                let old_values =
                    read_row_by_rowid(pager, current_root, existing_rowid, &plan.table_columns)?;
                for (idx_root, idx_col_indices) in &table_indexes {
                    let old_key = build_index_key(
                        &old_values,
                        idx_col_indices,
                        &plan.table_columns,
                        existing_rowid,
                    );
                    let _ = btree_index_delete(pager, *idx_root, &old_key);
                }
                btree_delete(pager, current_root, existing_rowid)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
        } else if let Some(on_conflict) = &plan.on_conflict {
            if btree_row_exists(pager, current_root, rowid)? {
                match on_conflict {
                    OnConflictPlan::DoNothing => continue,
                    OnConflictPlan::DoUpdate { assignments } => {
                        let old_values =
                            read_row_by_rowid(pager, current_root, rowid, &plan.table_columns)?;
                        let col_names: Vec<String> =
                            plan.table_columns.iter().map(|c| c.name.clone()).collect();
                        let old_row = Row {
                            values: old_values.clone(),
                        };
                        let mut updated = old_values.clone();
                        for (col_name, expr) in assignments {
                            let val = eval_expr(expr, &old_row, &col_names, pager, catalog)?;
                            let idx = plan
                                .table_columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                                .ok_or_else(|| {
                                    Error::Other(format!("unknown column: {col_name}"))
                                })?;
                            updated[idx] = val;
                        }
                        for (idx_root, idx_col_indices) in &table_indexes {
                            let old_key = build_index_key(
                                &old_values,
                                idx_col_indices,
                                &plan.table_columns,
                                rowid,
                            );
                            btree_index_delete(pager, *idx_root, &old_key)
                                .map_err(|e| Error::Other(e.to_string()))?;
                        }
                        btree_delete(pager, current_root, rowid)
                            .map_err(|e| Error::Other(e.to_string()))?;
                        let record = Record {
                            values: updated.clone(),
                        };
                        current_root = btree_insert(pager, current_root, rowid, &record)?;
                        for (idx_root, idx_col_indices) in &table_indexes {
                            let new_key = build_index_key(
                                &updated,
                                idx_col_indices,
                                &plan.table_columns,
                                rowid,
                            );
                            btree_index_insert(pager, *idx_root, &new_key)
                                .map_err(|e| Error::Other(e.to_string()))?;
                        }
                        rows_affected += 1;
                        continue;
                    }
                }
            }
        }

        check_not_null_constraints(&values, &plan.table_columns, &plan.table_name)?;
        check_unique_constraints(&values, &plan.table_columns, &plan.table_name, pager, current_root, None)?;
        check_check_constraints(&values, &plan.table_columns, &plan.table_name, pager, catalog)?;
        check_foreign_key_insert(&values, &plan.table_columns, &plan.table_name, pager, catalog)?;

        let new_named: Vec<(String, Value)> = plan.table_columns.iter()
            .zip(values.iter())
            .map(|(c, v)| (c.name.clone(), v.clone()))
            .collect();
        fire_triggers(
            &plan.table_name,
            &crate::catalog::TriggerTiming::Before,
            &crate::catalog::TriggerEvent::Insert,
            None,
            Some(&new_named),
            pager,
            catalog,
        )?;

        let record = Record {
            values: values.clone(),
        };
        current_root = btree_insert(pager, current_root, rowid, &record)?;
        last_rowid = rowid;
        if rowid > max_rowid_inserted {
            max_rowid_inserted = rowid;
        }

        for (idx_root, idx_col_indices) in &table_indexes {
            let key = build_index_key(&values, idx_col_indices, &plan.table_columns, rowid);
            btree_index_insert(pager, *idx_root, &key)
                .map_err(|e| Error::Other(e.to_string()))?;
        }

        fire_triggers(
            &plan.table_name,
            &crate::catalog::TriggerTiming::After,
            &crate::catalog::TriggerEvent::Insert,
            None,
            Some(&new_named),
            pager,
            catalog,
        )?;

        rows_affected += 1;
    }

    if is_autoincrement && max_rowid_inserted > 0 {
        update_autoincrement_seq(pager, catalog, &plan.table_name, max_rowid_inserted)?;
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    set_last_insert_rowid(last_rowid);
    set_changes(rows_affected as i64);
    Ok(ExecResult { rows_affected })
}

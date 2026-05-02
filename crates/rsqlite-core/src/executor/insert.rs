use rsqlite_storage::btree::{
    IndexCursor, btree_delete, btree_index_delete, btree_index_insert, btree_insert,
    btree_max_rowid, btree_row_exists,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::planner::{InsertPlan, OnConflictPlan};
use crate::types::Row;

use super::ExecResult;
use super::autoincrement::{compute_autoincrement_rowid, update_autoincrement_seq};
use super::constraints::{
    check_check_constraints, check_foreign_key_insert, check_not_null_constraints,
    check_unique_constraints, find_conflict_by_columns, find_unique_conflict_rowid,
};
use super::eval::eval_expr;
use super::helpers::{
    apply_column_defaults, apply_generated_columns, build_index_key, build_returning_result,
    declared_to_storage_order, eval_insert_row, get_table_indexes_with_predicates,
    index_predicate_matches, map_query_row_to_insert, read_row_by_rowid,
    without_rowid_pk_indices,
};
use super::state::{set_changes, set_last_insert_rowid};
use super::trigger::fire_triggers;

pub(super) fn execute_insert(
    plan: &InsertPlan,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<ExecResult> {
    let result = execute_insert_inner(plan, pager, catalog);
    // INSERT OR ROLLBACK: on any failure, roll the active transaction back
    // before propagating. Without an active transaction this is a no-op.
    if result.is_err()
        && plan.conflict_strategy == crate::planner::ConflictStrategy::Rollback
        && pager.in_transaction()
    {
        let _ = pager.rollback();
    }
    result
}

fn execute_insert_inner(
    plan: &InsertPlan,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<ExecResult> {
    // Reject explicit INSERT to a generated column.
    if let Some(targets) = &plan.target_columns {
        if let Some(td) = catalog.get_table(&plan.table_name) {
            for t in targets {
                if let Some(c) = td.columns.iter().find(|c| c.name.eq_ignore_ascii_case(t)) {
                    if c.generated.is_some() {
                        return Err(Error::Other(format!(
                            "cannot INSERT into generated column {}.{}",
                            plan.table_name, c.name
                        )));
                    }
                }
            }
        }
    }

    // WITHOUT ROWID tables use index-format storage with the PK columns
    // as the sort key and the full row record as the payload. Branch
    // into a dedicated path so we don't have to weave WITHOUT ROWID
    // handling through the rowid-keyed code below (autoincrement,
    // ON CONFLICT-by-rowid, etc.).
    if catalog
        .get_table(&plan.table_name)
        .is_some_and(|t| t.without_rowid)
    {
        return execute_insert_without_rowid(plan, pager, catalog);
    }

    let table_indexes = get_table_indexes_with_predicates(catalog, &plan.table_name);
    let mut rows_affected = 0u64;
    let mut current_root = plan.root_page;
    let is_autoincrement = catalog
        .get_table(&plan.table_name)
        .is_some_and(|t| t.has_autoincrement);
    let mut max_rowid_inserted = 0i64;
    let mut returning_values: Vec<Vec<Value>> = Vec::new();

    if let Some(source) = &plan.source_query {
        let query_result = super::execute(source, pager, catalog)?;
        for row in &query_result.rows {
            let (mut values, explicitly_set) =
                map_query_row_to_insert(&row.values, &plan.table_columns, &plan.target_columns)?;
            apply_column_defaults(
                &mut values,
                &explicitly_set,
                &plan.table_name,
                &plan.table_columns,
                pager,
                catalog,
            )?;
            apply_generated_columns(
                &mut values,
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
                None if is_autoincrement => {
                    compute_autoincrement_rowid(pager, catalog, &plan.table_name, current_root)?
                }
                None => btree_max_rowid(pager, current_root)? + 1,
            };

            check_not_null_constraints(&values, &plan.table_columns, &plan.table_name)?;
            check_unique_constraints(
                &values,
                &plan.table_columns,
                &plan.table_name,
                pager,
                current_root,
                None,
                catalog,
            )?;
            check_check_constraints(
                &values,
                &plan.table_columns,
                &plan.table_name,
                pager,
                catalog,
            )?;
            check_foreign_key_insert(
                &values,
                &plan.table_columns,
                &plan.table_name,
                pager,
                catalog,
            )?;
            let record = Record {
                values: values.clone(),
            };
            current_root = btree_insert(pager, current_root, rowid, &record)?;
            if rowid > max_rowid_inserted {
                max_rowid_inserted = rowid;
            }
            for (idx_root, idx_col_indices, predicate) in &table_indexes {
                if !index_predicate_matches(
                    predicate.as_deref(),
                    &values,
                    &plan.table_columns,
                    pager,
                    catalog,
                )? {
                    continue;
                }
                let key = build_index_key(&values, idx_col_indices, &plan.table_columns, pager, catalog, rowid)?;
                btree_index_insert(pager, *idx_root, &key)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
            if plan.returning.is_some() {
                returning_values.push(row_with_rowid(&values, rowid, &plan.table_columns));
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
        let returning = if let Some(items) = &plan.returning {
            Some(build_returning_result(
                items,
                &returning_values,
                &plan.table_columns,
                pager,
                catalog,
            )?)
        } else {
            None
        };
        return Ok(ExecResult {
            rows_affected,
            returning,
        });
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
        apply_generated_columns(
            &mut values,
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
            None if is_autoincrement => {
                compute_autoincrement_rowid(pager, catalog, &plan.table_name, current_root)?
            }
            None => btree_max_rowid(pager, current_root)? + 1,
        };

        if plan.or_replace && btree_row_exists(pager, current_root, rowid)? {
            let old_values = read_row_by_rowid(pager, current_root, rowid, &plan.table_columns)?;
            for (idx_root, idx_col_indices, _) in &table_indexes {
                let old_key =
                    build_index_key(&old_values, idx_col_indices, &plan.table_columns, pager, catalog, rowid)?;
                let _ = btree_index_delete(pager, *idx_root, &old_key);
            }
            btree_delete(pager, current_root, rowid).map_err(|e| Error::Other(e.to_string()))?;
        } else if plan.or_replace {
            let conflict_rowid =
                find_unique_conflict_rowid(&values, &plan.table_columns, pager, current_root)?;
            if let Some(existing_rowid) = conflict_rowid {
                let old_values =
                    read_row_by_rowid(pager, current_root, existing_rowid, &plan.table_columns)?;
                for (idx_root, idx_col_indices, _) in &table_indexes {
                    let old_key = build_index_key(&old_values, idx_col_indices, &plan.table_columns, pager, catalog, existing_rowid)?;
                    let _ = btree_index_delete(pager, *idx_root, &old_key);
                }
                btree_delete(pager, current_root, existing_rowid)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
        } else if let Some(on_conflict) = &plan.on_conflict {
            // Detect conflict: by named conflict_columns if given, otherwise
            // by rowid (legacy/default behavior).
            let conflict_rowid = match on_conflict {
                OnConflictPlan::DoNothing => {
                    if btree_row_exists(pager, current_root, rowid)? {
                        Some(rowid)
                    } else {
                        find_unique_conflict_rowid(
                            &values,
                            &plan.table_columns,
                            pager,
                            current_root,
                        )?
                    }
                }
                OnConflictPlan::DoUpdate {
                    conflict_columns, ..
                } => {
                    if conflict_columns.is_empty() {
                        if btree_row_exists(pager, current_root, rowid)? {
                            Some(rowid)
                        } else {
                            find_unique_conflict_rowid(
                                &values,
                                &plan.table_columns,
                                pager,
                                current_root,
                            )?
                        }
                    } else {
                        find_conflict_by_columns(
                            &values,
                            conflict_columns,
                            &plan.table_columns,
                            pager,
                            current_root,
                        )?
                    }
                }
            };

            if let Some(existing_rowid) = conflict_rowid {
                match on_conflict {
                    OnConflictPlan::DoNothing => continue,
                    OnConflictPlan::DoUpdate {
                        assignments,
                        where_clause,
                        ..
                    } => {
                        let old_values = read_row_by_rowid(
                            pager,
                            current_root,
                            existing_rowid,
                            &plan.table_columns,
                        )?;
                        // Build a combined row: [old_values..., new_values...]
                        // so `excluded.col` references (planned at indices
                        // table_columns.len() + col.column_index) resolve to
                        // the just-attempted INSERT values.
                        let mut combined_values = old_values.clone();
                        combined_values.extend_from_slice(&values);
                        let combined_row = Row { values: combined_values, rowid: None };
                        let mut combined_col_names: Vec<String> =
                            plan.table_columns.iter().map(|c| c.name.clone()).collect();
                        for c in &plan.table_columns {
                            combined_col_names.push(format!("excluded.{}", c.name));
                        }

                        // WHERE clause on DO UPDATE: skip if false.
                        if let Some(pred) = where_clause {
                            let v = eval_expr(
                                pred,
                                &combined_row,
                                &combined_col_names,
                                pager,
                                catalog,
                            )?;
                            if !crate::eval_helpers::is_truthy(&v) {
                                continue;
                            }
                        }

                        let mut updated = old_values.clone();
                        for (col_name, expr) in assignments {
                            let val = eval_expr(
                                expr,
                                &combined_row,
                                &combined_col_names,
                                pager,
                                catalog,
                            )?;
                            let idx = plan
                                .table_columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                                .ok_or_else(|| {
                                    Error::Other(format!("unknown column: {col_name}"))
                                })?;
                            updated[idx] = val;
                        }
                        for (idx_root, idx_col_indices, predicate) in &table_indexes {
                            if !index_predicate_matches(
                                predicate.as_deref(),
                                &old_values,
                                &plan.table_columns,
                                pager,
                                catalog,
                            )? {
                                continue;
                            }
                            let old_key = build_index_key(&old_values, idx_col_indices, &plan.table_columns, pager, catalog, existing_rowid)?;
                            btree_index_delete(pager, *idx_root, &old_key)
                                .map_err(|e| Error::Other(e.to_string()))?;
                        }
                        btree_delete(pager, current_root, existing_rowid)
                            .map_err(|e| Error::Other(e.to_string()))?;
                        let record = Record {
                            values: updated.clone(),
                        };
                        current_root = btree_insert(pager, current_root, existing_rowid, &record)?;
                        for (idx_root, idx_col_indices, predicate) in &table_indexes {
                            if !index_predicate_matches(
                                predicate.as_deref(),
                                &updated,
                                &plan.table_columns,
                                pager,
                                catalog,
                            )? {
                                continue;
                            }
                            let new_key = build_index_key(&updated, idx_col_indices, &plan.table_columns, pager, catalog, existing_rowid)?;
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
        check_unique_constraints(
            &values,
            &plan.table_columns,
            &plan.table_name,
            pager,
            current_root,
            None,
            catalog,
        )?;
        check_check_constraints(
            &values,
            &plan.table_columns,
            &plan.table_name,
            pager,
            catalog,
        )?;
        check_foreign_key_insert(
            &values,
            &plan.table_columns,
            &plan.table_name,
            pager,
            catalog,
        )?;

        let new_named: Vec<(String, Value)> = plan
            .table_columns
            .iter()
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

        for (idx_root, idx_col_indices, predicate) in &table_indexes {
            if !index_predicate_matches(
                predicate.as_deref(),
                &values,
                &plan.table_columns,
                pager,
                catalog,
            )? {
                continue;
            }
            let key = build_index_key(&values, idx_col_indices, &plan.table_columns, pager, catalog, rowid)?;
            btree_index_insert(pager, *idx_root, &key).map_err(|e| Error::Other(e.to_string()))?;
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

        if plan.returning.is_some() {
            returning_values.push(row_with_rowid(&values, rowid, &plan.table_columns));
        }
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
    let returning = if let Some(items) = &plan.returning {
        Some(build_returning_result(
            items,
            &returning_values,
            &plan.table_columns,
            pager,
            catalog,
        )?)
    } else {
        None
    };
    Ok(ExecResult {
        rows_affected,
        returning,
    })
}

/// Build a row whose rowid-alias columns carry the actual rowid (rather than
/// NULL when omitted from the INSERT).
fn row_with_rowid(
    values: &[Value],
    rowid: i64,
    table_columns: &[crate::planner::ColumnRef],
) -> Vec<Value> {
    table_columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if c.is_rowid_alias && matches!(values.get(i), Some(Value::Null) | None) {
                Value::Integer(rowid)
            } else {
                values.get(i).cloned().unwrap_or(Value::Null)
            }
        })
        .collect()
}

/// INSERT path for `CREATE TABLE … WITHOUT ROWID` tables. The btree at
/// `plan.root_page` is an index-format btree (LeafIndex/InteriorIndex
/// pages); the stored record is the row reordered as
/// `[pk_cols..., non_pk_cols...]` so the leading prefix functions as
/// the btree key. No rowid is generated.
fn execute_insert_without_rowid(
    plan: &InsertPlan,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<ExecResult> {
    let pk_indices = without_rowid_pk_indices(catalog, &plan.table_name);
    if pk_indices.is_empty() {
        return Err(Error::Other(format!(
            "WITHOUT ROWID table {} has no primary key columns",
            plan.table_name
        )));
    }

    let mut rows_affected = 0u64;
    let mut returning_values: Vec<Vec<Value>> = Vec::new();

    let row_iter: Vec<(Vec<Value>, Vec<bool>)> = if let Some(source) = &plan.source_query {
        let query_result = super::execute(source, pager, catalog)?;
        query_result
            .rows
            .iter()
            .map(|row| {
                map_query_row_to_insert(&row.values, &plan.table_columns, &plan.target_columns)
            })
            .collect::<Result<_>>()?
    } else {
        plan.rows
            .iter()
            .map(|row_exprs| {
                eval_insert_row(row_exprs, &plan.table_columns, &plan.target_columns)
            })
            .collect::<Result<_>>()?
    };

    for (mut values, explicitly_set) in row_iter {
        apply_column_defaults(
            &mut values,
            &explicitly_set,
            &plan.table_name,
            &plan.table_columns,
            pager,
            catalog,
        )?;
        apply_generated_columns(
            &mut values,
            &plan.table_name,
            &plan.table_columns,
            pager,
            catalog,
        )?;

        check_not_null_constraints(&values, &plan.table_columns, &plan.table_name)?;

        let pk_prefix: Vec<Value> = pk_indices
            .iter()
            .map(|&i| values.get(i).cloned().unwrap_or(Value::Null))
            .collect();
        for (slot, &i) in pk_indices.iter().enumerate() {
            if matches!(pk_prefix[slot], Value::Null) {
                return Err(Error::Other(format!(
                    "NOT NULL constraint failed: {}.{}",
                    plan.table_name, plan.table_columns[i].name
                )));
            }
        }
        let prefix_record = Record { values: pk_prefix };
        let mut probe = IndexCursor::new(pager, plan.root_page);
        let exists = probe
            .seek_first_with_prefix(&prefix_record)
            .map_err(|e| Error::Other(e.to_string()))?;
        if exists {
            let names = pk_indices
                .iter()
                .map(|&i| format!("{}.{}", plan.table_name, plan.table_columns[i].name))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::Other(format!("UNIQUE constraint failed: {names}")));
        }

        check_check_constraints(
            &values,
            &plan.table_columns,
            &plan.table_name,
            pager,
            catalog,
        )?;
        check_foreign_key_insert(
            &values,
            &plan.table_columns,
            &plan.table_name,
            pager,
            catalog,
        )?;

        let new_named: Vec<(String, Value)> = plan
            .table_columns
            .iter()
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

        let stored = declared_to_storage_order(&values, &pk_indices);
        let record = Record { values: stored };
        btree_index_insert(pager, plan.root_page, &record)
            .map_err(|e| Error::Other(e.to_string()))?;

        let table_indexes = get_table_indexes_with_predicates(catalog, &plan.table_name);
        for (idx_root, idx_col_indices, predicate) in &table_indexes {
            if !index_predicate_matches(
                predicate.as_deref(),
                &values,
                &plan.table_columns,
                pager,
                catalog,
            )? {
                continue;
            }
            let key = build_index_key(
                &values,
                idx_col_indices,
                &plan.table_columns,
                pager,
                catalog,
                0,
            )?;
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

        if plan.returning.is_some() {
            returning_values.push(values.clone());
        }
        rows_affected += 1;
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    set_changes(rows_affected as i64);
    let returning = if let Some(items) = &plan.returning {
        Some(build_returning_result(
            items,
            &returning_values,
            &plan.table_columns,
            pager,
            catalog,
        )?)
    } else {
        None
    };
    Ok(ExecResult {
        rows_affected,
        returning,
    })
}

use rsqlite_storage::btree::{
    BTreeCursor, btree_delete, btree_index_delete, btree_index_insert, btree_insert,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::is_truthy;
use crate::planner::UpdatePlan;
use crate::types::Row;

use super::ExecResult;
use super::constraints::{
    check_check_constraints, check_foreign_key_insert, check_not_null_constraints,
    check_unique_constraints,
};
use super::eval::eval_expr;
use super::helpers::{
    build_index_key, build_returning_result, get_table_indexes, row_values_for_rowid,
};
use super::state::set_changes;
use super::trigger::fire_triggers;

pub(super) fn execute_update(
    plan: &UpdatePlan,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<ExecResult> {
    let column_names: Vec<String> = plan.table_columns.iter().map(|c| c.name.clone()).collect();

    let mut cursor = BTreeCursor::new(pager, plan.root_page);
    let btree_rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

    // For UPDATE FROM, snapshot the FROM table rows once.
    let from_rows: Vec<Vec<Value>> = if let Some(from) = &plan.from {
        let mut from_cursor = BTreeCursor::new(pager, from.root_page);
        let raw = from_cursor
            .collect_all()
            .map_err(|e| Error::Other(e.to_string()))?;
        raw.into_iter()
            .map(|br| {
                from.columns
                    .iter()
                    .map(|c| {
                        if c.is_rowid_alias {
                            Value::Integer(br.rowid)
                        } else {
                            // c.column_index is offset by table_columns.len();
                            // map back to the local index into the FROM record.
                            let local_idx = c.column_index - plan.table_columns.len();
                            br.record
                                .values
                                .get(local_idx)
                                .cloned()
                                .unwrap_or(Value::Null)
                        }
                    })
                    .collect()
            })
            .collect()
    } else {
        Vec::new()
    };

    let combined_column_names: Vec<String> = if plan.from.is_some() {
        // Qualify both sides so eval_expr's "table.col" lookup finds the
        // intended side instead of falling back to bare-name matching.
        let mut names: Vec<String> = plan
            .table_columns
            .iter()
            .map(|c| format!("{}.{}", plan.table_name, c.name))
            .collect();
        if let Some(from) = &plan.from {
            for c in &from.columns {
                names.push(format!("{}.{}", from.table_name, c.name));
            }
        }
        names
    } else {
        column_names.clone()
    };

    let mut to_update: Vec<(i64, Vec<Value>)> = Vec::new();

    for btree_row in &btree_rows {
        let record_values = &btree_row.record.values;
        let mut row_values = Vec::with_capacity(plan.table_columns.len());

        for col in &plan.table_columns {
            if col.is_rowid_alias {
                row_values.push(Value::Integer(btree_row.rowid));
            } else {
                let val = record_values
                    .get(col.column_index)
                    .cloned()
                    .unwrap_or(Value::Null);
                row_values.push(val);
            }
        }

        if let Some(_) = &plan.from {
            // For each FROM row, extend the target row and check the predicate.
            // Use the LAST matching FROM row's values for assignments (matches
            // SQLite's "implementation-defined" behavior on multi-match).
            let mut last_match_combined: Option<Vec<Value>> = None;
            for from_row in &from_rows {
                let mut combined = row_values.clone();
                combined.extend_from_slice(from_row);
                let combined_row = Row {
                    values: combined.clone(),
                };

                let matches = match &plan.predicate {
                    Some(pred) => {
                        let val =
                            eval_expr(pred, &combined_row, &combined_column_names, pager, catalog)?;
                        is_truthy(&val)
                    }
                    None => true,
                };
                if matches {
                    last_match_combined = Some(combined);
                }
            }
            if let Some(combined) = last_match_combined {
                let combined_row = Row { values: combined };
                let mut new_values = row_values.clone();
                for (col_name, expr) in &plan.assignments {
                    let col_idx = column_names
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(col_name))
                        .ok_or_else(|| Error::Other(format!("unknown column: {col_name}")))?;
                    new_values[col_idx] =
                        eval_expr(expr, &combined_row, &combined_column_names, pager, catalog)?;
                }
                check_not_null_constraints(&new_values, &plan.table_columns, &plan.table_name)?;
                check_unique_constraints(
                    &new_values,
                    &plan.table_columns,
                    &plan.table_name,
                    pager,
                    plan.root_page,
                    Some(btree_row.rowid),
                )?;
                check_check_constraints(
                    &new_values,
                    &plan.table_columns,
                    &plan.table_name,
                    pager,
                    catalog,
                )?;
                check_foreign_key_insert(
                    &new_values,
                    &plan.table_columns,
                    &plan.table_name,
                    pager,
                    catalog,
                )?;
                to_update.push((btree_row.rowid, new_values));
            }
            continue;
        }

        let row = Row { values: row_values };

        let matches = match &plan.predicate {
            Some(pred) => {
                let val = eval_expr(pred, &row, &column_names, pager, catalog)?;
                is_truthy(&val)
            }
            None => true,
        };

        if matches {
            let mut new_values = row.values.clone();
            for (col_name, expr) in &plan.assignments {
                let col_idx = column_names
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| Error::Other(format!("unknown column: {col_name}")))?;
                new_values[col_idx] = eval_expr(expr, &row, &column_names, pager, catalog)?;
            }
            check_not_null_constraints(&new_values, &plan.table_columns, &plan.table_name)?;
            check_unique_constraints(
                &new_values,
                &plan.table_columns,
                &plan.table_name,
                pager,
                plan.root_page,
                Some(btree_row.rowid),
            )?;
            check_check_constraints(
                &new_values,
                &plan.table_columns,
                &plan.table_name,
                pager,
                catalog,
            )?;
            check_foreign_key_insert(
                &new_values,
                &plan.table_columns,
                &plan.table_name,
                pager,
                catalog,
            )?;
            to_update.push((btree_row.rowid, new_values));
        }
    }

    let rows_affected = to_update.len() as u64;
    let table_indexes = get_table_indexes(catalog, &plan.table_name);
    let mut returning_values: Vec<Vec<Value>> = Vec::new();

    let mut current_root = plan.root_page;
    for (rowid, new_values) in to_update {
        let old_values = row_values_for_rowid(&btree_rows, rowid, &plan.table_columns);
        let old_named: Vec<(String, Value)> = plan
            .table_columns
            .iter()
            .zip(old_values.iter())
            .map(|(c, v)| (c.name.clone(), v.clone()))
            .collect();
        let new_named: Vec<(String, Value)> = plan
            .table_columns
            .iter()
            .zip(new_values.iter())
            .map(|(c, v)| (c.name.clone(), v.clone()))
            .collect();

        fire_triggers(
            &plan.table_name,
            &crate::catalog::TriggerTiming::Before,
            &crate::catalog::TriggerEvent::Update,
            Some(&old_named),
            Some(&new_named),
            pager,
            catalog,
        )?;

        for (idx_root, idx_col_indices) in &table_indexes {
            let old_key = build_index_key(&old_values, idx_col_indices, &plan.table_columns, rowid);
            let _ = btree_index_delete(pager, *idx_root, &old_key);

            let new_key = build_index_key(&new_values, idx_col_indices, &plan.table_columns, rowid);
            let _ = btree_index_insert(pager, *idx_root, &new_key);
        }

        btree_delete(pager, current_root, rowid)?;
        let record = Record {
            values: new_values.clone(),
        };
        current_root = btree_insert(pager, current_root, rowid, &record)?;

        fire_triggers(
            &plan.table_name,
            &crate::catalog::TriggerTiming::After,
            &crate::catalog::TriggerEvent::Update,
            Some(&old_named),
            Some(&new_named),
            pager,
            catalog,
        )?;

        if plan.returning.is_some() {
            // For RETURNING, expose the post-update row values; rowid alias
            // columns get the rowid filled in.
            let mut row_for_returning = new_values.clone();
            for (i, c) in plan.table_columns.iter().enumerate() {
                if c.is_rowid_alias {
                    row_for_returning[i] = Value::Integer(rowid);
                }
            }
            returning_values.push(row_for_returning);
        }
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

use rsqlite_storage::btree::{BTreeCursor, btree_delete, btree_index_delete};
use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::is_truthy;
use crate::planner::DeletePlan;
use crate::types::Row;

use super::ExecResult;
use super::constraints::check_foreign_key_delete;
use super::eval::eval_expr;
use super::helpers::{
    build_index_key, build_returning_result, get_table_indexes, row_values_for_rowid,
};
use super::state::set_changes;
use super::trigger::fire_triggers;

pub(super) fn execute_delete(
    plan: &DeletePlan,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<ExecResult> {
    let column_names: Vec<String> = plan.table_columns.iter().map(|c| c.name.clone()).collect();

    let mut cursor = BTreeCursor::new(pager, plan.root_page);
    let btree_rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

    let mut to_delete: Vec<i64> = Vec::new();
    // Track per-rowid sort keys for LIMIT/ORDER BY ordering.
    let mut sort_keys: Vec<(i64, Vec<crate::types::Value>)> = Vec::new();

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

        let row = Row { values: row_values };

        let matches = match &plan.predicate {
            Some(pred) => {
                let val = eval_expr(pred, &row, &column_names, pager, catalog)?;
                is_truthy(&val)
            }
            None => true,
        };

        if matches {
            to_delete.push(btree_row.rowid);
            if !plan.order_by.is_empty() {
                let keys: Vec<crate::types::Value> = plan
                    .order_by
                    .iter()
                    .map(|sk| eval_expr(&sk.expr, &row, &column_names, pager, catalog))
                    .collect::<Result<Vec<_>>>()?;
                sort_keys.push((btree_row.rowid, keys));
            }
        }
    }

    // ORDER BY + LIMIT: sort the matched rowids, then truncate.
    if !plan.order_by.is_empty() {
        sort_keys.sort_by(|a, b| {
            for (i, sk) in plan.order_by.iter().enumerate() {
                let cmp = crate::eval_helpers::compare(&a.1[i], &b.1[i]);
                let ordering = if cmp < 0 {
                    std::cmp::Ordering::Less
                } else if cmp > 0 {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                };
                let ordering = if sk.descending {
                    ordering.reverse()
                } else {
                    ordering
                };
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });
        to_delete = sort_keys.iter().map(|(r, _)| *r).collect();
    }
    if let Some(limit) = plan.limit {
        to_delete.truncate(limit as usize);
    }

    let rows_affected = to_delete.len() as u64;
    let table_indexes = get_table_indexes(catalog, &plan.table_name);

    if let Some(table_def) = catalog.get_table(&plan.table_name) {
        let table_columns_def = table_def.columns.clone();
        for &rowid in &to_delete {
            let old_values = row_values_for_rowid(&btree_rows, rowid, &plan.table_columns);
            check_foreign_key_delete(
                rowid,
                &old_values,
                &plan.table_name,
                &table_columns_def,
                pager,
                catalog,
            )?;
        }
    }

    let mut returning_values: Vec<Vec<Value>> = Vec::new();
    for rowid in to_delete {
        let old_values = row_values_for_rowid(&btree_rows, rowid, &plan.table_columns);
        let old_named: Vec<(String, Value)> = plan
            .table_columns
            .iter()
            .zip(old_values.iter())
            .map(|(c, v)| (c.name.clone(), v.clone()))
            .collect();

        fire_triggers(
            &plan.table_name,
            &crate::catalog::TriggerTiming::Before,
            &crate::catalog::TriggerEvent::Delete,
            Some(&old_named),
            None,
            pager,
            catalog,
        )?;

        if plan.returning.is_some() {
            returning_values.push(old_values.clone());
        }

        for (idx_root, idx_col_indices) in &table_indexes {
            let old_key = build_index_key(&old_values, idx_col_indices, &plan.table_columns, rowid);
            let _ = btree_index_delete(pager, *idx_root, &old_key);
        }
        btree_delete(pager, plan.root_page, rowid)?;

        fire_triggers(
            &plan.table_name,
            &crate::catalog::TriggerTiming::After,
            &crate::catalog::TriggerEvent::Delete,
            Some(&old_named),
            None,
            pager,
            catalog,
        )?;
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

use rsqlite_storage::btree::{btree_delete, btree_index_delete, BTreeCursor};
use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::is_truthy;
use crate::planner::DeletePlan;
use crate::types::Row;

use super::constraints::check_foreign_key_delete;
use super::eval::eval_expr;
use super::helpers::{build_index_key, get_table_indexes, row_values_for_rowid};
use super::state::set_changes;
use super::trigger::fire_triggers;
use super::ExecResult;

pub(super) fn execute_delete(plan: &DeletePlan, pager: &mut Pager, catalog: &Catalog) -> Result<ExecResult> {
    let column_names: Vec<String> = plan.table_columns.iter().map(|c| c.name.clone()).collect();

    let mut cursor = BTreeCursor::new(pager, plan.root_page);
    let btree_rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

    let mut to_delete: Vec<i64> = Vec::new();

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

        let row = Row {
            values: row_values,
        };

        let matches = match &plan.predicate {
            Some(pred) => {
                let val = eval_expr(pred, &row, &column_names, pager, catalog)?;
                is_truthy(&val)
            }
            None => true,
        };

        if matches {
            to_delete.push(btree_row.rowid);
        }
    }

    let rows_affected = to_delete.len() as u64;
    let table_indexes = get_table_indexes(catalog, &plan.table_name);

    if let Some(table_def) = catalog.get_table(&plan.table_name) {
        let table_columns_def = table_def.columns.clone();
        for &rowid in &to_delete {
            let old_values = row_values_for_rowid(&btree_rows, rowid, &plan.table_columns);
            check_foreign_key_delete(rowid, &old_values, &plan.table_name, &table_columns_def, pager, catalog)?;
        }
    }

    for rowid in to_delete {
        let old_values = row_values_for_rowid(&btree_rows, rowid, &plan.table_columns);
        let old_named: Vec<(String, Value)> = plan.table_columns.iter()
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

        for (idx_root, idx_col_indices) in &table_indexes {
            let old_key =
                build_index_key(&old_values, idx_col_indices, &plan.table_columns, rowid);
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
    Ok(ExecResult { rows_affected })
}

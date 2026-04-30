mod aggregate;
mod autoincrement;
mod constraints;
mod ddl;
mod delete;
mod eval;
mod helpers;
mod insert;
mod pragma;
mod query;
mod scan;
mod sort;
pub(crate) mod state;
mod trigger;
mod update;
mod vacuum;
mod window;

use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::is_truthy;
use crate::planner::Plan;
use crate::types::{QueryResult, Row};

pub use state::{
    clear_params, get_changes_pub, get_last_insert_rowid_pub, get_total_changes_pub,
    set_foreign_keys_enabled, set_params,
};
pub use pragma::execute_pragma;

use aggregate::execute_aggregate;
use ddl::{
    execute_alter_add_column, execute_alter_rename, execute_create_index, execute_create_table,
    execute_create_table_as_select, execute_create_view, execute_drop_index, execute_drop_table,
    execute_drop_view,
};
use delete::execute_delete;
use eval::eval_expr;
use insert::execute_insert;
use query::{execute_join, execute_project};
use scan::{execute_index_range_scan, execute_index_scan, execute_scan};
use sort::{compare_rows_by_keys, row_hash_key};
use trigger::{execute_create_trigger, execute_drop_trigger};
use update::execute_update;
use vacuum::execute_vacuum;
use window::execute_window;

#[derive(Debug)]
pub struct ExecResult {
    pub rows_affected: u64,
}

pub fn execute(plan: &Plan, pager: &mut Pager, catalog: &Catalog) -> Result<QueryResult> {
    match plan {
        Plan::SingleRow => Ok(QueryResult {
            columns: vec![],
            rows: vec![Row { values: vec![] }],
        }),
        Plan::RecursiveCte { name, column_names, anchor, recursive } => {
            let anchor_result = execute(anchor, pager, catalog)?;
            let columns = if !column_names.is_empty() {
                column_names.clone()
            } else {
                anchor_result.columns.clone()
            };
            let mut all_rows = anchor_result.rows.clone();
            let mut working_rows = anchor_result.rows;

            const MAX_ITERATIONS: usize = 1000;
            for _ in 0..MAX_ITERATIONS {
                if working_rows.is_empty() {
                    break;
                }
                state::cte_working_set_insert(name.clone(), QueryResult {
                    columns: columns.clone(),
                    rows: working_rows.clone(),
                });
                let new_result = execute(recursive, pager, catalog)?;
                state::cte_working_set_remove(name);
                if new_result.rows.is_empty() {
                    break;
                }
                all_rows.extend(new_result.rows.clone());
                working_rows = new_result.rows;
            }
            Ok(QueryResult { columns, rows: all_rows })
        }
        Plan::RecursiveCteRef { name, .. } => {
            let result = state::cte_working_set_get(name);
            match result {
                Some(qr) => Ok(qr),
                None => Err(Error::Other(format!("recursive CTE '{}' not in scope", name))),
            }
        }
        Plan::Union { left, right, all } => {
            let left_result = execute(left, pager, catalog)?;
            let right_result = execute(right, pager, catalog)?;
            let mut rows = left_result.rows;
            if *all {
                rows.extend(right_result.rows);
            } else {
                for row in right_result.rows {
                    let is_dup = rows.iter().any(|existing| existing.values == row.values);
                    if !is_dup {
                        rows.push(row);
                    }
                }
            }
            Ok(QueryResult {
                columns: left_result.columns,
                rows,
            })
        }
        Plan::Intersect { left, right, all } => {
            let left_result = execute(left, pager, catalog)?;
            let right_result = execute(right, pager, catalog)?;
            let mut rows = Vec::new();
            // INTERSECT keeps left rows that also appear in right; INTERSECT (no
            // ALL) deduplicates.
            for row in &left_result.rows {
                let in_right = right_result
                    .rows
                    .iter()
                    .any(|r| r.values == row.values);
                if !in_right {
                    continue;
                }
                if *all {
                    rows.push(row.clone());
                } else if !rows.iter().any(|r: &crate::types::Row| r.values == row.values) {
                    rows.push(row.clone());
                }
            }
            Ok(QueryResult {
                columns: left_result.columns,
                rows,
            })
        }
        Plan::Except { left, right, all } => {
            let left_result = execute(left, pager, catalog)?;
            let right_result = execute(right, pager, catalog)?;
            let mut rows = Vec::new();
            // EXCEPT keeps left rows that do NOT appear in right; without ALL
            // it also deduplicates the result.
            for row in &left_result.rows {
                let in_right = right_result
                    .rows
                    .iter()
                    .any(|r| r.values == row.values);
                if in_right {
                    continue;
                }
                if *all {
                    rows.push(row.clone());
                } else if !rows.iter().any(|r: &crate::types::Row| r.values == row.values) {
                    rows.push(row.clone());
                }
            }
            Ok(QueryResult {
                columns: left_result.columns,
                rows,
            })
        }
        Plan::Project { input, outputs } => execute_project(input, outputs, pager, catalog),
        Plan::Filter { input, predicate } => {
            let inner = execute(input, pager, catalog)?;
            let input_columns = &inner.columns;
            let mut filtered_rows = Vec::new();
            for row in &inner.rows {
                let val = eval_expr(predicate, row, input_columns, pager, catalog)?;
                if is_truthy(&val) {
                    filtered_rows.push(row.clone());
                }
            }
            Ok(QueryResult {
                columns: inner.columns,
                rows: filtered_rows,
            })
        }
        Plan::Scan {
            root_page,
            columns,
            ..
        } => execute_scan(*root_page, columns, pager),
        Plan::IndexScan {
            table_root_page,
            index_root_page,
            columns,
            index_columns,
            lookup_values,
            ..
        } => execute_index_scan(
            *table_root_page,
            *index_root_page,
            columns,
            index_columns,
            lookup_values,
            pager,
            catalog,
        ),
        Plan::IndexRangeScan {
            table_root_page,
            index_root_page,
            columns,
            index_column,
            lower_bound,
            upper_bound,
            ..
        } => execute_index_range_scan(
            *table_root_page,
            *index_root_page,
            columns,
            index_column,
            lower_bound.as_ref(),
            upper_bound.as_ref(),
            pager,
            catalog,
        ),
        Plan::Sort { input, keys } => {
            let mut inner = execute(input, pager, catalog)?;
            let columns = inner.columns.clone();
            inner.rows.sort_by(|a, b| {
                compare_rows_by_keys(a, b, keys, &columns, pager, catalog)
            });
            Ok(inner)
        }
        Plan::Limit {
            input,
            limit,
            offset,
        } => {
            let inner = execute(input, pager, catalog)?;
            let offset = *offset as usize;
            let rows: Vec<Row> = match limit {
                Some(n) => inner.rows.into_iter().skip(offset).take(*n as usize).collect(),
                None => inner.rows.into_iter().skip(offset).collect(),
            };
            Ok(QueryResult {
                columns: inner.columns,
                rows,
            })
        }
        Plan::Distinct { input } => {
            let inner = execute(input, pager, catalog)?;
            let mut seen = std::collections::HashSet::new();
            let mut unique_rows = Vec::new();
            for row in inner.rows {
                let key = row_hash_key(&row);
                if seen.insert(key) {
                    unique_rows.push(row);
                }
            }
            Ok(QueryResult {
                columns: inner.columns,
                rows: unique_rows,
            })
        }
        Plan::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
        } => execute_join(left, right, condition.as_ref(), *join_type, pager, catalog),
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
        } => execute_aggregate(input, group_by, aggregates, having.as_ref(), pager, catalog),
        Plan::Pragma { .. } => Err(Error::Other(
            "pragmas must be routed through execute_pragma".to_string(),
        )),
        Plan::Window { input, window_exprs, output_columns } => {
            execute_window(input, window_exprs, output_columns, pager, catalog)
        }
        Plan::CreateTable(_)
        | Plan::CreateIndex(_)
        | Plan::Insert(_)
        | Plan::Update(_)
        | Plan::Delete(_)
        | Plan::AlterTableAddColumn { .. }
        | Plan::AlterTableRename { .. }
        | Plan::DropTable { .. }
        | Plan::DropIndex { .. }
        | Plan::DropView { .. }
        | Plan::CreateView { .. }
        | Plan::CreateTableAsSelect { .. }
        | Plan::Begin
        | Plan::Commit
        | Plan::Rollback
        | Plan::Savepoint(_)
        | Plan::Release(_)
        | Plan::RollbackTo(_)
        | Plan::Vacuum
        | Plan::CreateTrigger { .. }
        | Plan::DropTrigger { .. }
        | Plan::AttachDatabase { .. }
        | Plan::DetachDatabase { .. } => Err(Error::Other(
            "use execute_mut for DDL/DML statements".to_string(),
        )),
    }
}

pub fn execute_mut(
    plan: &Plan,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    match plan {
        Plan::CreateTable(ct) => execute_create_table(ct, pager, catalog),
        Plan::CreateIndex(ci) => execute_create_index(ci, pager, catalog),
        Plan::Insert(ins) => execute_insert(ins, pager, catalog),
        Plan::Update(upd) => execute_update(upd, pager, catalog),
        Plan::Delete(del) => execute_delete(del, pager, catalog),
        Plan::AlterTableAddColumn {
            table_name,
            column_name,
            column_type,
        } => execute_alter_add_column(table_name, column_name, column_type, pager, catalog),
        Plan::DropTable {
            table_name,
            if_exists,
        } => execute_drop_table(table_name, *if_exists, pager, catalog),
        Plan::DropIndex {
            index_name,
            if_exists,
        } => execute_drop_index(index_name, *if_exists, pager, catalog),
        Plan::AlterTableRename { old_name, new_name } => {
            execute_alter_rename(old_name, new_name, pager, catalog)
        }
        Plan::CreateView {
            name,
            sql,
            if_not_exists,
        } => execute_create_view(name, sql, *if_not_exists, pager, catalog),
        Plan::DropView { name, if_exists } => {
            execute_drop_view(name, *if_exists, pager, catalog)
        }
        Plan::CreateTableAsSelect {
            table_name,
            if_not_exists,
            query,
        } => execute_create_table_as_select(table_name, *if_not_exists, query, pager, catalog),
        Plan::Vacuum => execute_vacuum(pager, catalog),
        Plan::CreateTrigger { name, table_name, sql, if_not_exists } => {
            execute_create_trigger(name, table_name, sql, *if_not_exists, pager, catalog)
        }
        Plan::DropTrigger { name, if_exists } => {
            execute_drop_trigger(name, *if_exists, pager, catalog)
        }
        Plan::Begin => {
            pager.begin_transaction()?;
            Ok(ExecResult { rows_affected: 0 })
        }
        Plan::Commit => {
            pager.commit()?;
            Ok(ExecResult { rows_affected: 0 })
        }
        Plan::Rollback => {
            pager.rollback()?;
            Ok(ExecResult { rows_affected: 0 })
        }
        Plan::Savepoint(name) => {
            pager.savepoint(name)?;
            Ok(ExecResult { rows_affected: 0 })
        }
        Plan::Release(name) => {
            pager.release_savepoint(name)?;
            Ok(ExecResult { rows_affected: 0 })
        }
        Plan::RollbackTo(name) => {
            pager.rollback_to_savepoint(name)?;
            Ok(ExecResult { rows_affected: 0 })
        }
        _ => Err(Error::Other(
            "use execute for query statements".to_string(),
        )),
    }
}

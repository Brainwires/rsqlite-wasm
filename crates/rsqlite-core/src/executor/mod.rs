mod aggregate;
mod analyze;
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
mod table_function;
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

pub use pragma::execute_pragma;
pub use state::{
    clear_params, get_changes_pub, get_last_insert_rowid_pub, get_total_changes_pub,
    set_foreign_keys_enabled, set_params,
};

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

#[derive(Debug, Default)]
pub struct ExecResult {
    pub rows_affected: u64,
    /// Result rows when the DML carried a RETURNING clause. The dispatcher
    /// in Database unwraps this into a QueryResult for the caller.
    pub returning: Option<crate::types::QueryResult>,
}

impl ExecResult {
    pub fn affected(rows_affected: u64) -> Self {
        Self {
            rows_affected,
            returning: None,
        }
    }
}

pub fn execute(plan: &Plan, pager: &mut Pager, catalog: &Catalog) -> Result<QueryResult> {
    match plan {
        Plan::SingleRow => Ok(QueryResult {
            columns: vec![],
            rows: vec![Row::new(vec![])],
        }),
        Plan::TableFunction { name, args } => {
            table_function::execute_table_function(name, args, pager, catalog)
        }
        Plan::RecursiveCte {
            name,
            column_names,
            anchor,
            recursive,
        } => {
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
                state::cte_working_set_insert(
                    name.clone(),
                    QueryResult {
                        columns: columns.clone(),
                        rows: working_rows.clone(),
                    },
                );
                let new_result = execute(recursive, pager, catalog)?;
                state::cte_working_set_remove(name);
                if new_result.rows.is_empty() {
                    break;
                }
                all_rows.extend(new_result.rows.clone());
                working_rows = new_result.rows;
            }
            Ok(QueryResult {
                columns,
                rows: all_rows,
            })
        }
        Plan::RecursiveCteRef { name, .. } => {
            let result = state::cte_working_set_get(name);
            match result {
                Some(qr) => Ok(qr),
                None => Err(Error::Other(format!(
                    "recursive CTE '{}' not in scope",
                    name
                ))),
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
                let in_right = right_result.rows.iter().any(|r| r.values == row.values);
                if !in_right {
                    continue;
                }
                if *all {
                    rows.push(row.clone());
                } else if !rows
                    .iter()
                    .any(|r: &crate::types::Row| r.values == row.values)
                {
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
                let in_right = right_result.rows.iter().any(|r| r.values == row.values);
                if in_right {
                    continue;
                }
                if *all {
                    rows.push(row.clone());
                } else if !rows
                    .iter()
                    .any(|r: &crate::types::Row| r.values == row.values)
                {
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
            table, root_page, columns, ..
        } => {
            let mut result = execute_scan(table, *root_page, columns, pager, catalog)?;
            rehydrate_virtual_columns(&mut result, table, columns, pager, catalog)?;
            Ok(result)
        }
        Plan::IndexScan {
            table,
            table_root_page,
            index_root_page,
            columns,
            index_columns,
            lookup_values,
            ..
        } => {
            let mut result = execute_index_scan(
                *table_root_page,
                *index_root_page,
                columns,
                index_columns,
                lookup_values,
                pager,
                catalog,
            )?;
            rehydrate_virtual_columns(&mut result, table, columns, pager, catalog)?;
            Ok(result)
        }
        Plan::IndexRangeScan {
            table,
            table_root_page,
            index_root_page,
            columns,
            index_column,
            lower_bound,
            upper_bound,
            ..
        } => {
            let mut result = execute_index_range_scan(
                *table_root_page,
                *index_root_page,
                columns,
                index_column,
                lower_bound.as_ref(),
                upper_bound.as_ref(),
                pager,
                catalog,
            )?;
            rehydrate_virtual_columns(&mut result, table, columns, pager, catalog)?;
            Ok(result)
        }
        Plan::Sort { input, keys } => {
            let mut inner = execute(input, pager, catalog)?;
            let columns = inner.columns.clone();
            inner
                .rows
                .sort_by(|a, b| compare_rows_by_keys(a, b, keys, &columns, pager, catalog));
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
                Some(n) => inner
                    .rows
                    .into_iter()
                    .skip(offset)
                    .take(*n as usize)
                    .collect(),
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
        Plan::Window {
            input,
            window_exprs,
            output_columns,
        } => execute_window(input, window_exprs, output_columns, pager, catalog),
        Plan::VirtualScan { table, columns, .. } => execute_virtual_scan(table, columns, catalog),
        Plan::VecIndexNearest {
            table,
            query_expr,
            k,
        } => execute_vec_index_nearest(table, query_expr, *k, pager, catalog),
        Plan::VirtualFilteredScan {
            table,
            columns,
            rowids,
        } => execute_virtual_filtered_scan(table, columns, rowids, catalog),
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
        | Plan::Reindex { .. }
        | Plan::Analyze
        | Plan::CreateTrigger { .. }
        | Plan::DropTrigger { .. }
        | Plan::AttachDatabase { .. }
        | Plan::DetachDatabase { .. }
        | Plan::CreateVirtualTable { .. }
        | Plan::VirtualInsert { .. }
        | Plan::VirtualUpdate { .. }
        | Plan::VirtualDelete { .. } => Err(Error::Other(
            "use execute_mut for DDL/DML statements".to_string(),
        )),
    }
}

/// After a btree scan, replace any VIRTUAL generated-column placeholders
/// (NULL on disk) with values computed from the rest of the row. STORED
/// generated columns are persisted at write time and need no rehydration.
fn rehydrate_virtual_columns(
    result: &mut QueryResult,
    table_name: &str,
    columns: &[crate::planner::ColumnRef],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    let table_def = match catalog.get_table(table_name) {
        Some(td) => td,
        None => return Ok(()),
    };

    let mut virtual_targets: Vec<(usize, String)> = Vec::new();
    for (i, col) in columns.iter().enumerate() {
        if col.is_rowid_alias {
            continue;
        }
        if let Some(cat_col) = table_def
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&col.name))
            && let Some(g) = &cat_col.generated
            && !g.stored
        {
            virtual_targets.push((i, g.expr.clone()));
        }
    }

    if virtual_targets.is_empty() {
        return Ok(());
    }

    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    for (slot, expr_src) in &virtual_targets {
        let parsed = match rsqlite_parser::parse::parse_sql(&format!("SELECT {expr_src}")) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let expr_ast = parsed.into_iter().next().and_then(|stmt| {
            if let sqlparser::ast::Statement::Query(q) = stmt
                && let sqlparser::ast::SetExpr::Select(sel) = *q.body
                && let Some(sqlparser::ast::SelectItem::UnnamedExpr(e)) =
                    sel.projection.into_iter().next()
            {
                Some(e)
            } else {
                None
            }
        });
        let Some(ast) = expr_ast else { continue };
        let plan_expr = match crate::planner::plan_expr(&ast, columns, catalog) {
            Ok(p) => p,
            Err(_) => continue,
        };
        for row in &mut result.rows {
            let v = eval_expr(&plan_expr, row, &col_names, pager, catalog)?;
            if *slot < row.values.len() {
                row.values[*slot] = v;
            }
        }
    }

    Ok(())
}

/// vec_index pushdown: evaluate the query vector once at run time and
/// dispatch to HNSW search via VecIndexTable::nearest. Emits one row
/// per neighbor, with the table's `vector` column set so projections
/// referencing it still work.
fn execute_vec_index_nearest(
    table_name: &str,
    query_expr: &crate::planner::PlanExpr,
    k: usize,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let vt = catalog
        .virtual_tables
        .get(&table_name.to_lowercase())
        .ok_or_else(|| Error::Other(format!("virtual table not found: {table_name}")))?;
    let any = vt
        .instance
        .as_any()
        .ok_or_else(|| Error::Other("vec_index instance does not expose Any".into()))?;
    let table = any
        .downcast_ref::<crate::vtab::vec_index::VecIndexTable>()
        .ok_or_else(|| Error::Other(format!("table {table_name} is not vec_index")))?;
    let empty_row = Row::new(vec![]);
    let qval = eval_expr(query_expr, &empty_row, &[], pager, catalog)?;
    let blob = match &qval {
        crate::types::Value::Blob(b) => b.clone(),
        _ => {
            return Err(Error::Other(
                "vec_index: query vector must be a BLOB".into(),
            ));
        }
    };
    let query = crate::eval_helpers::blob_to_f32_vec(&blob)?;
    let neighbors = table.nearest(&query, k)?;
    let rows: Vec<Row> = neighbors
        .into_iter()
        .map(|(rid, _)| {
            Row::with_rowid(
                vec![crate::types::Value::Integer(rid)],
                rid,
            )
        })
        .collect();
    Ok(QueryResult {
        columns: vec!["rowid".to_string()],
        rows,
    })
}

fn execute_virtual_scan(
    table_name: &str,
    columns: &[crate::planner::ColumnRef],
    catalog: &Catalog,
) -> Result<QueryResult> {
    let vt = catalog
        .virtual_tables
        .get(&table_name.to_lowercase())
        .ok_or_else(|| Error::Other(format!("virtual table not found: {table_name}")))?;
    let rows = vt.instance.scan()?;
    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    Ok(QueryResult {
        columns: column_names,
        rows,
    })
}

/// Materialize a module-pushed-down virtual scan: scan the vtab once,
/// keep only rows whose rowid is in the module-supplied set. Preserves
/// the pre-sorted-ascending order of `rowids`.
fn execute_virtual_filtered_scan(
    table_name: &str,
    columns: &[crate::planner::ColumnRef],
    rowids: &[i64],
    catalog: &Catalog,
) -> Result<QueryResult> {
    let vt = catalog
        .virtual_tables
        .get(&table_name.to_lowercase())
        .ok_or_else(|| Error::Other(format!("virtual table not found: {table_name}")))?;
    let scan_rows = vt.instance.scan()?;
    let wanted: std::collections::HashSet<i64> = rowids.iter().copied().collect();
    let mut by_rowid: std::collections::HashMap<i64, Row> = std::collections::HashMap::new();
    for row in scan_rows {
        if let Some(rid) = row.rowid {
            if wanted.contains(&rid) {
                by_rowid.insert(rid, row);
            }
        }
    }
    let mut out = Vec::with_capacity(rowids.len());
    for rid in rowids {
        if let Some(row) = by_rowid.remove(rid) {
            out.push(row);
        }
    }
    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    Ok(QueryResult {
        columns: column_names,
        rows: out,
    })
}

fn execute_virtual_insert(
    table_name: &str,
    _columns: &[String],
    rows: &[Vec<crate::planner::PlanExpr>],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<ExecResult> {
    let vt = catalog
        .virtual_tables
        .get(&table_name.to_lowercase())
        .ok_or_else(|| Error::Other(format!("virtual table not found: {table_name}")))?;
    let empty_row = Row::new(vec![]);
    let mut affected = 0u64;
    for row in rows {
        let values: Vec<crate::types::Value> = row
            .iter()
            .map(|e| eval::eval_expr(e, &empty_row, &[], pager, catalog))
            .collect::<Result<Vec<_>>>()?;
        vt.instance.insert(&values)?;
        affected += 1;
    }
    persist_fts5_if_needed(table_name, vt, pager, catalog)?;
    Ok(ExecResult::affected(affected))
}

fn execute_virtual_update(
    table_name: &str,
    columns: &[crate::planner::ColumnRef],
    assignments: &[(String, crate::planner::PlanExpr)],
    predicate: Option<&crate::planner::PlanExpr>,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<ExecResult> {
    let vt = catalog
        .virtual_tables
        .get(&table_name.to_lowercase())
        .ok_or_else(|| Error::Other(format!("virtual table not found: {table_name}")))?;
    let rows = vt.instance.scan()?;
    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut affected = 0u64;
    for row in rows {
        let rowid = row
            .rowid
            .ok_or_else(|| Error::Other("virtual-table row missing rowid".into()))?;
        if let Some(p) = predicate {
            let v = eval::eval_expr(p, &row, &column_names, pager, catalog)?;
            if !crate::eval_helpers::is_truthy(&v) {
                continue;
            }
        }
        let mut new_values = row.values.clone();
        for (col_name, expr) in assignments {
            let idx = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| {
                    Error::Other(format!("unknown column in UPDATE: {col_name}"))
                })?;
            let new = eval::eval_expr(expr, &row, &column_names, pager, catalog)?;
            if idx < new_values.len() {
                new_values[idx] = new;
            }
        }
        vt.instance.update(rowid, &new_values)?;
        affected += 1;
    }
    persist_fts5_if_needed(table_name, vt, pager, catalog)?;
    Ok(ExecResult::affected(affected))
}

fn execute_virtual_delete(
    table_name: &str,
    columns: &[crate::planner::ColumnRef],
    predicate: Option<&crate::planner::PlanExpr>,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<ExecResult> {
    let vt = catalog
        .virtual_tables
        .get(&table_name.to_lowercase())
        .ok_or_else(|| Error::Other(format!("virtual table not found: {table_name}")))?;
    let rows = vt.instance.scan()?;
    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut to_delete = Vec::new();
    for row in rows {
        let rowid = row
            .rowid
            .ok_or_else(|| Error::Other("virtual-table row missing rowid".into()))?;
        if let Some(p) = predicate {
            let v = eval::eval_expr(p, &row, &column_names, pager, catalog)?;
            if !crate::eval_helpers::is_truthy(&v) {
                continue;
            }
        }
        to_delete.push(rowid);
    }
    let mut affected = 0u64;
    for rid in to_delete {
        vt.instance.delete(rid)?;
        affected += 1;
    }
    persist_fts5_if_needed(table_name, vt, pager, catalog)?;
    Ok(ExecResult::affected(affected))
}

fn persist_fts5_if_needed(
    table_name: &str,
    vt: &crate::catalog::VirtualTableDef,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    if !vt.module.eq_ignore_ascii_case("fts5") {
        return Ok(());
    }
    if let Some(any) = vt.instance.as_any() {
        if let Some(table) = any.downcast_ref::<crate::vtab::fts5::Fts5Table>() {
            crate::vtab::fts5::persist::write_snapshot(table, table_name, pager, catalog)?;
        }
    }
    Ok(())
}

fn execute_create_virtual_table(
    name: &str,
    module: &str,
    args: &[String],
    if_not_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    let key = name.to_lowercase();
    if catalog.virtual_tables.contains_key(&key) {
        if if_not_exists {
            return Ok(ExecResult::affected(0));
        }
        return Err(Error::Other(format!("virtual table {name} already exists")));
    }
    let module_def = crate::vtab::lookup_module(module).ok_or_else(|| {
        Error::Other(format!("virtual-table module not registered: {module}"))
    })?;
    let instance = module_def.create(name, args)?;
    catalog.virtual_tables.insert(
        key.clone(),
        crate::catalog::VirtualTableDef {
            name: name.to_string(),
            module: module.to_string(),
            args: args.to_vec(),
            instance: instance.clone(),
        },
    );

    // FTS5 persistence: stand up the shadow table + schema marker and
    // rehydrate from any existing snapshot. Other modules opt out by
    // leaving `snapshot` returning `None`.
    if module.eq_ignore_ascii_case("fts5") {
        if let Some(any) = instance.as_any() {
            if let Some(table) = any.downcast_ref::<crate::vtab::fts5::Fts5Table>() {
                let create_sql = render_fts5_create_sql(name, args);
                crate::vtab::fts5::persist::ensure_persistence(
                    name, &create_sql, pager, catalog,
                )?;
                crate::vtab::fts5::persist::restore_if_present(
                    table, name, pager, catalog,
                )?;
            }
        }
    }
    Ok(ExecResult::affected(0))
}

fn render_fts5_create_sql(name: &str, args: &[String]) -> String {
    let inner = args.join(", ");
    format!("CREATE VIRTUAL TABLE {name} USING fts5({inner})")
}

pub fn execute_mut(plan: &Plan, pager: &mut Pager, catalog: &mut Catalog) -> Result<ExecResult> {
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
        Plan::DropView { name, if_exists } => execute_drop_view(name, *if_exists, pager, catalog),
        Plan::CreateTableAsSelect {
            table_name,
            if_not_exists,
            query,
        } => execute_create_table_as_select(table_name, *if_not_exists, query, pager, catalog),
        Plan::Vacuum => execute_vacuum(pager, catalog),
        Plan::CreateVirtualTable {
            name,
            module,
            args,
            if_not_exists,
        } => execute_create_virtual_table(name, module, args, *if_not_exists, pager, catalog),
        Plan::VirtualInsert {
            table,
            columns,
            rows,
        } => execute_virtual_insert(table, columns, rows, pager, catalog),
        Plan::VirtualUpdate {
            table,
            columns,
            assignments,
            predicate,
        } => execute_virtual_update(
            table,
            columns,
            assignments,
            predicate.as_ref(),
            pager,
            catalog,
        ),
        Plan::VirtualDelete {
            table,
            columns,
            predicate,
        } => execute_virtual_delete(table, columns, predicate.as_ref(), pager, catalog),
        // REINDEX is currently a no-op: our btree implementation doesn't
        // suffer from the corruption modes (collation changes, etc.) that
        // real SQLite addresses with REINDEX. Accepted for tool compat.
        Plan::Reindex { .. } => Ok(ExecResult::affected(0)),
        // ANALYZE would populate sqlite_stat1 for a cost-based planner; our
        // planner is rule-based, so this is a no-op stub.
        Plan::Analyze => analyze::execute_analyze(pager, catalog),
        Plan::CreateTrigger {
            name,
            table_name,
            sql,
            if_not_exists,
        } => execute_create_trigger(name, table_name, sql, *if_not_exists, pager, catalog),
        Plan::DropTrigger { name, if_exists } => {
            execute_drop_trigger(name, *if_exists, pager, catalog)
        }
        Plan::Begin => {
            pager.begin_transaction()?;
            Ok(ExecResult::affected(0))
        }
        Plan::Commit => {
            pager.commit()?;
            Ok(ExecResult::affected(0))
        }
        Plan::Rollback => {
            pager.rollback()?;
            Ok(ExecResult::affected(0))
        }
        Plan::Savepoint(name) => {
            pager.savepoint(name)?;
            Ok(ExecResult::affected(0))
        }
        Plan::Release(name) => {
            pager.release_savepoint(name)?;
            Ok(ExecResult::affected(0))
        }
        Plan::RollbackTo(name) => {
            pager.rollback_to_savepoint(name)?;
            Ok(ExecResult::affected(0))
        }
        _ => Err(Error::Other("use execute for query statements".to_string())),
    }
}

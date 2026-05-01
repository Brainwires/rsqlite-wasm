//! `ANALYZE` implementation.
//!
//! Builds (or refreshes) `sqlite_stat1`, the SQLite-format statistics table
//! the optimizer can consult. The schema matches SQLite:
//!
//! ```sql
//! CREATE TABLE sqlite_stat1(tbl TEXT, idx TEXT, stat TEXT)
//! ```
//!
//! For each table we write one row with the row count, plus one row per
//! index with `<row_count> 1` (the second column is the average rows per
//! distinct index-prefix lookup; we use 1 as a conservative placeholder
//! pending a real distinct-value scan).
//!
//! The planner doesn't yet consume these stats — it stays rule-based for
//! v0.1 — so ANALYZE today is purely about being SQLite-tool compatible
//! (`sqlite3` CLI commands like `.stats` and external query analyzers can
//! read what we write). Wiring the stats into a cost-aware planner is
//! tracked in `LIMITATIONS.md`.

use rsqlite_storage::btree::{BTreeCursor, IndexCursor};
use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};

use super::ExecResult;

/// True for tables that the engine manages internally and shouldn't appear
/// in `sqlite_stat1` (matches SQLite's behavior of skipping its own
/// metadata tables).
fn is_internal_table(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower == "sqlite_stat1" || lower == "sqlite_sequence" || lower.starts_with("sqlite_")
}

fn run_sql(sql: &str, pager: &mut Pager, catalog: &mut Catalog) -> Result<()> {
    let stmts = rsqlite_parser::parse::parse_sql(sql)?;
    for stmt in stmts {
        let plan = crate::planner::plan_statement(&stmt, catalog)?;
        super::execute_mut(&plan, pager, catalog)?;
    }
    Ok(())
}

fn run_sql_with_params(
    sql: &str,
    params: Vec<Value>,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<()> {
    let stmts = rsqlite_parser::parse::parse_sql(sql)?;
    let mut first = true;
    for stmt in stmts {
        let plan = crate::planner::plan_statement(&stmt, catalog)?;
        if first {
            super::set_params(params.clone());
            first = false;
        }
        super::execute_mut(&plan, pager, catalog)?;
    }
    super::clear_params();
    Ok(())
}

pub(super) fn execute_analyze(pager: &mut Pager, catalog: &mut Catalog) -> Result<ExecResult> {
    // Make sure sqlite_stat1 exists. CREATE TABLE IF NOT EXISTS is a no-op
    // when the table already exists, and the catalog refreshes itself
    // either way.
    run_sql(
        "CREATE TABLE IF NOT EXISTS sqlite_stat1(tbl TEXT, idx TEXT, stat TEXT)",
        pager,
        catalog,
    )?;
    run_sql("DELETE FROM sqlite_stat1", pager, catalog)?;

    // Snapshot the table list before we start mutating sqlite_stat1 — the
    // catalog HashMap iteration order isn't stable across mutations.
    let table_names: Vec<String> = catalog
        .all_tables()
        .filter(|t| !is_internal_table(&t.name))
        .map(|t| t.name.clone())
        .collect();

    for table_name in table_names {
        let table_def = match catalog.get_table(&table_name) {
            Some(t) => t.clone(),
            None => continue,
        };

        let row_count = {
            let mut cursor = BTreeCursor::new(pager, table_def.root_page);
            cursor
                .collect_all()
                .map_err(|e| Error::Other(e.to_string()))?
                .len() as i64
        };

        // Per-table stat: idx column is NULL.
        run_sql_with_params(
            "INSERT INTO sqlite_stat1 VALUES (?, NULL, ?)",
            vec![
                Value::Text(table_name.clone()),
                Value::Text(row_count.to_string()),
            ],
            pager,
            catalog,
        )?;

        // Per-index stat for every index on this table. We approximate the
        // "average rows per distinct lookup" as 1 — accurate when each
        // index entry is unique, and a safe lower bound otherwise.
        let index_defs: Vec<(String, u32)> = catalog
            .indexes
            .values()
            .filter(|i| i.table_name.eq_ignore_ascii_case(&table_name))
            .map(|i| (i.name.clone(), i.root_page))
            .collect();

        for (idx_name, idx_root) in index_defs {
            let idx_count = {
                let mut idx_cursor = IndexCursor::new(pager, idx_root);
                idx_cursor
                    .collect_all()
                    .map_err(|e| Error::Other(e.to_string()))?
                    .len() as i64
            };
            let stat = format!("{idx_count} 1");
            run_sql_with_params(
                "INSERT INTO sqlite_stat1 VALUES (?, ?, ?)",
                vec![
                    Value::Text(table_name.clone()),
                    Value::Text(idx_name),
                    Value::Text(stat),
                ],
                pager,
                catalog,
            )?;
        }
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }
    Ok(ExecResult::affected(0))
}

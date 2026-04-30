use rsqlite_storage::btree::{delete_schema_entries, insert_schema_entry};
use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::{Catalog, TriggerEvent, TriggerTiming};
use crate::error::{Error, Result};
use crate::eval_helpers::is_truthy;
use crate::planner::Plan;

use super::ExecResult;

pub(super) fn execute_create_trigger(
    name: &str,
    table_name: &str,
    encoded: &str,
    if_not_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if catalog.triggers.contains_key(&name.to_lowercase()) {
        if if_not_exists {
            return Ok(ExecResult { rows_affected: 0 });
        }
        return Err(Error::Other(format!("trigger {name} already exists")));
    }
    if catalog.get_table(table_name).is_none() {
        return Err(Error::Other(format!("no such table: {table_name}")));
    }

    let parts: Vec<&str> = encoded.splitn(7, '|').collect();
    if parts.len() != 7 {
        return Err(Error::Other("invalid trigger encoding".to_string()));
    }
    let timing_str = parts[2];
    let event_str = parts[3];
    let when_str = parts[5];
    let body_str = parts[6];

    let sql = format!(
        "CREATE TRIGGER {name} {timing_str} {event_str} ON {table_name} FOR EACH ROW{when} BEGIN {body_str} END",
        when = if when_str.is_empty() { String::new() } else { format!(" WHEN {when_str}") },
    );

    let was_in_txn = pager.in_transaction();
    if !was_in_txn {
        pager.begin_transaction()?;
    }
    insert_schema_entry(pager, "trigger", name, table_name, 0, &sql)?;
    if !was_in_txn {
        pager.commit()?;
    }
    catalog.reload(pager)?;
    Ok(ExecResult { rows_affected: 0 })
}

pub(super) fn execute_drop_trigger(
    name: &str,
    if_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if !catalog.triggers.contains_key(&name.to_lowercase()) {
        if if_exists {
            return Ok(ExecResult { rows_affected: 0 });
        }
        return Err(Error::Other(format!("no such trigger: {name}")));
    }

    let was_in_txn = pager.in_transaction();
    if !was_in_txn {
        pager.begin_transaction()?;
    }
    delete_schema_entries(pager, name)?;
    if !was_in_txn {
        pager.commit()?;
    }
    catalog.reload(pager)?;
    Ok(ExecResult { rows_affected: 0 })
}

pub(super) fn fire_triggers(
    table_name: &str,
    timing: &TriggerTiming,
    event: &TriggerEvent,
    old_row: Option<&[(String, Value)]>,
    new_row: Option<&[(String, Value)]>,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    let triggers: Vec<_> = catalog.triggers_for_table(table_name, timing, event)
        .into_iter().cloned().collect();
    if triggers.is_empty() {
        return Ok(());
    }

    let depth = super::state::trigger_depth_get();
    if depth >= 32 {
        return Err(Error::Other("too many levels of trigger recursion".to_string()));
    }

    for trigger in &triggers {
        if let Some(ref cond) = trigger.when_condition {
            let resolved_cond = resolve_trigger_references(cond, old_row, new_row);
            let cond_sql = format!("SELECT ({resolved_cond})");
            let stmts = match rsqlite_parser::parse::parse_sql(&cond_sql) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let cond_plan = match crate::planner::plan_statement(&stmts[0], catalog) {
                Ok(p) => p,
                Err(_) => continue,
            };
            match super::execute(&cond_plan, pager, catalog) {
                Ok(qr) => {
                    if qr.rows.is_empty() || !is_truthy(&qr.rows[0].values[0]) {
                        continue;
                    }
                }
                Err(_) => continue,
            }
        }

        super::state::trigger_depth_inc();

        let body_stmts: Vec<&str> = trigger.body_sql.split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        for body_sql in body_stmts {
            let resolved = resolve_trigger_references(body_sql, old_row, new_row);
            match rsqlite_parser::parse::parse_sql(&resolved) {
                Ok(stmts) => {
                    for stmt in &stmts {
                        let plan = crate::planner::plan_statement(stmt, catalog)?;
                        if matches!(plan, Plan::Insert(_) | Plan::Update(_) | Plan::Delete(_)) {
                            let mut cat_clone = catalog.clone();
                            super::execute_mut(&plan, pager, &mut cat_clone)?;
                        } else {
                            let _ = super::execute(&plan, pager, catalog);
                        }
                    }
                }
                Err(e) => {
                    super::state::trigger_depth_dec();
                    return Err(Error::Other(format!("trigger body parse error: {e}")));
                }
            }
        }

        super::state::trigger_depth_dec();
    }
    Ok(())
}

fn resolve_trigger_references(
    sql: &str,
    old_row: Option<&[(String, Value)]>,
    new_row: Option<&[(String, Value)]>,
) -> String {
    let mut result = sql.to_string();
    if let Some(new_vals) = new_row {
        for (col, val) in new_vals {
            let patterns = [
                format!("NEW.{col}"),
                format!("new.{col}"),
                format!("New.{col}"),
                format!("NEW.{}", col.to_lowercase()),
                format!("new.{}", col.to_lowercase()),
            ];
            let replacement = super::helpers::value_to_sql_literal(val);
            for pat in &patterns {
                result = result.replace(pat, &replacement);
            }
        }
    }
    if let Some(old_vals) = old_row {
        for (col, val) in old_vals {
            let patterns = [
                format!("OLD.{col}"),
                format!("old.{col}"),
                format!("Old.{col}"),
                format!("OLD.{}", col.to_lowercase()),
                format!("old.{}", col.to_lowercase()),
            ];
            let replacement = super::helpers::value_to_sql_literal(val);
            for pat in &patterns {
                result = result.replace(pat, &replacement);
            }
        }
    }
    result
}

use rsqlite_storage::btree::{
    BTreeCursor, btree_create_index, btree_create_table, btree_delete, btree_index_insert,
    btree_insert, delete_schema_entries, insert_schema_entry,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::planner::{CreateIndexPlan, CreateTablePlan, Plan};

use super::ExecResult;

pub(super) fn execute_create_table(
    plan: &CreateTablePlan,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if plan.if_not_exists && catalog.get_table(&plan.table_name).is_some() {
        return Ok(ExecResult::affected(0));
    }

    if catalog.get_table(&plan.table_name).is_some() {
        return Err(Error::Other(format!(
            "table {} already exists",
            plan.table_name
        )));
    }

    let root_page = btree_create_table(pager)?;

    insert_schema_entry(
        pager,
        "table",
        &plan.table_name,
        &plan.table_name,
        root_page,
        &plan.sql,
    )?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;

    if let Some(table_def) = catalog.get_table(&plan.table_name) {
        if table_def.has_autoincrement {
            super::autoincrement::ensure_sqlite_sequence(pager, catalog)?;
            super::autoincrement::update_autoincrement_seq(pager, catalog, &plan.table_name, 0)?;
            if !pager.in_transaction() {
                pager.flush()?;
            }
        }
    }

    Ok(ExecResult::affected(0))
}

pub(super) fn execute_create_table_as_select(
    table_name: &str,
    if_not_exists: bool,
    query: &Plan,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if if_not_exists && catalog.get_table(table_name).is_some() {
        return Ok(ExecResult::affected(0));
    }

    if catalog.get_table(table_name).is_some() {
        return Err(Error::Other(format!("table {table_name} already exists")));
    }

    let query_result = super::execute(query, pager, catalog)?;

    let col_defs: Vec<String> = query_result
        .columns
        .iter()
        .map(|name| {
            let clean = name.rsplit('.').next().unwrap_or(name);
            format!("{clean} TEXT")
        })
        .collect();
    let create_sql = format!("CREATE TABLE {table_name} ({})", col_defs.join(", "));

    let root_page = btree_create_table(pager)?;

    insert_schema_entry(
        pager,
        "table",
        table_name,
        table_name,
        root_page,
        &create_sql,
    )?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;

    let mut current_root = root_page;
    let mut rows_affected = 0u64;
    for (i, row) in query_result.rows.iter().enumerate() {
        let rowid = (i + 1) as i64;
        let record = Record {
            values: row.values.clone(),
        };
        current_root = btree_insert(pager, current_root, rowid, &record)?;
        rows_affected += 1;
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    Ok(ExecResult::affected(rows_affected))
}

pub(super) fn execute_create_index(
    plan: &CreateIndexPlan,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if plan.if_not_exists
        && catalog
            .indexes
            .contains_key(&plan.index_name.to_lowercase())
    {
        return Ok(ExecResult::affected(0));
    }

    if catalog
        .indexes
        .contains_key(&plan.index_name.to_lowercase())
    {
        return Err(Error::Other(format!(
            "index {} already exists",
            plan.index_name
        )));
    }

    let table_def = catalog
        .get_table(&plan.table_name)
        .ok_or_else(|| Error::Other(format!("table not found: {}", plan.table_name)))?;
    let table_root = table_def.root_page;

    // Resolve each indexed expression. Plain column names are looked up in
    // the table; arbitrary expressions become None here and are emitted as
    // NULL keys (acceptable since query-side use of expression indexes is
    // not yet implemented — we just want the index to build without error).
    let col_indices: Vec<Option<usize>> = plan
        .columns
        .iter()
        .map(|col_name| {
            table_def
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
        })
        .collect();

    let has_rowid_alias = table_def.columns.iter().any(|c| c.is_rowid_alias);

    let root_page = btree_create_index(pager)?;

    let mut cursor = BTreeCursor::new(pager, table_root);
    let rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

    // Build planning-side ColumnRefs once for the predicate evaluator.
    let plan_columns: Vec<crate::planner::ColumnRef> = table_def
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
    let col_names: Vec<String> = table_def.columns.iter().map(|c| c.name.clone()).collect();
    let predicate_plan = predicate_from_sql(plan.predicate.as_deref(), &plan_columns, catalog);

    let mut current_root = root_page;
    for row in &rows {
        // Build the row in user-facing column order (rowid alias gets the
        // rowid, otherwise pull from the record).
        let row_values: Vec<Value> = table_def
            .columns
            .iter()
            .map(|c| {
                if c.is_rowid_alias {
                    Value::Integer(row.rowid)
                } else {
                    row.record
                        .values
                        .get(c.column_index)
                        .cloned()
                        .unwrap_or(Value::Null)
                }
            })
            .collect();
        let row_obj = crate::types::Row { values: row_values.clone(), rowid: None };

        // Partial index: only include rows where the predicate is truthy.
        if let Some(pred) = &predicate_plan {
            let v = super::eval::eval_expr(pred, &row_obj, &col_names, pager, catalog)?;
            if !crate::eval_helpers::is_truthy(&v) {
                continue;
            }
        }

        let mut key_values: Vec<Value> = Vec::new();
        for &col_idx_opt in &col_indices {
            match col_idx_opt {
                Some(col_idx) => {
                    let table_col = &table_def.columns[col_idx];
                    if table_col.is_rowid_alias {
                        key_values.push(Value::Integer(row.rowid));
                    } else {
                        key_values.push(
                            row.record
                                .values
                                .get(col_idx)
                                .cloned()
                                .unwrap_or(Value::Null),
                        );
                    }
                }
                None => key_values.push(Value::Null),
            }
        }
        key_values.push(Value::Integer(row.rowid));

        let key_record = Record { values: key_values };
        current_root = btree_index_insert(pager, current_root, &key_record)
            .map_err(|e| Error::Other(e.to_string()))?;
    }

    let _ = has_rowid_alias;

    insert_schema_entry(
        pager,
        "index",
        &plan.index_name,
        &plan.table_name,
        current_root,
        &plan.sql,
    )
    .map_err(|e| Error::Other(e.to_string()))?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;

    Ok(ExecResult::affected(0))
}

/// Parse a partial-index WHERE clause back into a PlanExpr. Returns None
/// for missing or unparseable predicates (the index then behaves as a
/// non-partial index — safer than silently filtering nothing).
pub(super) fn predicate_from_sql(
    sql: Option<&str>,
    columns: &[crate::planner::ColumnRef],
    catalog: &Catalog,
) -> Option<crate::planner::PlanExpr> {
    let s = sql?;
    let parsed = rsqlite_parser::parse::parse_sql(&format!("SELECT 1 WHERE {s}")).ok()?;
    let stmt = parsed.into_iter().next()?;
    if let sqlparser::ast::Statement::Query(q) = stmt {
        if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
            if let Some(expr) = sel.selection {
                return crate::planner::plan_expr(&expr, columns, catalog).ok();
            }
        }
    }
    None
}

pub(super) fn execute_alter_rename(
    old_name: &str,
    new_name: &str,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if catalog.get_table(old_name).is_none() {
        return Err(Error::Other(format!("no such table: {old_name}")));
    }
    if catalog.get_table(new_name).is_some() {
        return Err(Error::Other(format!(
            "there is already a table named {new_name}"
        )));
    }

    let mut cursor = BTreeCursor::new(pager, 1);
    let mut entries_to_update = Vec::new();
    let mut has_row = cursor.first().map_err(|e| Error::Other(e.to_string()))?;
    while has_row {
        let current = cursor.current().map_err(|e| Error::Other(e.to_string()))?;
        let tbl_name_match = current.record.values.get(2).is_some_and(|v| {
            if let Value::Text(s) = v {
                s.eq_ignore_ascii_case(old_name)
            } else {
                false
            }
        });
        if tbl_name_match {
            entries_to_update.push((current.rowid, current.record));
        }
        has_row = cursor.next().map_err(|e| Error::Other(e.to_string()))?;
    }

    for (rowid, record) in &entries_to_update {
        let mut new_values = record.values.clone();
        if let Value::Text(ref name) = new_values[1] {
            if name.eq_ignore_ascii_case(old_name) {
                new_values[1] = Value::Text(new_name.to_string());
            }
        }
        new_values[2] = Value::Text(new_name.to_string());
        if let Value::Text(ref sql) = new_values[4] {
            let new_sql = replace_table_name_in_sql(sql, old_name, new_name);
            new_values[4] = Value::Text(new_sql);
        }
        let new_record = Record { values: new_values };
        btree_delete(pager, 1, *rowid).map_err(|e| Error::Other(e.to_string()))?;
        let new_root =
            btree_insert(pager, 1, *rowid, &new_record).map_err(|e| Error::Other(e.to_string()))?;
        if new_root != 1 {
            return Err(Error::Other(
                "sqlite_schema root page split — not yet supported".to_string(),
            ));
        }
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult::affected(0))
}

fn replace_table_name_in_sql(sql: &str, old_name: &str, new_name: &str) -> String {
    let lower_sql = sql.to_lowercase();
    let lower_old = old_name.to_lowercase();
    if let Some(pos) = lower_sql.find(&lower_old) {
        let mut result = String::with_capacity(sql.len());
        result.push_str(&sql[..pos]);
        result.push_str(new_name);
        result.push_str(&sql[pos + old_name.len()..]);
        result
    } else {
        sql.to_string()
    }
}

pub(super) fn execute_alter_add_column(
    table_name: &str,
    column_name: &str,
    column_type: &str,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    let table = catalog
        .get_table(table_name)
        .ok_or_else(|| Error::Other(format!("no such table: {table_name}")))?;

    if table
        .columns
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(column_name))
    {
        return Err(Error::Other(format!(
            "duplicate column name: {column_name}"
        )));
    }

    let mut cursor = BTreeCursor::new(pager, 1);
    let mut target_rowid = None;
    let mut original_record = None;
    let mut has_row = cursor.first().map_err(|e| Error::Other(e.to_string()))?;
    while has_row {
        let current = cursor.current().map_err(|e| Error::Other(e.to_string()))?;
        let is_match = current.record.values.get(1).is_some_and(|v| {
            if let Value::Text(s) = v {
                s.eq_ignore_ascii_case(table_name)
            } else {
                false
            }
        }) && current.record.values.first().is_some_and(|v| {
            if let Value::Text(s) = v {
                s == "table"
            } else {
                false
            }
        });
        if is_match {
            target_rowid = Some(current.rowid);
            original_record = Some(current.record);
            break;
        }
        has_row = cursor.next().map_err(|e| Error::Other(e.to_string()))?;
    }

    let rowid = target_rowid
        .ok_or_else(|| Error::Other(format!("schema entry not found for table: {table_name}")))?;
    let record = original_record.unwrap();

    let old_sql = match &record.values[4] {
        Value::Text(s) => s.clone(),
        _ => return Err(Error::Other("invalid schema SQL".to_string())),
    };

    let col_def = if column_type.is_empty() {
        column_name.to_string()
    } else {
        format!("{column_name} {column_type}")
    };
    let new_sql = if let Some(pos) = old_sql.rfind(')') {
        format!("{}, {col_def})", &old_sql[..pos])
    } else {
        return Err(Error::Other("malformed CREATE TABLE SQL".to_string()));
    };

    let mut new_values = record.values.clone();
    new_values[4] = Value::Text(new_sql);
    let new_record = Record { values: new_values };

    btree_delete(pager, 1, rowid).map_err(|e| Error::Other(e.to_string()))?;
    let new_root =
        btree_insert(pager, 1, rowid, &new_record).map_err(|e| Error::Other(e.to_string()))?;
    if new_root != 1 {
        return Err(Error::Other(
            "sqlite_schema root page split — not yet supported".to_string(),
        ));
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult::affected(0))
}

pub(super) fn execute_drop_table(
    table_name: &str,
    if_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if catalog.get_table(table_name).is_none() {
        if if_exists {
            return Ok(ExecResult::affected(0));
        }
        return Err(Error::Other(format!("no such table: {table_name}")));
    }

    delete_schema_entries(pager, table_name).map_err(|e| Error::Other(e.to_string()))?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult::affected(0))
}

pub(super) fn execute_drop_index(
    index_name: &str,
    if_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if !catalog.indexes.contains_key(&index_name.to_lowercase()) {
        if if_exists {
            return Ok(ExecResult::affected(0));
        }
        return Err(Error::Other(format!("no such index: {index_name}")));
    }

    delete_schema_entries(pager, index_name).map_err(|e| Error::Other(e.to_string()))?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult::affected(0))
}

pub(super) fn execute_create_view(
    name: &str,
    sql: &str,
    if_not_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if catalog.get_view(name).is_some() {
        if if_not_exists {
            return Ok(ExecResult::affected(0));
        }
        return Err(Error::Other(format!("view {name} already exists")));
    }

    insert_schema_entry(pager, "view", name, name, 0, sql)
        .map_err(|e| Error::Other(e.to_string()))?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult::affected(0))
}

pub(super) fn execute_drop_view(
    name: &str,
    if_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if catalog.get_view(name).is_none() {
        if if_exists {
            return Ok(ExecResult::affected(0));
        }
        return Err(Error::Other(format!("no such view: {name}")));
    }

    delete_schema_entries(pager, name).map_err(|e| Error::Other(e.to_string()))?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult::affected(0))
}

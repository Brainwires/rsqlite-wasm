use std::cell::RefCell;

use rsqlite_storage::btree::{
    btree_create_index, btree_create_table, btree_delete, btree_index_delete, btree_index_insert,
    btree_insert, btree_max_rowid, btree_row_exists, delete_schema_entries, insert_schema_entry,
    BTreeCursor, CursorRow, IndexCursor,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::{
    compare, eval_binop, eval_cast, eval_scalar_function, eval_unaryop, is_truthy, like_match,
    literal_to_value, value_to_text,
};
use crate::planner::{
    agg_column_name, AggFunc, ColumnRef, CreateIndexPlan, CreateTablePlan, DeletePlan, InsertPlan,
    JoinType, OnConflictPlan, Plan, PlanExpr, ProjectionItem, SortKey, UnaryOp, UpdatePlan,
};
use crate::types::{QueryResult, Row};

thread_local! {
    static BOUND_PARAMS: RefCell<Vec<Value>> = RefCell::new(Vec::new());
    static LAST_INSERT_ROWID: RefCell<i64> = RefCell::new(0);
    static LAST_CHANGES: RefCell<i64> = RefCell::new(0);
    static TOTAL_CHANGES_COUNT: RefCell<i64> = RefCell::new(0);
}

pub fn set_params(params: Vec<Value>) {
    BOUND_PARAMS.with(|p| *p.borrow_mut() = params);
}

pub fn clear_params() {
    BOUND_PARAMS.with(|p| p.borrow_mut().clear());
}

fn get_param(index: usize) -> Value {
    BOUND_PARAMS.with(|p| {
        p.borrow()
            .get(index)
            .cloned()
            .unwrap_or(Value::Null)
    })
}

fn set_last_insert_rowid(rowid: i64) {
    LAST_INSERT_ROWID.with(|r| *r.borrow_mut() = rowid);
}

fn get_last_insert_rowid() -> i64 {
    LAST_INSERT_ROWID.with(|r| *r.borrow())
}

fn set_changes(count: i64) {
    LAST_CHANGES.with(|c| *c.borrow_mut() = count);
    TOTAL_CHANGES_COUNT.with(|t| *t.borrow_mut() += count);
}

fn get_changes() -> i64 {
    LAST_CHANGES.with(|c| *c.borrow())
}

fn get_total_changes() -> i64 {
    TOTAL_CHANGES_COUNT.with(|t| *t.borrow())
}

pub fn get_last_insert_rowid_pub() -> i64 {
    get_last_insert_rowid()
}

pub fn get_changes_pub() -> i64 {
    get_changes()
}

pub fn get_total_changes_pub() -> i64 {
    get_total_changes()
}

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
        | Plan::RollbackTo(_) => Err(Error::Other(
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

pub fn execute_pragma(
    name: &str,
    argument: Option<&str>,
    pager: &Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    match name {
        "table_info" => {
            let table_name = argument.ok_or_else(|| {
                Error::Other("PRAGMA table_info requires a table name".to_string())
            })?;
            let table = catalog.get_table(table_name).ok_or_else(|| {
                Error::Other(format!("no such table: {table_name}"))
            })?;
            let columns = vec![
                "cid".to_string(),
                "name".to_string(),
                "type".to_string(),
                "notnull".to_string(),
                "dflt_value".to_string(),
                "pk".to_string(),
            ];
            let rows = table
                .columns
                .iter()
                .map(|col| Row {
                    values: vec![
                        Value::Integer(col.column_index as i64),
                        Value::Text(col.name.clone()),
                        Value::Text(col.type_name.clone()),
                        Value::Integer(if col.nullable { 0 } else { 1 }),
                        Value::Null,
                        Value::Integer(if col.is_primary_key { 1 } else { 0 }),
                    ],
                })
                .collect();
            Ok(QueryResult { columns, rows })
        }
        "table_list" => {
            let columns = vec![
                "schema".to_string(),
                "name".to_string(),
                "type".to_string(),
            ];
            let mut rows: Vec<Row> = catalog
                .tables
                .values()
                .map(|t| Row {
                    values: vec![
                        Value::Text("main".to_string()),
                        Value::Text(t.name.clone()),
                        Value::Text("table".to_string()),
                    ],
                })
                .collect();
            rows.sort_by(|a, b| a.values[1].to_string().cmp(&b.values[1].to_string()));
            Ok(QueryResult { columns, rows })
        }
        "index_list" => {
            let table_name = argument.ok_or_else(|| {
                Error::Other("PRAGMA index_list requires a table name".to_string())
            })?;
            let columns = vec![
                "seq".to_string(),
                "name".to_string(),
                "unique".to_string(),
                "origin".to_string(),
                "partial".to_string(),
            ];
            let mut rows = Vec::new();
            let mut seq = 0i64;
            for idx in catalog.indexes.values() {
                if idx.table_name.eq_ignore_ascii_case(table_name) {
                    rows.push(Row {
                        values: vec![
                            Value::Integer(seq),
                            Value::Text(idx.name.clone()),
                            Value::Integer(0),
                            Value::Text("c".to_string()),
                            Value::Integer(0),
                        ],
                    });
                    seq += 1;
                }
            }
            Ok(QueryResult { columns, rows })
        }
        "index_info" => {
            let index_name = argument.ok_or_else(|| {
                Error::Other("PRAGMA index_info requires an index name".to_string())
            })?;
            let idx = catalog
                .indexes
                .get(&index_name.to_lowercase())
                .ok_or_else(|| Error::Other(format!("no such index: {index_name}")))?;
            let table = catalog.get_table(&idx.table_name);
            let columns = vec![
                "seqno".to_string(),
                "cid".to_string(),
                "name".to_string(),
            ];
            let rows = idx
                .columns
                .iter()
                .enumerate()
                .map(|(i, col_name)| {
                    let cid = table
                        .and_then(|t| {
                            t.columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                        })
                        .map(|p| p as i64)
                        .unwrap_or(-1);
                    Row {
                        values: vec![
                            Value::Integer(i as i64),
                            Value::Integer(cid),
                            Value::Text(col_name.clone()),
                        ],
                    }
                })
                .collect();
            Ok(QueryResult { columns, rows })
        }
        "page_size" => Ok(QueryResult {
            columns: vec!["page_size".to_string()],
            rows: vec![Row {
                values: vec![Value::Integer(pager.page_size() as i64)],
            }],
        }),
        "page_count" => Ok(QueryResult {
            columns: vec!["page_count".to_string()],
            rows: vec![Row {
                values: vec![Value::Integer(pager.page_count() as i64)],
            }],
        }),
        "database_list" => Ok(QueryResult {
            columns: vec![
                "seq".to_string(),
                "name".to_string(),
                "file".to_string(),
            ],
            rows: vec![Row {
                values: vec![
                    Value::Integer(0),
                    Value::Text("main".to_string()),
                    Value::Text(String::new()),
                ],
            }],
        }),
        _ => Err(Error::Other(format!("unsupported PRAGMA: {name}"))),
    }
}

fn execute_create_table(
    plan: &CreateTablePlan,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if plan.if_not_exists && catalog.get_table(&plan.table_name).is_some() {
        return Ok(ExecResult { rows_affected: 0 });
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
            ensure_sqlite_sequence(pager, catalog)?;
            update_autoincrement_seq(pager, catalog, &plan.table_name, 0)?;
            if !pager.in_transaction() {
                pager.flush()?;
            }
        }
    }

    Ok(ExecResult { rows_affected: 0 })
}

fn execute_create_table_as_select(
    table_name: &str,
    if_not_exists: bool,
    query: &Plan,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if if_not_exists && catalog.get_table(table_name).is_some() {
        return Ok(ExecResult { rows_affected: 0 });
    }

    if catalog.get_table(table_name).is_some() {
        return Err(Error::Other(format!(
            "table {table_name} already exists"
        )));
    }

    let query_result = execute(query, pager, catalog)?;

    let col_defs: Vec<String> = query_result
        .columns
        .iter()
        .map(|name| {
            let clean = name.rsplit('.').next().unwrap_or(name);
            format!("{clean} TEXT")
        })
        .collect();
    let create_sql = format!(
        "CREATE TABLE {table_name} ({})",
        col_defs.join(", ")
    );

    let root_page = btree_create_table(pager)?;

    insert_schema_entry(pager, "table", table_name, table_name, root_page, &create_sql)?;

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

    Ok(ExecResult { rows_affected })
}

fn execute_create_index(
    plan: &CreateIndexPlan,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if plan.if_not_exists && catalog.indexes.contains_key(&plan.index_name.to_lowercase()) {
        return Ok(ExecResult { rows_affected: 0 });
    }

    if catalog.indexes.contains_key(&plan.index_name.to_lowercase()) {
        return Err(Error::Other(format!(
            "index {} already exists",
            plan.index_name
        )));
    }

    let table_def = catalog.get_table(&plan.table_name).ok_or_else(|| {
        Error::Other(format!("table not found: {}", plan.table_name))
    })?;
    let table_root = table_def.root_page;

    let col_indices: Vec<usize> = plan
        .columns
        .iter()
        .map(|col_name| {
            table_def
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| {
                    Error::Other(format!(
                        "column {} not found in table {}",
                        col_name, plan.table_name
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let has_rowid_alias = table_def
        .columns
        .iter()
        .any(|c| c.is_rowid_alias);

    let root_page = btree_create_index(pager)?;

    let mut cursor = BTreeCursor::new(pager, table_root);
    let rows = cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;

    let mut current_root = root_page;
    for row in &rows {
        let mut key_values: Vec<Value> = Vec::new();
        for &col_idx in &col_indices {
            let table_col = &table_def.columns[col_idx];
            if table_col.is_rowid_alias {
                key_values.push(Value::Integer(row.rowid));
            } else {
                let val = row.record.values
                    .get(col_idx)
                    .cloned()
                    .unwrap_or(Value::Null);
                key_values.push(val);
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

    Ok(ExecResult { rows_affected: 0 })
}

fn execute_alter_rename(
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
        let new_root = btree_insert(pager, 1, *rowid, &new_record)
            .map_err(|e| Error::Other(e.to_string()))?;
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
    Ok(ExecResult { rows_affected: 0 })
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

fn execute_alter_add_column(
    table_name: &str,
    column_name: &str,
    column_type: &str,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    let table = catalog.get_table(table_name).ok_or_else(|| {
        Error::Other(format!("no such table: {table_name}"))
    })?;

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

    let rowid = target_rowid.ok_or_else(|| {
        Error::Other(format!("schema entry not found for table: {table_name}"))
    })?;
    let record = original_record.unwrap();

    let old_sql = match &record.values[4] {
        Value::Text(s) => s.clone(),
        _ => {
            return Err(Error::Other(
                "invalid schema SQL".to_string(),
            ))
        }
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
    let new_root = btree_insert(pager, 1, rowid, &new_record)
        .map_err(|e| Error::Other(e.to_string()))?;
    if new_root != 1 {
        return Err(Error::Other(
            "sqlite_schema root page split — not yet supported".to_string(),
        ));
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult { rows_affected: 0 })
}

fn execute_drop_table(
    table_name: &str,
    if_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if catalog.get_table(table_name).is_none() {
        if if_exists {
            return Ok(ExecResult { rows_affected: 0 });
        }
        return Err(Error::Other(format!("no such table: {table_name}")));
    }

    delete_schema_entries(pager, table_name).map_err(|e| Error::Other(e.to_string()))?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult { rows_affected: 0 })
}

fn execute_drop_index(
    index_name: &str,
    if_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if !catalog.indexes.contains_key(&index_name.to_lowercase()) {
        if if_exists {
            return Ok(ExecResult { rows_affected: 0 });
        }
        return Err(Error::Other(format!("no such index: {index_name}")));
    }

    delete_schema_entries(pager, index_name).map_err(|e| Error::Other(e.to_string()))?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult { rows_affected: 0 })
}

fn execute_create_view(
    name: &str,
    sql: &str,
    if_not_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if catalog.get_view(name).is_some() {
        if if_not_exists {
            return Ok(ExecResult { rows_affected: 0 });
        }
        return Err(Error::Other(format!("view {name} already exists")));
    }

    insert_schema_entry(pager, "view", name, name, 0, sql)
        .map_err(|e| Error::Other(e.to_string()))?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult { rows_affected: 0 })
}

fn execute_drop_view(
    name: &str,
    if_exists: bool,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<ExecResult> {
    if catalog.get_view(name).is_none() {
        if if_exists {
            return Ok(ExecResult { rows_affected: 0 });
        }
        return Err(Error::Other(format!("no such view: {name}")));
    }

    delete_schema_entries(pager, name).map_err(|e| Error::Other(e.to_string()))?;

    if !pager.in_transaction() {
        pager.flush()?;
    }

    catalog.reload(pager)?;
    Ok(ExecResult { rows_affected: 0 })
}

fn execute_insert(plan: &InsertPlan, pager: &mut Pager, catalog: &Catalog) -> Result<ExecResult> {
    let table_indexes = get_table_indexes(catalog, &plan.table_name);
    let mut rows_affected = 0u64;
    let mut current_root = plan.root_page;
    let is_autoincrement = catalog
        .get_table(&plan.table_name)
        .is_some_and(|t| t.has_autoincrement);
    let mut max_rowid_inserted = 0i64;

    if let Some(source) = &plan.source_query {
        let query_result = execute(source, pager, catalog)?;
        for row in &query_result.rows {
            let values = map_query_row_to_insert(
                &row.values,
                &plan.table_columns,
                &plan.target_columns,
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
        let values = eval_insert_row(row_exprs, &plan.table_columns, &plan.target_columns)?;

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

fn ensure_sqlite_sequence(pager: &mut Pager, catalog: &mut Catalog) -> Result<()> {
    if catalog.get_table("sqlite_sequence").is_some() {
        return Ok(());
    }
    let root_page = btree_create_table(pager)?;
    insert_schema_entry(
        pager,
        "table",
        "sqlite_sequence",
        "sqlite_sequence",
        root_page,
        "CREATE TABLE sqlite_sequence(name,seq)",
    )?;
    catalog.reload(pager)?;
    Ok(())
}

fn read_autoincrement_seq(pager: &mut Pager, catalog: &Catalog, table_name: &str) -> Result<i64> {
    let seq_table = match catalog.get_table("sqlite_sequence") {
        Some(t) => t,
        None => return Ok(0),
    };
    let mut cursor = BTreeCursor::new(pager, seq_table.root_page);
    let rows = cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;
    for row in &rows {
        if let Some(Value::Text(name)) = row.record.values.first() {
            if name.eq_ignore_ascii_case(table_name) {
                if let Some(Value::Integer(seq)) = row.record.values.get(1) {
                    return Ok(*seq);
                }
            }
        }
    }
    Ok(0)
}

fn update_autoincrement_seq(
    pager: &mut Pager,
    catalog: &Catalog,
    table_name: &str,
    new_seq: i64,
) -> Result<()> {
    let seq_table = match catalog.get_table("sqlite_sequence") {
        Some(t) => t,
        None => return Ok(()),
    };
    let mut cursor = BTreeCursor::new(pager, seq_table.root_page);
    let rows = cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;
    let mut existing_rowid = None;
    for row in &rows {
        if let Some(Value::Text(name)) = row.record.values.first() {
            if name.eq_ignore_ascii_case(table_name) {
                existing_rowid = Some(row.rowid);
                break;
            }
        }
    }

    let record = Record {
        values: vec![Value::Text(table_name.to_string()), Value::Integer(new_seq)],
    };
    let root = seq_table.root_page;
    match existing_rowid {
        Some(rowid) => {
            btree_delete(pager, root, rowid).map_err(|e| Error::Other(e.to_string()))?;
            btree_insert(pager, root, rowid, &record)?;
        }
        None => {
            let rowid = btree_max_rowid(pager, root)? + 1;
            btree_insert(pager, root, rowid, &record)?;
        }
    }
    Ok(())
}

fn compute_autoincrement_rowid(pager: &mut Pager, catalog: &Catalog, table_name: &str, current_root: u32) -> Result<i64> {
    let seq = read_autoincrement_seq(pager, catalog, table_name)?;
    let max_rowid = btree_max_rowid(pager, current_root)?;
    Ok(std::cmp::max(seq, max_rowid) + 1)
}

fn check_check_constraints(
    values: &[Value],
    columns: &[ColumnRef],
    table_name: &str,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    let table_def = match catalog.get_table(table_name) {
        Some(td) => td,
        None => return Ok(()),
    };
    if table_def.check_constraints.is_empty() {
        return Ok(());
    }

    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let row = Row { values: values.to_vec() };

    for check_sql in &table_def.check_constraints {
        let parsed = rsqlite_parser::parse::parse_sql(&format!("SELECT {check_sql}"));
        let expr = match parsed {
            Ok(stmts) => {
                if let Some(sqlparser::ast::Statement::Query(q)) = stmts.into_iter().next() {
                    if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
                        if let Some(item) = sel.projection.into_iter().next() {
                            if let sqlparser::ast::SelectItem::UnnamedExpr(e) = item {
                                Some(e)
                            } else { None }
                        } else { None }
                    } else { None }
                } else { None }
            }
            Err(_) => None,
        };
        let expr = match expr {
            Some(e) => e,
            None => continue,
        };
        let plan_expr = match crate::planner::plan_expr(&expr, columns, catalog) {
            Ok(pe) => pe,
            Err(_) => continue,
        };
        let val = eval_expr(&plan_expr, &row, &col_names, pager, catalog)?;
        if !is_truthy(&val) && !matches!(val, Value::Null) {
            return Err(Error::Other(format!(
                "CHECK constraint failed: {table_name}"
            )));
        }
    }
    Ok(())
}

fn check_not_null_constraints(values: &[Value], columns: &[ColumnRef], table_name: &str) -> Result<()> {
    for (i, col) in columns.iter().enumerate() {
        if !col.nullable && !col.is_rowid_alias {
            if let Some(Value::Null) = values.get(i) {
                return Err(Error::Other(format!(
                    "NOT NULL constraint failed: {}.{}", table_name, col.name
                )));
            }
        }
    }
    Ok(())
}

fn check_unique_constraints(
    values: &[Value],
    columns: &[ColumnRef],
    table_name: &str,
    pager: &mut Pager,
    root_page: u32,
    exclude_rowid: Option<i64>,
) -> Result<()> {
    let unique_cols: Vec<(usize, &str)> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_unique && !c.is_rowid_alias)
        .map(|(i, c)| (i, c.name.as_str()))
        .collect();

    if unique_cols.is_empty() {
        return Ok(());
    }

    let mut cursor = BTreeCursor::new(pager, root_page);
    let rows = cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;

    for (col_idx, col_name) in &unique_cols {
        let new_val = &values[*col_idx];
        if matches!(new_val, Value::Null) {
            continue;
        }
        for row in &rows {
            if let Some(exclude) = exclude_rowid {
                if row.rowid == exclude {
                    continue;
                }
            }
            if let Some(existing) = row.record.values.get(*col_idx) {
                if compare(existing, new_val) == 0 {
                    return Err(Error::Other(format!(
                        "UNIQUE constraint failed: {}.{}", table_name, col_name
                    )));
                }
            }
        }
    }
    Ok(())
}

fn map_query_row_to_insert(
    query_values: &[Value],
    table_columns: &[ColumnRef],
    target_columns: &Option<Vec<String>>,
) -> Result<Vec<Value>> {
    let num_table_cols = table_columns.len();
    let mut values = vec![Value::Null; num_table_cols];

    if let Some(targets) = target_columns {
        for (i, col_name) in targets.iter().enumerate() {
            let idx = table_columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| Error::Other(format!("unknown column: {col_name}")))?;
            values[idx] = query_values.get(i).cloned().unwrap_or(Value::Null);
        }
    } else {
        for (i, val) in query_values.iter().enumerate() {
            if i < num_table_cols {
                values[i] = val.clone();
            }
        }
    }

    Ok(values)
}

fn read_row_by_rowid(
    pager: &mut Pager,
    root_page: u32,
    target_rowid: i64,
    table_columns: &[ColumnRef],
) -> Result<Vec<Value>> {
    let mut cursor = BTreeCursor::new(pager, root_page);
    let mut has_row = cursor.first().map_err(|e| Error::Other(e.to_string()))?;
    while has_row {
        let row = cursor.current().map_err(|e| Error::Other(e.to_string()))?;
        if row.rowid == target_rowid {
            let mut values = Vec::with_capacity(table_columns.len());
            for col in table_columns {
                if col.is_rowid_alias {
                    values.push(Value::Integer(row.rowid));
                } else {
                    values.push(
                        row.record
                            .values
                            .get(col.column_index)
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                }
            }
            return Ok(values);
        }
        if row.rowid > target_rowid {
            break;
        }
        has_row = cursor.next().map_err(|e| Error::Other(e.to_string()))?;
    }
    Err(Error::Other(format!("row not found: rowid={target_rowid}")))
}

fn row_values_for_rowid(
    btree_rows: &[CursorRow],
    rowid: i64,
    table_columns: &[ColumnRef],
) -> Vec<Value> {
    for row in btree_rows {
        if row.rowid == rowid {
            let mut values = Vec::with_capacity(table_columns.len());
            for col in table_columns {
                if col.is_rowid_alias {
                    values.push(Value::Integer(row.rowid));
                } else {
                    values.push(
                        row.record
                            .values
                            .get(col.column_index)
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                }
            }
            return values;
        }
    }
    vec![Value::Null; table_columns.len()]
}

fn get_table_indexes(catalog: &Catalog, table_name: &str) -> Vec<(u32, Vec<usize>)> {
    let table_def = match catalog.get_table(table_name) {
        Some(t) => t,
        None => return vec![],
    };

    catalog
        .indexes
        .values()
        .filter(|idx| idx.table_name.eq_ignore_ascii_case(table_name) && !idx.columns.is_empty())
        .filter_map(|idx| {
            let col_indices: Vec<usize> = idx
                .columns
                .iter()
                .filter_map(|col_name| {
                    table_def
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(col_name))
                })
                .collect();
            if col_indices.len() == idx.columns.len() {
                Some((idx.root_page, col_indices))
            } else {
                None
            }
        })
        .collect()
}

fn build_index_key(
    values: &[Value],
    col_indices: &[usize],
    table_columns: &[ColumnRef],
    rowid: i64,
) -> Record {
    let mut key_values: Vec<Value> = Vec::new();
    for &col_idx in col_indices {
        if table_columns[col_idx].is_rowid_alias {
            key_values.push(Value::Integer(rowid));
        } else {
            key_values.push(values.get(col_idx).cloned().unwrap_or(Value::Null));
        }
    }
    key_values.push(Value::Integer(rowid));
    Record { values: key_values }
}

fn eval_insert_row(
    row_exprs: &[PlanExpr],
    table_columns: &[ColumnRef],
    target_columns: &Option<Vec<String>>,
) -> Result<Vec<Value>> {
    match target_columns {
        None => {
            let mut values = Vec::with_capacity(table_columns.len());
            for (i, col) in table_columns.iter().enumerate() {
                if i < row_exprs.len() {
                    values.push(eval_literal(&row_exprs[i])?);
                } else if col.is_rowid_alias {
                    values.push(Value::Null);
                } else {
                    values.push(Value::Null);
                }
            }
            Ok(values)
        }
        Some(target_cols) => {
            let mut values = vec![Value::Null; table_columns.len()];
            for (i, target_name) in target_cols.iter().enumerate() {
                let col_idx = table_columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(target_name))
                    .ok_or_else(|| {
                        Error::Other(format!("unknown column: {target_name}"))
                    })?;
                if i < row_exprs.len() {
                    values[col_idx] = eval_literal(&row_exprs[i])?;
                }
            }
            Ok(values)
        }
    }
}

fn eval_literal(expr: &PlanExpr) -> Result<Value> {
    match expr {
        PlanExpr::Literal(lit) => Ok(literal_to_value(lit)),
        PlanExpr::Param(index) => Ok(get_param(*index)),
        PlanExpr::UnaryOp {
            op: UnaryOp::Neg,
            operand,
        } => {
            let v = eval_literal(operand)?;
            match v {
                Value::Integer(n) => Ok(Value::Integer(-n)),
                Value::Real(f) => Ok(Value::Real(-f)),
                _ => Ok(Value::Integer(0)),
            }
        }
        _ => Err(Error::Other(
            "only literal values are supported in INSERT".to_string(),
        )),
    }
}

fn compare_rows_by_keys(
    a: &Row,
    b: &Row,
    keys: &[SortKey],
    columns: &[String],
    pager: &mut Pager,
    catalog: &Catalog,
) -> std::cmp::Ordering {
    for key in keys {
        let va = eval_expr(&key.expr, a, columns, pager, catalog).unwrap_or(Value::Null);
        let vb = eval_expr(&key.expr, b, columns, pager, catalog).unwrap_or(Value::Null);

        let nocase = has_nocase_collation(&key.expr);
        let cmp_val = if nocase {
            compare(&fold_nocase(&va), &fold_nocase(&vb))
        } else {
            compare(&va, &vb)
        };
        let ordering = if cmp_val < 0 {
            std::cmp::Ordering::Less
        } else if cmp_val > 0 {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        };

        let ordering = if key.descending {
            ordering.reverse()
        } else {
            ordering
        };

        if ordering != std::cmp::Ordering::Equal {
            // Handle NULLS FIRST / NULLS LAST
            let a_null = matches!(va, Value::Null);
            let b_null = matches!(vb, Value::Null);
            if a_null || b_null {
                let nulls_first = key.nulls_first.unwrap_or(!key.descending);
                if a_null && !b_null {
                    return if nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    };
                }
                if !a_null && b_null {
                    return if nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    };
                }
            }

            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn row_hash_key(row: &Row) -> Vec<u8> {
    let mut key = Vec::new();
    for val in &row.values {
        match val {
            Value::Null => key.push(0),
            Value::Integer(n) => {
                key.push(1);
                key.extend_from_slice(&n.to_le_bytes());
            }
            Value::Real(f) => {
                key.push(2);
                key.extend_from_slice(&f.to_le_bytes());
            }
            Value::Text(s) => {
                key.push(3);
                key.extend_from_slice(s.as_bytes());
                key.push(0);
            }
            Value::Blob(b) => {
                key.push(4);
                key.extend_from_slice(b);
                key.push(0);
            }
        }
    }
    key
}

fn execute_update(plan: &UpdatePlan, pager: &mut Pager, catalog: &Catalog) -> Result<ExecResult> {
    let column_names: Vec<String> = plan.table_columns.iter().map(|c| c.name.clone()).collect();

    let mut cursor = BTreeCursor::new(pager, plan.root_page);
    let btree_rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

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
            let mut new_values = row.values.clone();
            for (col_name, expr) in &plan.assignments {
                let col_idx = column_names
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| {
                        Error::Other(format!("unknown column: {col_name}"))
                    })?;
                new_values[col_idx] = eval_expr(expr, &row, &column_names, pager, catalog)?;
            }
            check_not_null_constraints(&new_values, &plan.table_columns, &plan.table_name)?;
            check_unique_constraints(&new_values, &plan.table_columns, &plan.table_name, pager, plan.root_page, Some(btree_row.rowid))?;
            check_check_constraints(&new_values, &plan.table_columns, &plan.table_name, pager, catalog)?;
            to_update.push((btree_row.rowid, new_values));
        }
    }

    let rows_affected = to_update.len() as u64;
    let table_indexes = get_table_indexes(catalog, &plan.table_name);

    let mut current_root = plan.root_page;
    for (rowid, new_values) in to_update {
        // Remove old index entries and add new ones
        for (idx_root, idx_col_indices) in &table_indexes {
            let old_key = build_index_key(
                &row_values_for_rowid(&btree_rows, rowid, &plan.table_columns),
                idx_col_indices,
                &plan.table_columns,
                rowid,
            );
            let _ = btree_index_delete(pager, *idx_root, &old_key);

            let new_key =
                build_index_key(&new_values, idx_col_indices, &plan.table_columns, rowid);
            let _ = btree_index_insert(pager, *idx_root, &new_key);
        }

        btree_delete(pager, current_root, rowid)?;
        let record = Record {
            values: new_values,
        };
        current_root = btree_insert(pager, current_root, rowid, &record)?;
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    set_changes(rows_affected as i64);
    Ok(ExecResult { rows_affected })
}

fn execute_delete(plan: &DeletePlan, pager: &mut Pager, catalog: &Catalog) -> Result<ExecResult> {
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

    for rowid in to_delete {
        for (idx_root, idx_col_indices) in &table_indexes {
            let old_values = row_values_for_rowid(&btree_rows, rowid, &plan.table_columns);
            let old_key =
                build_index_key(&old_values, idx_col_indices, &plan.table_columns, rowid);
            let _ = btree_index_delete(pager, *idx_root, &old_key);
        }
        btree_delete(pager, plan.root_page, rowid)?;
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    set_changes(rows_affected as i64);
    Ok(ExecResult { rows_affected })
}

fn execute_scan(
    root_page: u32,
    columns: &[ColumnRef],
    pager: &mut Pager,
) -> Result<QueryResult> {
    let column_names: Vec<String> = columns
        .iter()
        .map(|c| {
            if let Some(t) = &c.table {
                format!("{}.{}", t, c.name)
            } else {
                c.name.clone()
            }
        })
        .collect();

    let mut cursor = BTreeCursor::new(pager, root_page);
    let btree_rows = cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;

    let mut rows = Vec::with_capacity(btree_rows.len());
    for btree_row in &btree_rows {
        let record_values = &btree_row.record.values;
        let mut row_values = Vec::with_capacity(columns.len());

        for col in columns {
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

        rows.push(Row {
            values: row_values,
        });
    }

    Ok(QueryResult {
        columns: column_names,
        rows,
    })
}

fn execute_index_scan(
    table_root_page: u32,
    index_root_page: u32,
    columns: &[ColumnRef],
    index_columns: &[String],
    lookup_values: &[PlanExpr],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let column_names: Vec<String> = columns
        .iter()
        .map(|c| {
            if let Some(t) = &c.table {
                format!("{}.{}", t, c.name)
            } else {
                c.name.clone()
            }
        })
        .collect();

    let eval_values: Vec<Value> = lookup_values
        .iter()
        .map(|expr| eval_expr(expr, &Row { values: vec![] }, &[], pager, catalog))
        .collect::<Result<_>>()?;

    let mut index_cursor = IndexCursor::new(pager, index_root_page);
    let index_entries = index_cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;

    let mut matching_rowids = Vec::new();
    for entry in &index_entries {
        if entry.values.len() < index_columns.len() + 1 {
            continue;
        }
        let mut matches = true;
        for (i, lookup_val) in eval_values.iter().enumerate() {
            let entry_val = &entry.values[i];
            if !values_equal(entry_val, lookup_val) {
                matches = false;
                break;
            }
        }
        if matches {
            if let Some(Value::Integer(rowid)) = entry.values.last() {
                matching_rowids.push(*rowid);
            }
        }
    }

    let mut table_cursor = BTreeCursor::new(pager, table_root_page);
    let all_rows = table_cursor.collect_all().map_err(|e| Error::Other(e.to_string()))?;

    let mut rows = Vec::with_capacity(matching_rowids.len());
    for rowid in &matching_rowids {
        for btree_row in &all_rows {
            if btree_row.rowid == *rowid {
                let record_values = &btree_row.record.values;
                let mut row_values = Vec::with_capacity(columns.len());
                for col in columns {
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
                rows.push(Row { values: row_values });
                break;
            }
        }
    }

    Ok(QueryResult {
        columns: column_names,
        rows,
    })
}

fn execute_index_range_scan(
    table_root_page: u32,
    index_root_page: u32,
    columns: &[ColumnRef],
    _index_column: &str,
    lower_bound: Option<&(PlanExpr, bool)>,
    upper_bound: Option<&(PlanExpr, bool)>,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let column_names: Vec<String> = columns
        .iter()
        .map(|c| {
            if let Some(t) = &c.table {
                format!("{}.{}", t, c.name)
            } else {
                c.name.clone()
            }
        })
        .collect();

    let empty_row = Row { values: vec![] };
    let lower = lower_bound
        .map(|(expr, incl)| {
            eval_expr(expr, &empty_row, &[], pager, catalog).map(|v| (v, *incl))
        })
        .transpose()?;
    let upper = upper_bound
        .map(|(expr, incl)| {
            eval_expr(expr, &empty_row, &[], pager, catalog).map(|v| (v, *incl))
        })
        .transpose()?;

    let mut index_cursor = IndexCursor::new(pager, index_root_page);
    let index_entries = index_cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

    let mut matching_rowids = Vec::new();
    for entry in &index_entries {
        if entry.values.len() < 2 {
            continue;
        }
        let idx_val = &entry.values[0];

        let passes_lower = match &lower {
            Some((bound_val, inclusive)) => {
                let cmp = compare(idx_val, bound_val);
                if *inclusive { cmp >= 0 } else { cmp > 0 }
            }
            None => true,
        };

        let passes_upper = match &upper {
            Some((bound_val, inclusive)) => {
                let cmp = compare(idx_val, bound_val);
                if *inclusive { cmp <= 0 } else { cmp < 0 }
            }
            None => true,
        };

        if passes_lower && passes_upper {
            if let Some(Value::Integer(rowid)) = entry.values.last() {
                matching_rowids.push(*rowid);
            }
        }
    }

    let mut table_cursor = BTreeCursor::new(pager, table_root_page);
    let all_rows = table_cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

    let mut rows = Vec::with_capacity(matching_rowids.len());
    for rowid in &matching_rowids {
        for btree_row in &all_rows {
            if btree_row.rowid == *rowid {
                let record_values = &btree_row.record.values;
                let mut row_values = Vec::with_capacity(columns.len());
                for col in columns {
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
                rows.push(Row { values: row_values });
                break;
            }
        }
    }

    Ok(QueryResult {
        columns: column_names,
        rows,
    })
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => false,
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Real(x), Value::Real(y)) => x == y,
        (Value::Integer(x), Value::Real(y)) => (*x as f64) == *y,
        (Value::Real(x), Value::Integer(y)) => *x == (*y as f64),
        (Value::Text(x), Value::Text(y)) => x == y,
        (Value::Blob(x), Value::Blob(y)) => x == y,
        _ => false,
    }
}

fn execute_project(
    input: &Plan,
    outputs: &[ProjectionItem],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let inner = execute(input, pager, catalog)?;
    let input_columns = &inner.columns;

    let output_names: Vec<String> = outputs.iter().map(|o| o.alias.clone()).collect();

    let mut rows = Vec::with_capacity(inner.rows.len());
    for row in &inner.rows {
        let mut values = Vec::with_capacity(outputs.len());
        for output in outputs {
            let val = eval_expr(&output.expr, row, input_columns, pager, catalog)?;
            values.push(val);
        }
        rows.push(Row { values });
    }

    Ok(QueryResult {
        columns: output_names,
        rows,
    })
}

fn execute_join(
    left: &Plan,
    right: &Plan,
    condition: Option<&PlanExpr>,
    join_type: JoinType,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let left_result = execute(left, pager, catalog)?;
    let right_result = execute(right, pager, catalog)?;

    let combined_columns: Vec<String> = left_result
        .columns
        .iter()
        .chain(right_result.columns.iter())
        .cloned()
        .collect();

    let right_width = right_result.columns.len();
    let null_right = vec![Value::Null; right_width];

    let mut rows = Vec::new();

    for left_row in &left_result.rows {
        let mut matched = false;

        for right_row in &right_result.rows {
            let mut combined_values = left_row.values.clone();
            combined_values.extend_from_slice(&right_row.values);
            let combined_row = Row {
                values: combined_values,
            };

            let passes = match condition {
                Some(cond) => {
                    let val = eval_expr(cond, &combined_row, &combined_columns, pager, catalog)?;
                    is_truthy(&val)
                }
                None => true,
            };

            if passes {
                matched = true;
                rows.push(combined_row);
            }
        }

        if join_type == JoinType::Left && !matched {
            let mut combined_values = left_row.values.clone();
            combined_values.extend_from_slice(&null_right);
            rows.push(Row {
                values: combined_values,
            });
        }
    }

    Ok(QueryResult {
        columns: combined_columns,
        rows,
    })
}

fn execute_aggregate(
    input: &Plan,
    group_by: &[PlanExpr],
    aggregates: &[(AggFunc, PlanExpr, bool)],
    having: Option<&PlanExpr>,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let inner = execute(input, pager, catalog)?;
    let input_columns = &inner.columns;

    let mut groups: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();

    for (row_idx, row) in inner.rows.iter().enumerate() {
        let key: Vec<Value> = group_by
            .iter()
            .map(|expr| eval_expr(expr, row, input_columns, pager, catalog))
            .collect::<Result<Vec<_>>>()?;

        let found = groups.iter_mut().find(|(k, _)| {
            k.len() == key.len()
                && k.iter()
                    .zip(key.iter())
                    .all(|(a, b)| compare(a, b) == 0)
        });

        if let Some((_, indices)) = found {
            indices.push(row_idx);
        } else {
            groups.push((key, vec![row_idx]));
        }
    }

    if group_by.is_empty() && groups.is_empty() {
        groups.push((vec![], vec![]));
    }

    let mut output_columns = Vec::new();
    for expr in group_by {
        let name = match expr {
            PlanExpr::Column(c) => {
                if let Some(t) = &c.table {
                    format!("{}.{}", t, c.name)
                } else {
                    c.name.clone()
                }
            }
            _ => format!("{:?}", expr),
        };
        output_columns.push(name);
    }
    for (func, arg, distinct) in aggregates {
        output_columns.push(agg_column_name(func, arg, *distinct));
    }

    let mut rows = Vec::new();
    for (key_values, row_indices) in &groups {
        let group_rows: Vec<&Row> = row_indices.iter().map(|&i| &inner.rows[i]).collect();
        let mut row_values = key_values.clone();

        for (func, arg, distinct) in aggregates {
            let agg_val =
                compute_aggregate(func, arg, *distinct, &group_rows, input_columns, pager, catalog)?;
            row_values.push(agg_val);
        }

        let row = Row {
            values: row_values,
        };

        if let Some(having_expr) = having {
            let val = eval_expr(having_expr, &row, &output_columns, pager, catalog)?;
            if !is_truthy(&val) {
                continue;
            }
        }

        rows.push(row);
    }

    Ok(QueryResult {
        columns: output_columns,
        rows,
    })
}

fn compute_aggregate(
    func: &AggFunc,
    arg: &PlanExpr,
    distinct: bool,
    rows: &[&Row],
    columns: &[String],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<Value> {
    match func {
        AggFunc::Count => {
            if matches!(arg, PlanExpr::Wildcard) {
                Ok(Value::Integer(rows.len() as i64))
            } else {
                let mut count = 0i64;
                let mut seen = std::collections::HashSet::new();
                for row in rows {
                    let val = eval_expr(arg, row, columns, pager, catalog)?;
                    if !matches!(val, Value::Null) {
                        if distinct {
                            if seen.insert(value_hash_key(&val)) {
                                count += 1;
                            }
                        } else {
                            count += 1;
                        }
                    }
                }
                Ok(Value::Integer(count))
            }
        }
        AggFunc::Sum => {
            let mut sum_int = 0i64;
            let mut sum_real = 0f64;
            let mut has_real = false;
            let mut has_any = false;
            let mut seen = std::collections::HashSet::new();

            for row in rows {
                let val = eval_expr(arg, row, columns, pager, catalog)?;
                if matches!(val, Value::Null) {
                    continue;
                }
                if distinct && !seen.insert(value_hash_key(&val)) {
                    continue;
                }
                has_any = true;
                match &val {
                    Value::Integer(n) => sum_int += n,
                    Value::Real(f) => {
                        has_real = true;
                        sum_real += f;
                    }
                    _ => {}
                }
            }

            if !has_any {
                Ok(Value::Null)
            } else if has_real {
                Ok(Value::Real(sum_real + sum_int as f64))
            } else {
                Ok(Value::Integer(sum_int))
            }
        }
        AggFunc::Avg => {
            let mut sum = 0f64;
            let mut count = 0i64;
            let mut seen = std::collections::HashSet::new();

            for row in rows {
                let val = eval_expr(arg, row, columns, pager, catalog)?;
                if matches!(val, Value::Null) {
                    continue;
                }
                if distinct && !seen.insert(value_hash_key(&val)) {
                    continue;
                }
                match &val {
                    Value::Integer(n) => sum += *n as f64,
                    Value::Real(f) => sum += f,
                    _ => {}
                }
                count += 1;
            }

            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Real(sum / count as f64))
            }
        }
        AggFunc::Min => {
            let mut min: Option<Value> = None;
            for row in rows {
                let val = eval_expr(arg, row, columns, pager, catalog)?;
                if matches!(val, Value::Null) {
                    continue;
                }
                min = Some(match min {
                    Some(current) if compare(&val, &current) < 0 => val,
                    Some(current) => current,
                    None => val,
                });
            }
            Ok(min.unwrap_or(Value::Null))
        }
        AggFunc::Max => {
            let mut max: Option<Value> = None;
            for row in rows {
                let val = eval_expr(arg, row, columns, pager, catalog)?;
                if matches!(val, Value::Null) {
                    continue;
                }
                max = Some(match max {
                    Some(current) if compare(&val, &current) > 0 => val,
                    Some(current) => current,
                    None => val,
                });
            }
            Ok(max.unwrap_or(Value::Null))
        }
        AggFunc::Total => {
            let mut sum = 0f64;
            let mut seen = std::collections::HashSet::new();
            for row in rows {
                let val = eval_expr(arg, row, columns, pager, catalog)?;
                if matches!(val, Value::Null) {
                    continue;
                }
                if distinct && !seen.insert(value_hash_key(&val)) {
                    continue;
                }
                match &val {
                    Value::Integer(n) => sum += *n as f64,
                    Value::Real(f) => sum += f,
                    _ => {}
                }
            }
            Ok(Value::Real(sum))
        }
        AggFunc::GroupConcat { separator } => {
            let sep = separator.as_deref().unwrap_or(",");
            let mut parts = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for row in rows {
                let val = eval_expr(arg, row, columns, pager, catalog)?;
                if matches!(val, Value::Null) {
                    continue;
                }
                let text = value_to_text(&val);
                if distinct {
                    if !seen.insert(text.clone()) {
                        continue;
                    }
                }
                parts.push(text);
            }
            if parts.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(Value::Text(parts.join(sep)))
            }
        }
    }
}

fn value_hash_key(val: &Value) -> Vec<u8> {
    let mut key = Vec::new();
    match val {
        Value::Null => key.push(0),
        Value::Integer(n) => {
            key.push(1);
            key.extend_from_slice(&n.to_le_bytes());
        }
        Value::Real(f) => {
            key.push(2);
            key.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            key.push(3);
            key.extend_from_slice(s.as_bytes());
        }
        Value::Blob(b) => {
            key.push(4);
            key.extend_from_slice(b);
        }
    }
    key
}

fn eval_expr(expr: &PlanExpr, row: &Row, columns: &[String], pager: &mut Pager, catalog: &Catalog) -> Result<Value> {
    match expr {
        PlanExpr::Column(col_ref) => {
            let qualified = col_ref
                .table
                .as_ref()
                .map(|t| format!("{}.{}", t, col_ref.name));

            let idx = if let Some(ref qname) = qualified {
                columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(qname))
            } else {
                None
            }
            .or_else(|| {
                columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(&col_ref.name))
            })
            .or_else(|| {
                columns.iter().position(|c| {
                    c.rsplit('.').next().is_some_and(|suffix| {
                        suffix.eq_ignore_ascii_case(&col_ref.name)
                    })
                })
            })
            .ok_or_else(|| {
                Error::Other(format!(
                    "column not found in row: {}",
                    qualified.as_deref().unwrap_or(&col_ref.name)
                ))
            })?;
            Ok(row.values.get(idx).cloned().unwrap_or(Value::Null))
        }
        PlanExpr::Rowid => {
            // Rowid should be mapped to the rowid-alias column by the scan
            Err(Error::Other(
                "bare ROWID reference not yet supported".to_string(),
            ))
        }
        PlanExpr::Literal(lit) => Ok(literal_to_value(lit)),
        PlanExpr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, row, columns, pager, catalog)?;
            let r = eval_expr(right, row, columns, pager, catalog)?;
            let nocase = has_nocase_collation(left) || has_nocase_collation(right);
            if nocase {
                let l = fold_nocase(&l);
                let r = fold_nocase(&r);
                eval_binop(*op, &l, &r)
            } else {
                eval_binop(*op, &l, &r)
            }
        }
        PlanExpr::UnaryOp { op, operand } => {
            let v = eval_expr(operand, row, columns, pager, catalog)?;
            eval_unaryop(*op, &v)
        }
        PlanExpr::IsNull(inner) => {
            let v = eval_expr(inner, row, columns, pager, catalog)?;
            Ok(Value::Integer(if matches!(v, Value::Null) {
                1
            } else {
                0
            }))
        }
        PlanExpr::IsNotNull(inner) => {
            let v = eval_expr(inner, row, columns, pager, catalog)?;
            Ok(Value::Integer(if matches!(v, Value::Null) {
                0
            } else {
                1
            }))
        }
        PlanExpr::Wildcard => Err(Error::Other("wildcard in expression context".to_string())),
        PlanExpr::Aggregate { func, arg, distinct } => {
            let name = agg_column_name(func, arg, *distinct);
            let idx = columns
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&name))
                .ok_or_else(|| {
                    Error::Other(format!("aggregate column not found: {name}"))
                })?;
            Ok(row.values.get(idx).cloned().unwrap_or(Value::Null))
        }
        PlanExpr::Function { name, args } => {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| eval_expr(a, row, columns, pager, catalog))
                .collect::<Result<Vec<_>>>()?;
            eval_scalar_function(name, &vals)
        }
        PlanExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let val = eval_expr(expr, row, columns, pager, catalog)?;
            let pat = eval_expr(pattern, row, columns, pager, catalog)?;
            if matches!(val, Value::Null) || matches!(pat, Value::Null) {
                return Ok(Value::Null);
            }
            let val_str = value_to_text(&val);
            let pat_str = value_to_text(&pat);
            let matched = like_match(&pat_str, &val_str);
            let result = if *negated { !matched } else { matched };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }
        PlanExpr::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_expr(expr, row, columns, pager, catalog)?;
            if matches!(val, Value::Null) {
                return Ok(Value::Null);
            }
            let nocase = has_nocase_collation(expr);
            let val_cmp = if nocase { fold_nocase(&val) } else { val.clone() };
            let mut found = false;
            for item in list {
                let item_val = eval_expr(item, row, columns, pager, catalog)?;
                let item_cmp = if nocase { fold_nocase(&item_val) } else { item_val };
                if values_equal(&val_cmp, &item_cmp) {
                    found = true;
                    break;
                }
            }
            let result = if *negated { !found } else { found };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }
        PlanExpr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let op_val = operand
                .as_ref()
                .map(|e| eval_expr(e, row, columns, pager, catalog))
                .transpose()?;
            for (condition, result) in when_clauses {
                let cond_val = eval_expr(condition, row, columns, pager, catalog)?;
                let matched = if let Some(ref ov) = op_val {
                    values_equal(ov, &cond_val)
                } else {
                    is_truthy(&cond_val)
                };
                if matched {
                    return eval_expr(result, row, columns, pager, catalog);
                }
            }
            if let Some(else_expr) = else_result {
                eval_expr(else_expr, row, columns, pager, catalog)
            } else {
                Ok(Value::Null)
            }
        }
        PlanExpr::Cast { expr, type_name } => {
            let val = eval_expr(expr, row, columns, pager, catalog)?;
            eval_cast(val, type_name)
        }
        PlanExpr::Subquery(sub_plan) => {
            let result = execute(sub_plan, pager, catalog)?;
            Ok(result
                .rows
                .first()
                .and_then(|r| r.values.first().cloned())
                .unwrap_or(Value::Null))
        }
        PlanExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let val = eval_expr(expr, row, columns, pager, catalog)?;
            if matches!(val, Value::Null) {
                return Ok(Value::Null);
            }
            let result = execute(subquery, pager, catalog)?;
            let mut found = false;
            for sub_row in &result.rows {
                if let Some(sub_val) = sub_row.values.first() {
                    if values_equal(&val, sub_val) {
                        found = true;
                        break;
                    }
                }
            }
            let result = if *negated { !found } else { found };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }
        PlanExpr::Exists { subquery, negated } => {
            let result = execute(subquery, pager, catalog)?;
            let exists = !result.rows.is_empty();
            let result = if *negated { !exists } else { exists };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }
        PlanExpr::Param(index) => Ok(get_param(*index)),
        PlanExpr::WindowFunction { .. } => {
            Err(Error::Other("window function should not be evaluated directly".into()))
        }
        PlanExpr::Collate { expr, .. } => {
            eval_expr(expr, row, columns, pager, catalog)
        }
    }
}

fn has_nocase_collation(expr: &PlanExpr) -> bool {
    matches!(expr, PlanExpr::Collate { collation, .. } if collation == "NOCASE")
}

fn fold_nocase(val: &Value) -> Value {
    match val {
        Value::Text(s) => Value::Text(s.to_lowercase()),
        other => other.clone(),
    }
}

fn execute_window(
    input: &Plan,
    window_exprs: &[(PlanExpr, String)],
    output_columns: &[String],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let inner = execute(input, pager, catalog)?;
    let input_columns = &inner.columns;
    let mut rows: Vec<Vec<Value>> = inner.rows.iter().map(|r| r.values.clone()).collect();

    for (win_expr, _alias) in window_exprs {
        if let PlanExpr::WindowFunction { func_name, args, partition_by, order_by } = win_expr {
            let partitions = partition_rows(&rows, partition_by, input_columns, output_columns, pager, catalog)?;

            let mut result_values: Vec<Value> = vec![Value::Null; rows.len()];

            for mut partition_indices in partitions {
                if !order_by.is_empty() {
                    sort_partition(&rows, &mut partition_indices, order_by, input_columns, output_columns, pager, catalog)?;
                }

                compute_window_for_partition(
                    func_name, args, order_by, &partition_indices, &rows,
                    input_columns, output_columns,
                    &mut result_values, pager, catalog,
                )?;
            }

            for (i, row) in rows.iter_mut().enumerate() {
                row.push(result_values[i].clone());
            }
        }
    }

    Ok(QueryResult {
        columns: output_columns.to_vec(),
        rows: rows.into_iter().map(|values| Row { values }).collect(),
    })
}

fn partition_rows(
    rows: &[Vec<Value>],
    partition_by: &[PlanExpr],
    input_columns: &[String],
    all_columns: &[String],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<Vec<Vec<usize>>> {
    if partition_by.is_empty() {
        return Ok(vec![(0..rows.len()).collect()]);
    }

    let mut partition_map: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();

    for (i, row) in rows.iter().enumerate() {
        let tmp_row = Row { values: row.clone() };
        let cols = if input_columns.len() >= row.len() { input_columns } else { all_columns };
        let key: Vec<Value> = partition_by
            .iter()
            .map(|e| eval_expr(e, &tmp_row, cols, pager, catalog))
            .collect::<Result<Vec<_>>>()?;

        if let Some(entry) = partition_map.iter_mut().find(|(k, _)| *k == key) {
            entry.1.push(i);
        } else {
            partition_map.push((key, vec![i]));
        }
    }

    Ok(partition_map.into_iter().map(|(_, indices)| indices).collect())
}

fn sort_partition(
    rows: &[Vec<Value>],
    indices: &mut [usize],
    order_by: &[(PlanExpr, bool)],
    input_columns: &[String],
    all_columns: &[String],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    let cols = if input_columns.len() >= rows.get(0).map_or(0, |r| r.len()) { input_columns } else { all_columns };
    let mut sort_keys: Vec<(usize, Vec<Value>)> = indices
        .iter()
        .map(|&idx| {
            let tmp_row = Row { values: rows[idx].clone() };
            let keys: Vec<Value> = order_by
                .iter()
                .map(|(e, _)| eval_expr(e, &tmp_row, cols, pager, catalog).unwrap_or(Value::Null))
                .collect();
            (idx, keys)
        })
        .collect();

    sort_keys.sort_by(|a, b| {
        for (i, (_expr, desc)) in order_by.iter().enumerate() {
            let cmp = compare(&a.1[i], &b.1[i]);
            if cmp != 0 {
                return if *desc {
                    compare_ordering(cmp).reverse()
                } else {
                    compare_ordering(cmp)
                };
            }
        }
        std::cmp::Ordering::Equal
    });

    for (i, (idx, _)) in sort_keys.into_iter().enumerate() {
        indices[i] = idx;
    }
    Ok(())
}

fn compare_ordering(cmp: i32) -> std::cmp::Ordering {
    if cmp < 0 { std::cmp::Ordering::Less }
    else if cmp > 0 { std::cmp::Ordering::Greater }
    else { std::cmp::Ordering::Equal }
}

fn compute_window_for_partition(
    func_name: &str,
    args: &[PlanExpr],
    order_by: &[(PlanExpr, bool)],
    partition_indices: &[usize],
    rows: &[Vec<Value>],
    input_columns: &[String],
    all_columns: &[String],
    result_values: &mut [Value],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    let cols = if input_columns.len() >= rows.get(0).map_or(0, |r| r.len()) { input_columns } else { all_columns };
    let partition_len = partition_indices.len();

    match func_name {
        "ROW_NUMBER" => {
            for (rank, &row_idx) in partition_indices.iter().enumerate() {
                result_values[row_idx] = Value::Integer((rank + 1) as i64);
            }
        }
        "RANK" => {
            let mut rank = 1i64;
            let order_exprs: Vec<&PlanExpr> = order_by.iter().map(|(e, _)| e).collect();
            for i in 0..partition_len {
                let row_idx = partition_indices[i];
                if i == 0 {
                    result_values[row_idx] = Value::Integer(1);
                } else {
                    let prev_idx = partition_indices[i - 1];
                    let prev_row = Row { values: rows[prev_idx].clone() };
                    let curr_row = Row { values: rows[row_idx].clone() };
                    let same = order_exprs.is_empty() || {
                        let prev_vals: Vec<Value> = order_exprs.iter()
                            .map(|a| eval_expr(a, &prev_row, cols, pager, catalog).unwrap_or(Value::Null))
                            .collect();
                        let curr_vals: Vec<Value> = order_exprs.iter()
                            .map(|a| eval_expr(a, &curr_row, cols, pager, catalog).unwrap_or(Value::Null))
                            .collect();
                        prev_vals == curr_vals
                    };
                    if !same {
                        rank = (i + 1) as i64;
                    }
                    result_values[row_idx] = Value::Integer(rank);
                }
            }
        }
        "DENSE_RANK" => {
            let mut rank = 1i64;
            let order_exprs: Vec<&PlanExpr> = order_by.iter().map(|(e, _)| e).collect();
            for i in 0..partition_len {
                let row_idx = partition_indices[i];
                if i == 0 {
                    result_values[row_idx] = Value::Integer(1);
                } else {
                    let prev_idx = partition_indices[i - 1];
                    let prev_row = Row { values: rows[prev_idx].clone() };
                    let curr_row = Row { values: rows[row_idx].clone() };
                    let same = order_exprs.is_empty() || {
                        let prev_vals: Vec<Value> = order_exprs.iter()
                            .map(|a| eval_expr(a, &prev_row, cols, pager, catalog).unwrap_or(Value::Null))
                            .collect();
                        let curr_vals: Vec<Value> = order_exprs.iter()
                            .map(|a| eval_expr(a, &curr_row, cols, pager, catalog).unwrap_or(Value::Null))
                            .collect();
                        prev_vals == curr_vals
                    };
                    if !same {
                        rank += 1;
                    }
                    result_values[row_idx] = Value::Integer(rank);
                }
            }
        }
        "NTILE" => {
            let n = if let Some(arg) = args.first() {
                let tmp_row = Row { values: rows[partition_indices[0]].clone() };
                match eval_expr(arg, &tmp_row, cols, pager, catalog)? {
                    Value::Integer(n) => n.max(1) as usize,
                    _ => 1,
                }
            } else {
                1
            };
            for (i, &row_idx) in partition_indices.iter().enumerate() {
                let bucket = (i * n / partition_len) + 1;
                result_values[row_idx] = Value::Integer(bucket as i64);
            }
        }
        "LAG" | "LEAD" => {
            let offset = if args.len() > 1 {
                let tmp_row = Row { values: rows[partition_indices[0]].clone() };
                match eval_expr(&args[1], &tmp_row, cols, pager, catalog)? {
                    Value::Integer(n) => n as usize,
                    _ => 1,
                }
            } else {
                1
            };
            let default_val = if args.len() > 2 {
                let tmp_row = Row { values: rows[partition_indices[0]].clone() };
                eval_expr(&args[2], &tmp_row, cols, pager, catalog)?
            } else {
                Value::Null
            };

            for (i, &row_idx) in partition_indices.iter().enumerate() {
                let target_pos = if func_name == "LAG" {
                    if i >= offset { Some(i - offset) } else { None }
                } else {
                    let next = i + offset;
                    if next < partition_len { Some(next) } else { None }
                };

                let val = if let Some(pos) = target_pos {
                    if let Some(arg_expr) = args.first() {
                        let target_row = Row { values: rows[partition_indices[pos]].clone() };
                        eval_expr(arg_expr, &target_row, cols, pager, catalog)?
                    } else {
                        Value::Null
                    }
                } else {
                    default_val.clone()
                };
                result_values[row_idx] = val;
            }
        }
        "FIRST_VALUE" => {
            if let Some(arg) = args.first() {
                let first_row = Row { values: rows[partition_indices[0]].clone() };
                let val = eval_expr(arg, &first_row, cols, pager, catalog)?;
                for &row_idx in partition_indices {
                    result_values[row_idx] = val.clone();
                }
            }
        }
        "LAST_VALUE" => {
            if let Some(arg) = args.first() {
                let last_row = Row { values: rows[*partition_indices.last().unwrap()].clone() };
                let val = eval_expr(arg, &last_row, cols, pager, catalog)?;
                for &row_idx in partition_indices {
                    result_values[row_idx] = val.clone();
                }
            }
        }
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "TOTAL" => {
            let arg = args.first();
            let is_count_star = arg.is_none() || matches!(arg, Some(PlanExpr::Wildcard));

            let mut agg_values: Vec<Value> = Vec::new();
            for &row_idx in partition_indices {
                if is_count_star {
                    agg_values.push(Value::Integer(1));
                } else if let Some(arg_expr) = arg {
                    let tmp_row = Row { values: rows[row_idx].clone() };
                    let val = eval_expr(arg_expr, &tmp_row, cols, pager, catalog)?;
                    if !matches!(val, Value::Null) {
                        agg_values.push(val);
                    }
                }
            }

            let result = match func_name {
                "COUNT" => Value::Integer(agg_values.len() as i64),
                "SUM" => {
                    if agg_values.is_empty() {
                        Value::Null
                    } else {
                        let mut sum_i: i64 = 0;
                        let mut sum_f: f64 = 0.0;
                        let mut is_real = false;
                        for v in &agg_values {
                            match v {
                                Value::Integer(n) => sum_i += n,
                                Value::Real(f) => { sum_f += f; is_real = true; }
                                _ => {}
                            }
                        }
                        if is_real { Value::Real(sum_f + sum_i as f64) } else { Value::Integer(sum_i) }
                    }
                }
                "TOTAL" => {
                    let mut total: f64 = 0.0;
                    for v in &agg_values {
                        match v {
                            Value::Integer(n) => total += *n as f64,
                            Value::Real(f) => total += f,
                            _ => {}
                        }
                    }
                    Value::Real(total)
                }
                "AVG" => {
                    if agg_values.is_empty() {
                        Value::Null
                    } else {
                        let mut sum: f64 = 0.0;
                        for v in &agg_values {
                            match v {
                                Value::Integer(n) => sum += *n as f64,
                                Value::Real(f) => sum += f,
                                _ => {}
                            }
                        }
                        Value::Real(sum / agg_values.len() as f64)
                    }
                }
                "MIN" => {
                    if agg_values.is_empty() { Value::Null }
                    else {
                        let mut min = agg_values[0].clone();
                        for v in &agg_values[1..] {
                            if compare(v, &min) < 0 { min = v.clone(); }
                        }
                        min
                    }
                }
                "MAX" => {
                    if agg_values.is_empty() { Value::Null }
                    else {
                        let mut max = agg_values[0].clone();
                        for v in &agg_values[1..] {
                            if compare(v, &max) > 0 { max = v.clone(); }
                        }
                        max
                    }
                }
                _ => Value::Null,
            };

            for &row_idx in partition_indices {
                result_values[row_idx] = result.clone();
            }
        }
        _ => {
            return Err(Error::Other(format!("unknown window function: {func_name}")));
        }
    }
    Ok(())
}


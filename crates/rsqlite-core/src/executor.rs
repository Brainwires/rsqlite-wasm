use rsqlite_storage::btree::{
    btree_create_index, btree_create_table, btree_delete, btree_index_delete, btree_index_insert,
    btree_insert, btree_max_rowid, delete_schema_entries, insert_schema_entry, BTreeCursor,
    CursorRow, IndexCursor,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::planner::{
    agg_column_name, AggFunc, BinOp, ColumnRef, CreateIndexPlan, CreateTablePlan, DeletePlan,
    InsertPlan, JoinType, LiteralValue, Plan, PlanExpr, ProjectionItem, SortKey, UnaryOp,
    UpdatePlan,
};
use crate::types::{QueryResult, Row};

#[derive(Debug)]
pub struct ExecResult {
    pub rows_affected: u64,
}

pub fn execute(plan: &Plan, pager: &mut Pager, catalog: &Catalog) -> Result<QueryResult> {
    match plan {
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
        Plan::CreateTable(_)
        | Plan::CreateIndex(_)
        | Plan::Insert(_)
        | Plan::Update(_)
        | Plan::Delete(_)
        | Plan::AlterTableAddColumn { .. }
        | Plan::DropTable { .. }
        | Plan::DropIndex { .. }
        | Plan::Begin
        | Plan::Commit
        | Plan::Rollback => Err(Error::Other(
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

    Ok(ExecResult { rows_affected: 0 })
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

fn execute_insert(plan: &InsertPlan, pager: &mut Pager, catalog: &Catalog) -> Result<ExecResult> {
    let table_indexes = get_table_indexes(catalog, &plan.table_name);
    let mut rows_affected = 0u64;
    let mut current_root = plan.root_page;

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
            None => btree_max_rowid(pager, current_root)? + 1,
        };

        let record = Record {
            values: values.clone(),
        };
        current_root = btree_insert(pager, current_root, rowid, &record)?;

        for (idx_root, idx_col_indices) in &table_indexes {
            let key = build_index_key(&values, idx_col_indices, &plan.table_columns, rowid);
            btree_index_insert(pager, *idx_root, &key)
                .map_err(|e| Error::Other(e.to_string()))?;
        }

        rows_affected += 1;
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    Ok(ExecResult { rows_affected })
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

        let cmp_val = compare(&va, &vb);
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
            eval_binop(*op, &l, &r)
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
            let mut found = false;
            for item in list {
                let item_val = eval_expr(item, row, columns, pager, catalog)?;
                if values_equal(&val, &item_val) {
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
    }
}

fn eval_scalar_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "LENGTH" => {
            if args.is_empty() {
                return Err(Error::Other("LENGTH requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Integer(s.chars().count() as i64)),
                Value::Blob(b) => Ok(Value::Integer(b.len() as i64)),
                Value::Integer(_) | Value::Real(_) => {
                    let s = value_to_text(&args[0]);
                    Ok(Value::Integer(s.len() as i64))
                }
            }
        }
        "UPPER" => {
            if args.is_empty() {
                return Err(Error::Other("UPPER requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                other => Ok(Value::Text(value_to_text(other).to_uppercase())),
            }
        }
        "LOWER" => {
            if args.is_empty() {
                return Err(Error::Other("LOWER requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                other => Ok(Value::Text(value_to_text(other).to_lowercase())),
            }
        }
        "ABS" => {
            if args.is_empty() {
                return Err(Error::Other("ABS requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(n) => Ok(Value::Integer(n.abs())),
                Value::Real(f) => Ok(Value::Real(f.abs())),
                _ => Ok(Value::Integer(0)),
            }
        }
        "TYPEOF" => {
            if args.is_empty() {
                return Err(Error::Other("TYPEOF requires 1 argument".into()));
            }
            let t = match &args[0] {
                Value::Null => "null",
                Value::Integer(_) => "integer",
                Value::Real(_) => "real",
                Value::Text(_) => "text",
                Value::Blob(_) => "blob",
            };
            Ok(Value::Text(t.to_string()))
        }
        "COALESCE" => {
            for v in args {
                if !matches!(v, Value::Null) {
                    return Ok(v.clone());
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" => {
            if args.len() < 2 {
                return Err(Error::Other("IFNULL requires 2 arguments".into()));
            }
            if matches!(args[0], Value::Null) {
                Ok(args[1].clone())
            } else {
                Ok(args[0].clone())
            }
        }
        "NULLIF" => {
            if args.len() < 2 {
                return Err(Error::Other("NULLIF requires 2 arguments".into()));
            }
            if compare(&args[0], &args[1]) == 0 {
                Ok(Value::Null)
            } else {
                Ok(args[0].clone())
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            if args.len() < 2 {
                return Err(Error::Other("SUBSTR requires 2-3 arguments".into()));
            }
            if matches!(args[0], Value::Null) {
                return Ok(Value::Null);
            }
            let s = value_to_text(&args[0]);
            let chars: Vec<char> = s.chars().collect();
            let start = match &args[1] {
                Value::Integer(n) => *n,
                _ => 1,
            };
            // SQLite SUBSTR is 1-indexed; negative means from end
            let (start_idx, take_len) = if start > 0 {
                let idx = (start - 1) as usize;
                let len = if args.len() > 2 {
                    match &args[2] {
                        Value::Integer(n) => *n as usize,
                        _ => chars.len(),
                    }
                } else {
                    chars.len()
                };
                (idx, len)
            } else if start == 0 {
                let len = if args.len() > 2 {
                    match &args[2] {
                        Value::Integer(n) => (*n as usize).saturating_sub(1),
                        _ => chars.len(),
                    }
                } else {
                    chars.len()
                };
                (0, len)
            } else {
                let from_end = (-start) as usize;
                let idx = chars.len().saturating_sub(from_end);
                let len = if args.len() > 2 {
                    match &args[2] {
                        Value::Integer(n) => *n as usize,
                        _ => chars.len(),
                    }
                } else {
                    chars.len()
                };
                (idx, len)
            };
            let result: String = chars
                .iter()
                .skip(start_idx)
                .take(take_len)
                .collect();
            Ok(Value::Text(result))
        }
        "REPLACE" => {
            if args.len() < 3 {
                return Err(Error::Other("REPLACE requires 3 arguments".into()));
            }
            if matches!(args[0], Value::Null) {
                return Ok(Value::Null);
            }
            let s = value_to_text(&args[0]);
            let from = value_to_text(&args[1]);
            let to = value_to_text(&args[2]);
            Ok(Value::Text(s.replace(&from, &to)))
        }
        "INSTR" => {
            if args.len() < 2 {
                return Err(Error::Other("INSTR requires 2 arguments".into()));
            }
            if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
                return Ok(Value::Null);
            }
            let haystack = value_to_text(&args[0]);
            let needle = value_to_text(&args[1]);
            match haystack.find(&needle) {
                Some(pos) => {
                    let char_pos = haystack[..pos].chars().count() + 1;
                    Ok(Value::Integer(char_pos as i64))
                }
                None => Ok(Value::Integer(0)),
            }
        }
        "TRIM" => {
            if args.is_empty() {
                return Err(Error::Other("TRIM requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                other => Ok(Value::Text(value_to_text(other).trim().to_string())),
            }
        }
        "LTRIM" => {
            if args.is_empty() {
                return Err(Error::Other("LTRIM requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                other => Ok(Value::Text(value_to_text(other).trim_start().to_string())),
            }
        }
        "RTRIM" => {
            if args.is_empty() {
                return Err(Error::Other("RTRIM requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                other => Ok(Value::Text(value_to_text(other).trim_end().to_string())),
            }
        }
        "HEX" => {
            if args.is_empty() {
                return Err(Error::Other("HEX requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Blob(b) => {
                    let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
                other => {
                    let s = value_to_text(other);
                    let hex: String = s.bytes().map(|b| format!("{:02X}", b)).collect();
                    Ok(Value::Text(hex))
                }
            }
        }
        "QUOTE" => {
            if args.is_empty() {
                return Err(Error::Other("QUOTE requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Text("NULL".to_string())),
                Value::Integer(n) => Ok(Value::Text(n.to_string())),
                Value::Real(f) => Ok(Value::Text(f.to_string())),
                Value::Text(s) => {
                    let escaped = s.replace('\'', "''");
                    Ok(Value::Text(format!("'{escaped}'")))
                }
                Value::Blob(b) => {
                    let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(format!("X'{hex}'")))
                }
            }
        }
        "UNICODE" => {
            if args.is_empty() {
                return Err(Error::Other("UNICODE requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => match s.chars().next() {
                    Some(c) => Ok(Value::Integer(c as i64)),
                    None => Ok(Value::Null),
                },
                other => {
                    let s = value_to_text(other);
                    match s.chars().next() {
                        Some(c) => Ok(Value::Integer(c as i64)),
                        None => Ok(Value::Null),
                    }
                }
            }
        }
        "CHAR" => {
            let mut result = String::new();
            for v in args {
                if let Value::Integer(n) = v {
                    if let Some(c) = char::from_u32(*n as u32) {
                        result.push(c);
                    }
                }
            }
            Ok(Value::Text(result))
        }
        "ZEROBLOB" => {
            if args.is_empty() {
                return Err(Error::Other("ZEROBLOB requires 1 argument".into()));
            }
            match &args[0] {
                Value::Integer(n) => Ok(Value::Blob(vec![0u8; *n as usize])),
                _ => Ok(Value::Blob(vec![])),
            }
        }
        "RANDOM" => Ok(Value::Integer(rand_i64())),
        _ => Err(Error::Other(format!("unknown function: {name}"))),
    }
}

fn value_to_text(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Integer(n) => n.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

fn like_match(pattern: &str, value: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let val: Vec<char> = value.chars().collect();
    like_match_inner(&pat, &val)
}

fn like_match_inner(pattern: &[char], value: &[char]) -> bool {
    let mut pi = 0;
    let mut vi = 0;
    let mut star_pi = usize::MAX;
    let mut star_vi = 0;

    while vi < value.len() {
        if pi < pattern.len() && pattern[pi] == '%' {
            star_pi = pi;
            star_vi = vi;
            pi += 1;
        } else if pi < pattern.len()
            && (pattern[pi] == '_'
                || pattern[pi].to_ascii_lowercase() == value[vi].to_ascii_lowercase())
        {
            pi += 1;
            vi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_vi += 1;
            vi = star_vi;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == '%' {
        pi += 1;
    }
    pi == pattern.len()
}

fn eval_cast(val: Value, type_name: &str) -> Result<Value> {
    if matches!(val, Value::Null) {
        return Ok(Value::Null);
    }
    let upper = type_name.to_uppercase();
    if upper.contains("INT") {
        match &val {
            Value::Integer(_) => Ok(val),
            Value::Real(f) => Ok(Value::Integer(*f as i64)),
            Value::Text(s) => {
                let n = s.trim().parse::<i64>().unwrap_or(0);
                Ok(Value::Integer(n))
            }
            Value::Blob(b) => {
                let s = String::from_utf8_lossy(b);
                let n = s.trim().parse::<i64>().unwrap_or(0);
                Ok(Value::Integer(n))
            }
            Value::Null => Ok(Value::Null),
        }
    } else if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUBLE") {
        match &val {
            Value::Real(_) => Ok(val),
            Value::Integer(n) => Ok(Value::Real(*n as f64)),
            Value::Text(s) => {
                let f = s.trim().parse::<f64>().unwrap_or(0.0);
                Ok(Value::Real(f))
            }
            _ => Ok(Value::Real(0.0)),
        }
    } else if upper.contains("TEXT") || upper.contains("CHAR") || upper.contains("CLOB") {
        Ok(Value::Text(value_to_text(&val)))
    } else if upper.contains("BLOB") {
        match val {
            Value::Blob(_) => Ok(val),
            Value::Text(s) => Ok(Value::Blob(s.into_bytes())),
            _ => Ok(Value::Blob(value_to_text(&val).into_bytes())),
        }
    } else {
        Ok(val)
    }
}

fn rand_i64() -> i64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;
    let mut h = DefaultHasher::new();
    SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    h.finish() as i64
}

fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Null => Value::Null,
        LiteralValue::Integer(n) => Value::Integer(*n),
        LiteralValue::Real(f) => Value::Real(*f),
        LiteralValue::Text(s) => Value::Text(s.clone()),
        LiteralValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
    }
}

fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Null => false,
        Value::Integer(0) => false,
        Value::Integer(_) => true,
        Value::Real(f) => *f != 0.0,
        Value::Text(s) => !s.is_empty(),
        Value::Blob(b) => !b.is_empty(),
    }
}

fn eval_binop(op: BinOp, left: &Value, right: &Value) -> Result<Value> {
    // NULL propagation for most operators
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return match op {
            BinOp::And => {
                // FALSE AND NULL => FALSE, NULL AND TRUE => NULL
                if matches!(left, Value::Integer(0)) || matches!(right, Value::Integer(0)) {
                    Ok(Value::Integer(0))
                } else {
                    Ok(Value::Null)
                }
            }
            BinOp::Or => {
                // TRUE OR NULL => TRUE, NULL OR FALSE => NULL
                if is_truthy(left) || is_truthy(right) {
                    Ok(Value::Integer(1))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null),
        };
    }

    match op {
        BinOp::Eq => Ok(Value::Integer(if compare(left, right) == 0 { 1 } else { 0 })),
        BinOp::NotEq => Ok(Value::Integer(if compare(left, right) != 0 { 1 } else { 0 })),
        BinOp::Lt => Ok(Value::Integer(if compare(left, right) < 0 { 1 } else { 0 })),
        BinOp::LtEq => Ok(Value::Integer(if compare(left, right) <= 0 { 1 } else { 0 })),
        BinOp::Gt => Ok(Value::Integer(if compare(left, right) > 0 { 1 } else { 0 })),
        BinOp::GtEq => Ok(Value::Integer(if compare(left, right) >= 0 { 1 } else { 0 })),
        BinOp::And => Ok(Value::Integer(
            if is_truthy(left) && is_truthy(right) {
                1
            } else {
                0
            },
        )),
        BinOp::Or => Ok(Value::Integer(
            if is_truthy(left) || is_truthy(right) {
                1
            } else {
                0
            },
        )),
        BinOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinOp::Div => {
            // Integer division truncates
            numeric_op(left, right, |a, b| if b != 0 { a / b } else { 0 }, |a, b| a / b)
        }
        BinOp::Mod => {
            numeric_op(left, right, |a, b| if b != 0 { a % b } else { 0 }, |a, b| a % b)
        }
        BinOp::Concat => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                return Ok(Value::Null);
            }
            let l = value_to_text(left);
            let r = value_to_text(right);
            Ok(Value::Text(format!("{l}{r}")))
        }
    }
}

fn eval_unaryop(op: UnaryOp, val: &Value) -> Result<Value> {
    match (op, val) {
        (UnaryOp::Not, Value::Null) => Ok(Value::Null),
        (UnaryOp::Not, v) => Ok(Value::Integer(if is_truthy(v) { 0 } else { 1 })),
        (UnaryOp::Neg, Value::Null) => Ok(Value::Null),
        (UnaryOp::Neg, Value::Integer(n)) => Ok(Value::Integer(-n)),
        (UnaryOp::Neg, Value::Real(f)) => Ok(Value::Real(-f)),
        (UnaryOp::Neg, _) => Ok(Value::Integer(0)),
    }
}

/// SQLite comparison ordering: NULL < INTEGER/REAL < TEXT < BLOB
fn type_order(val: &Value) -> i32 {
    match val {
        Value::Null => 0,
        Value::Integer(_) => 1,
        Value::Real(_) => 1,
        Value::Text(_) => 2,
        Value::Blob(_) => 3,
    }
}

fn compare(left: &Value, right: &Value) -> i32 {
    let lo = type_order(left);
    let ro = type_order(right);
    if lo != ro {
        return lo - ro;
    }

    match (left, right) {
        (Value::Null, Value::Null) => 0,
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b) as i32,
        (Value::Real(a), Value::Real(b)) => a.partial_cmp(b).map_or(0, |o| o as i32),
        (Value::Integer(a), Value::Real(b)) => (*a as f64).partial_cmp(b).map_or(0, |o| o as i32),
        (Value::Real(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)).map_or(0, |o| o as i32),
        (Value::Text(a), Value::Text(b)) => a.cmp(b) as i32,
        (Value::Blob(a), Value::Blob(b)) => a.cmp(b) as i32,
        _ => 0,
    }
}

fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(int_op(*a, *b))),
        (Value::Real(a), Value::Real(b)) => Ok(Value::Real(float_op(*a, *b))),
        (Value::Integer(a), Value::Real(b)) => Ok(Value::Real(float_op(*a as f64, *b))),
        (Value::Real(a), Value::Integer(b)) => Ok(Value::Real(float_op(*a, *b as f64))),
        _ => Ok(Value::Integer(0)),
    }
}

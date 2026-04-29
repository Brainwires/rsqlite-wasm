use rsqlite_storage::btree::{
    btree_create_table, btree_delete, btree_insert, btree_max_rowid, insert_schema_entry,
    BTreeCursor,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::planner::{
    BinOp, ColumnRef, CreateTablePlan, DeletePlan, InsertPlan, LiteralValue, Plan, PlanExpr,
    ProjectionItem, UnaryOp, UpdatePlan,
};
use crate::types::{QueryResult, Row};

pub struct ExecResult {
    pub rows_affected: u64,
}

pub fn execute(plan: &Plan, pager: &mut Pager) -> Result<QueryResult> {
    match plan {
        Plan::Project { input, outputs } => execute_project(input, outputs, pager),
        Plan::Filter { input, predicate } => {
            let inner = execute(input, pager)?;
            let input_columns = &inner.columns;
            let mut filtered_rows = Vec::new();
            for row in &inner.rows {
                let val = eval_expr(predicate, row, input_columns)?;
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
        Plan::CreateTable(_)
        | Plan::Insert(_)
        | Plan::Update(_)
        | Plan::Delete(_)
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
        Plan::Insert(ins) => execute_insert(ins, pager),
        Plan::Update(upd) => execute_update(upd, pager),
        Plan::Delete(del) => execute_delete(del, pager),
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

fn execute_insert(plan: &InsertPlan, pager: &mut Pager) -> Result<ExecResult> {
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

        let record = Record { values };
        current_root = btree_insert(pager, current_root, rowid, &record)?;
        rows_affected += 1;
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    Ok(ExecResult { rows_affected })
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

fn execute_update(plan: &UpdatePlan, pager: &mut Pager) -> Result<ExecResult> {
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
                let val = eval_expr(pred, &row, &column_names)?;
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
                new_values[col_idx] = eval_expr(expr, &row, &column_names)?;
            }
            to_update.push((btree_row.rowid, new_values));
        }
    }

    let rows_affected = to_update.len() as u64;

    let mut current_root = plan.root_page;
    for (rowid, values) in to_update {
        btree_delete(pager, current_root, rowid)?;
        let record = Record { values };
        current_root = btree_insert(pager, current_root, rowid, &record)?;
    }

    if !pager.in_transaction() {
        pager.flush()?;
    }

    Ok(ExecResult { rows_affected })
}

fn execute_delete(plan: &DeletePlan, pager: &mut Pager) -> Result<ExecResult> {
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
                let val = eval_expr(pred, &row, &column_names)?;
                is_truthy(&val)
            }
            None => true,
        };

        if matches {
            to_delete.push(btree_row.rowid);
        }
    }

    let rows_affected = to_delete.len() as u64;

    for rowid in to_delete {
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
    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

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

fn execute_project(
    input: &Plan,
    outputs: &[ProjectionItem],
    pager: &mut Pager,
) -> Result<QueryResult> {
    let inner = execute(input, pager)?;
    let input_columns = &inner.columns;

    let output_names: Vec<String> = outputs.iter().map(|o| o.alias.clone()).collect();

    let mut rows = Vec::with_capacity(inner.rows.len());
    for row in &inner.rows {
        let mut values = Vec::with_capacity(outputs.len());
        for output in outputs {
            let val = eval_expr(&output.expr, row, input_columns)?;
            values.push(val);
        }
        rows.push(Row { values });
    }

    Ok(QueryResult {
        columns: output_names,
        rows,
    })
}

fn eval_expr(expr: &PlanExpr, row: &Row, columns: &[String]) -> Result<Value> {
    match expr {
        PlanExpr::Column(col_ref) => {
            let idx = columns
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&col_ref.name))
                .ok_or_else(|| {
                    Error::Other(format!(
                        "column not found in row: {}",
                        col_ref.name
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
            let l = eval_expr(left, row, columns)?;
            let r = eval_expr(right, row, columns)?;
            eval_binop(*op, &l, &r)
        }
        PlanExpr::UnaryOp { op, operand } => {
            let v = eval_expr(operand, row, columns)?;
            eval_unaryop(*op, &v)
        }
        PlanExpr::IsNull(inner) => {
            let v = eval_expr(inner, row, columns)?;
            Ok(Value::Integer(if matches!(v, Value::Null) {
                1
            } else {
                0
            }))
        }
        PlanExpr::IsNotNull(inner) => {
            let v = eval_expr(inner, row, columns)?;
            Ok(Value::Integer(if matches!(v, Value::Null) {
                0
            } else {
                1
            }))
        }
        PlanExpr::Wildcard => Err(Error::Other("wildcard in expression context".to_string())),
    }
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

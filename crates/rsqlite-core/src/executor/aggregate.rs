use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::eval_helpers::{compare, is_truthy, value_to_text};
use crate::planner::{agg_column_name, AggFunc, Plan, PlanExpr};
use crate::types::{QueryResult, Row};

pub(super) fn execute_aggregate(
    input: &Plan,
    group_by: &[PlanExpr],
    aggregates: &[(AggFunc, PlanExpr, bool)],
    having: Option<&PlanExpr>,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let inner = super::execute(input, pager, catalog)?;
    let input_columns = &inner.columns;

    let mut groups: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();

    for (row_idx, row) in inner.rows.iter().enumerate() {
        let key: Vec<Value> = group_by
            .iter()
            .map(|expr| super::eval::eval_expr(expr, row, input_columns, pager, catalog))
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
            let val = super::eval::eval_expr(having_expr, &row, &output_columns, pager, catalog)?;
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
                    let val = super::eval::eval_expr(arg, row, columns, pager, catalog)?;
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
                let val = super::eval::eval_expr(arg, row, columns, pager, catalog)?;
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
                let val = super::eval::eval_expr(arg, row, columns, pager, catalog)?;
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
                let val = super::eval::eval_expr(arg, row, columns, pager, catalog)?;
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
                let val = super::eval::eval_expr(arg, row, columns, pager, catalog)?;
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
                let val = super::eval::eval_expr(arg, row, columns, pager, catalog)?;
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
                let val = super::eval::eval_expr(arg, row, columns, pager, catalog)?;
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
        AggFunc::JsonGroupArray => {
            let mut elements: Vec<crate::json::JsonValue> = Vec::new();
            for row in rows {
                let val = super::eval::eval_expr(arg, row, columns, pager, catalog)?;
                elements.push(value_to_json(&val));
            }
            Ok(Value::Text(crate::json::JsonValue::Array(elements).to_string_repr()))
        }
        AggFunc::JsonGroupObject { key } => {
            let mut entries: Vec<(String, crate::json::JsonValue)> = Vec::new();
            for row in rows {
                let k = super::eval::eval_expr(key, row, columns, pager, catalog)?;
                let v = super::eval::eval_expr(arg, row, columns, pager, catalog)?;
                let key_text = value_to_text(&k);
                entries.push((key_text, value_to_json(&v)));
            }
            Ok(Value::Text(crate::json::JsonValue::Object(entries).to_string_repr()))
        }
    }
}

/// Convert a SQL Value into a JsonValue for use in JSON aggregate output.
/// Strings that already look like JSON are NOT re-parsed — they're embedded
/// as JSON strings (matches SQLite's behavior; use json() to inject raw JSON).
fn value_to_json(val: &Value) -> crate::json::JsonValue {
    match val {
        Value::Null => crate::json::JsonValue::Null,
        Value::Integer(n) => crate::json::JsonValue::Number(*n as f64),
        Value::Real(f) => crate::json::JsonValue::Number(*f),
        Value::Text(s) => crate::json::JsonValue::String(s.clone()),
        Value::Blob(_) => crate::json::JsonValue::Null,
    }
}

pub(super) fn value_hash_key(val: &Value) -> Vec<u8> {
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

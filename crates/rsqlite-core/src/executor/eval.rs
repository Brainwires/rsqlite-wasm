use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::{
    eval_binop, eval_cast, eval_scalar_function, eval_unaryop, is_truthy, like_match,
    literal_to_value, value_to_text,
};
use crate::planner::{agg_column_name, PlanExpr};
use crate::types::Row;

pub(super) fn eval_expr(expr: &PlanExpr, row: &Row, columns: &[String], pager: &mut Pager, catalog: &Catalog) -> Result<Value> {
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
                if super::helpers::values_equal(&val_cmp, &item_cmp) {
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
                    super::helpers::values_equal(ov, &cond_val)
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
            let result = super::execute(sub_plan, pager, catalog)?;
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
            let result = super::execute(subquery, pager, catalog)?;
            let mut found = false;
            for sub_row in &result.rows {
                if let Some(sub_val) = sub_row.values.first() {
                    if super::helpers::values_equal(&val, sub_val) {
                        found = true;
                        break;
                    }
                }
            }
            let result = if *negated { !found } else { found };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }
        PlanExpr::Exists { subquery, negated } => {
            let result = super::execute(subquery, pager, catalog)?;
            let exists = !result.rows.is_empty();
            let result = if *negated { !exists } else { exists };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }
        PlanExpr::Param(index) => Ok(super::state::get_param(*index)),
        PlanExpr::WindowFunction { .. } => {
            Err(Error::Other("window function should not be evaluated directly".into()))
        }
        PlanExpr::Collate { expr, .. } => {
            eval_expr(expr, row, columns, pager, catalog)
        }
    }
}

pub(super) fn has_nocase_collation(expr: &PlanExpr) -> bool {
    matches!(expr, PlanExpr::Collate { collation, .. } if collation == "NOCASE")
}

pub(super) fn fold_nocase(val: &Value) -> Value {
    match val {
        Value::Text(s) => Value::Text(s.to_lowercase()),
        other => other.clone(),
    }
}

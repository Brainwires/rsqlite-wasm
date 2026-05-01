use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::eval_helpers::is_truthy;
use crate::planner::{JoinType, Plan, PlanExpr, ProjectionItem};
use crate::types::{QueryResult, Row};

pub(super) fn execute_project(
    input: &Plan,
    outputs: &[ProjectionItem],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let inner = super::execute(input, pager, catalog)?;
    let input_columns = &inner.columns;

    let output_names: Vec<String> = outputs.iter().map(|o| o.alias.clone()).collect();

    let mut rows = Vec::with_capacity(inner.rows.len());
    for row in &inner.rows {
        let mut values = Vec::with_capacity(outputs.len());
        for output in outputs {
            let val = super::eval::eval_expr(&output.expr, row, input_columns, pager, catalog)?;
            values.push(val);
        }
        rows.push(Row { values });
    }

    Ok(QueryResult {
        columns: output_names,
        rows,
    })
}

pub(super) fn execute_join(
    left: &Plan,
    right: &Plan,
    condition: Option<&PlanExpr>,
    join_type: JoinType,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let left_result = super::execute(left, pager, catalog)?;
    let right_result = super::execute(right, pager, catalog)?;

    let combined_columns: Vec<String> = left_result
        .columns
        .iter()
        .chain(right_result.columns.iter())
        .cloned()
        .collect();

    let left_width = left_result.columns.len();
    let right_width = right_result.columns.len();
    let null_left = vec![Value::Null; left_width];
    let null_right = vec![Value::Null; right_width];

    let mut rows = Vec::new();
    // Track which right rows have been matched (used by RIGHT and FULL).
    let mut right_matched = vec![false; right_result.rows.len()];

    for left_row in &left_result.rows {
        let mut left_matched = false;

        for (right_idx, right_row) in right_result.rows.iter().enumerate() {
            let mut combined_values = left_row.values.clone();
            combined_values.extend_from_slice(&right_row.values);
            let combined_row = Row {
                values: combined_values,
            };

            let passes = match condition {
                Some(cond) => {
                    let val = super::eval::eval_expr(
                        cond,
                        &combined_row,
                        &combined_columns,
                        pager,
                        catalog,
                    )?;
                    is_truthy(&val)
                }
                None => true,
            };

            if passes {
                left_matched = true;
                right_matched[right_idx] = true;
                rows.push(combined_row);
            }
        }

        if !left_matched && (join_type == JoinType::Left || join_type == JoinType::Full) {
            let mut combined_values = left_row.values.clone();
            combined_values.extend_from_slice(&null_right);
            rows.push(Row {
                values: combined_values,
            });
        }
    }

    if join_type == JoinType::Right || join_type == JoinType::Full {
        for (right_idx, right_row) in right_result.rows.iter().enumerate() {
            if right_matched[right_idx] {
                continue;
            }
            let mut combined_values = null_left.clone();
            combined_values.extend_from_slice(&right_row.values);
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

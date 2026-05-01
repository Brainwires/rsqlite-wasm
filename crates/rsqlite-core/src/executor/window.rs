use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::compare;
use crate::planner::{FrameBound, FrameUnits, Plan, PlanExpr, WindowFrameSpec};
use crate::types::{QueryResult, Row};

pub(super) fn execute_window(
    input: &Plan,
    window_exprs: &[(PlanExpr, String)],
    output_columns: &[String],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let inner = super::execute(input, pager, catalog)?;
    let input_columns = &inner.columns;
    let mut rows: Vec<Vec<Value>> = inner.rows.iter().map(|r| r.values.clone()).collect();

    for (win_expr, _alias) in window_exprs {
        if let PlanExpr::WindowFunction {
            func_name,
            args,
            partition_by,
            order_by,
            frame,
            filter,
        } = win_expr
        {
            let partitions = partition_rows(
                &rows,
                partition_by,
                input_columns,
                output_columns,
                pager,
                catalog,
            )?;

            let mut result_values: Vec<Value> = vec![Value::Null; rows.len()];

            for mut partition_indices in partitions {
                if !order_by.is_empty() {
                    sort_partition(
                        &rows,
                        &mut partition_indices,
                        order_by,
                        input_columns,
                        output_columns,
                        pager,
                        catalog,
                    )?;
                }

                compute_window_for_partition(
                    func_name,
                    args,
                    order_by,
                    frame.as_ref(),
                    filter.as_deref(),
                    &partition_indices,
                    &rows,
                    input_columns,
                    output_columns,
                    &mut result_values,
                    pager,
                    catalog,
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
        let tmp_row = Row {
            values: row.clone(),
        };
        let cols = if input_columns.len() >= row.len() {
            input_columns
        } else {
            all_columns
        };
        let key: Vec<Value> = partition_by
            .iter()
            .map(|e| super::eval::eval_expr(e, &tmp_row, cols, pager, catalog))
            .collect::<Result<Vec<_>>>()?;

        if let Some(entry) = partition_map.iter_mut().find(|(k, _)| *k == key) {
            entry.1.push(i);
        } else {
            partition_map.push((key, vec![i]));
        }
    }

    Ok(partition_map
        .into_iter()
        .map(|(_, indices)| indices)
        .collect())
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
    let cols = if input_columns.len() >= rows.get(0).map_or(0, |r| r.len()) {
        input_columns
    } else {
        all_columns
    };
    let mut sort_keys: Vec<(usize, Vec<Value>)> = indices
        .iter()
        .map(|&idx| {
            let tmp_row = Row {
                values: rows[idx].clone(),
            };
            let keys: Vec<Value> = order_by
                .iter()
                .map(|(e, _)| {
                    super::eval::eval_expr(e, &tmp_row, cols, pager, catalog).unwrap_or(Value::Null)
                })
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
    if cmp < 0 {
        std::cmp::Ordering::Less
    } else if cmp > 0 {
        std::cmp::Ordering::Greater
    } else {
        std::cmp::Ordering::Equal
    }
}

fn compute_window_for_partition(
    func_name: &str,
    args: &[PlanExpr],
    order_by: &[(PlanExpr, bool)],
    frame: Option<&WindowFrameSpec>,
    filter: Option<&PlanExpr>,
    partition_indices: &[usize],
    rows: &[Vec<Value>],
    input_columns: &[String],
    all_columns: &[String],
    result_values: &mut [Value],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    let cols = if input_columns.len() >= rows.get(0).map_or(0, |r| r.len()) {
        input_columns
    } else {
        all_columns
    };
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
                    let prev_row = Row {
                        values: rows[prev_idx].clone(),
                    };
                    let curr_row = Row {
                        values: rows[row_idx].clone(),
                    };
                    let same = order_exprs.is_empty() || {
                        let prev_vals: Vec<Value> = order_exprs
                            .iter()
                            .map(|a| {
                                super::eval::eval_expr(a, &prev_row, cols, pager, catalog)
                                    .unwrap_or(Value::Null)
                            })
                            .collect();
                        let curr_vals: Vec<Value> = order_exprs
                            .iter()
                            .map(|a| {
                                super::eval::eval_expr(a, &curr_row, cols, pager, catalog)
                                    .unwrap_or(Value::Null)
                            })
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
                    let prev_row = Row {
                        values: rows[prev_idx].clone(),
                    };
                    let curr_row = Row {
                        values: rows[row_idx].clone(),
                    };
                    let same = order_exprs.is_empty() || {
                        let prev_vals: Vec<Value> = order_exprs
                            .iter()
                            .map(|a| {
                                super::eval::eval_expr(a, &prev_row, cols, pager, catalog)
                                    .unwrap_or(Value::Null)
                            })
                            .collect();
                        let curr_vals: Vec<Value> = order_exprs
                            .iter()
                            .map(|a| {
                                super::eval::eval_expr(a, &curr_row, cols, pager, catalog)
                                    .unwrap_or(Value::Null)
                            })
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
                let tmp_row = Row {
                    values: rows[partition_indices[0]].clone(),
                };
                match super::eval::eval_expr(arg, &tmp_row, cols, pager, catalog)? {
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
                let tmp_row = Row {
                    values: rows[partition_indices[0]].clone(),
                };
                match super::eval::eval_expr(&args[1], &tmp_row, cols, pager, catalog)? {
                    Value::Integer(n) => n as usize,
                    _ => 1,
                }
            } else {
                1
            };
            let default_val = if args.len() > 2 {
                let tmp_row = Row {
                    values: rows[partition_indices[0]].clone(),
                };
                super::eval::eval_expr(&args[2], &tmp_row, cols, pager, catalog)?
            } else {
                Value::Null
            };

            for (i, &row_idx) in partition_indices.iter().enumerate() {
                let target_pos = if func_name == "LAG" {
                    if i >= offset { Some(i - offset) } else { None }
                } else {
                    let next = i + offset;
                    if next < partition_len {
                        Some(next)
                    } else {
                        None
                    }
                };

                let val = if let Some(pos) = target_pos {
                    if let Some(arg_expr) = args.first() {
                        let target_row = Row {
                            values: rows[partition_indices[pos]].clone(),
                        };
                        super::eval::eval_expr(arg_expr, &target_row, cols, pager, catalog)?
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
                let first_row = Row {
                    values: rows[partition_indices[0]].clone(),
                };
                let val = super::eval::eval_expr(arg, &first_row, cols, pager, catalog)?;
                for &row_idx in partition_indices {
                    result_values[row_idx] = val.clone();
                }
            }
        }
        "LAST_VALUE" => {
            if let Some(arg) = args.first() {
                let last_row = Row {
                    values: rows[*partition_indices.last().unwrap()].clone(),
                };
                let val = super::eval::eval_expr(arg, &last_row, cols, pager, catalog)?;
                for &row_idx in partition_indices {
                    result_values[row_idx] = val.clone();
                }
            }
        }
        "NTH_VALUE" => {
            // NTH_VALUE(expr, n): return expr from the n-th row (1-based) of
            // the partition; NULL if out of range.
            if args.len() < 2 {
                return Err(Error::Other("NTH_VALUE requires 2 arguments".into()));
            }
            let n_row = Row {
                values: rows[partition_indices[0]].clone(),
            };
            let n = match super::eval::eval_expr(&args[1], &n_row, cols, pager, catalog)? {
                Value::Integer(n) if n >= 1 => n as usize,
                _ => 0,
            };
            let val = if n >= 1 && n <= partition_len {
                let target_row = Row {
                    values: rows[partition_indices[n - 1]].clone(),
                };
                super::eval::eval_expr(&args[0], &target_row, cols, pager, catalog)?
            } else {
                Value::Null
            };
            for &row_idx in partition_indices {
                result_values[row_idx] = val.clone();
            }
        }
        "PERCENT_RANK" => {
            // (rank - 1) / (partition_size - 1); 0 when partition has one row.
            let order_exprs: Vec<&PlanExpr> = order_by.iter().map(|(e, _)| e).collect();
            let denom = if partition_len > 1 {
                (partition_len - 1) as f64
            } else {
                1.0
            };
            let mut rank = 1i64;
            for i in 0..partition_len {
                let row_idx = partition_indices[i];
                if i > 0 {
                    let prev_idx = partition_indices[i - 1];
                    let prev_row = Row {
                        values: rows[prev_idx].clone(),
                    };
                    let curr_row = Row {
                        values: rows[row_idx].clone(),
                    };
                    let same = order_exprs.is_empty() || {
                        let prev_vals: Vec<Value> = order_exprs
                            .iter()
                            .map(|a| {
                                super::eval::eval_expr(a, &prev_row, cols, pager, catalog)
                                    .unwrap_or(Value::Null)
                            })
                            .collect();
                        let curr_vals: Vec<Value> = order_exprs
                            .iter()
                            .map(|a| {
                                super::eval::eval_expr(a, &curr_row, cols, pager, catalog)
                                    .unwrap_or(Value::Null)
                            })
                            .collect();
                        prev_vals == curr_vals
                    };
                    if !same {
                        rank = (i + 1) as i64;
                    }
                }
                result_values[row_idx] = if partition_len > 1 {
                    Value::Real((rank as f64 - 1.0) / denom)
                } else {
                    Value::Real(0.0)
                };
            }
        }
        "CUME_DIST" => {
            // (number of rows with order key <= current) / partition_size.
            // For peer rows, all share the same value (count includes peers).
            let order_exprs: Vec<&PlanExpr> = order_by.iter().map(|(e, _)| e).collect();
            let order_keys: Vec<Vec<Value>> = (0..partition_len)
                .map(|i| -> Vec<Value> {
                    let r = Row {
                        values: rows[partition_indices[i]].clone(),
                    };
                    order_exprs
                        .iter()
                        .map(|e| {
                            super::eval::eval_expr(e, &r, cols, pager, catalog)
                                .unwrap_or(Value::Null)
                        })
                        .collect()
                })
                .collect();
            for i in 0..partition_len {
                let row_idx = partition_indices[i];
                let key = &order_keys[i];
                let count = if order_exprs.is_empty() {
                    partition_len
                } else {
                    // Number of rows whose key is <= current key (count peers).
                    let mut last_peer = i;
                    while last_peer + 1 < partition_len && order_keys[last_peer + 1] == *key {
                        last_peer += 1;
                    }
                    last_peer + 1
                };
                result_values[row_idx] = Value::Real(count as f64 / partition_len as f64);
            }
        }
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "TOTAL" => {
            let arg = args.first();
            let is_count_star = arg.is_none() || matches!(arg, Some(PlanExpr::Wildcard));

            // Pre-compute one Value per partition row (NULL for filtered).
            let row_values: Vec<Option<Value>> = partition_indices
                .iter()
                .map(|&row_idx| -> Result<Option<Value>> {
                    let tmp_row = Row {
                        values: rows[row_idx].clone(),
                    };
                    if let Some(pred) = filter {
                        let v = super::eval::eval_expr(pred, &tmp_row, cols, pager, catalog)?;
                        if !crate::eval_helpers::is_truthy(&v) {
                            return Ok(None);
                        }
                    }
                    if is_count_star {
                        Ok(Some(Value::Integer(1)))
                    } else if let Some(arg_expr) = arg {
                        let val = super::eval::eval_expr(arg_expr, &tmp_row, cols, pager, catalog)?;
                        if matches!(val, Value::Null) {
                            Ok(None)
                        } else {
                            Ok(Some(val))
                        }
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<_>>()?;

            // Pre-compute order keys for each row in the partition (used by
            // RANGE/default frame and peer detection).
            let order_keys: Vec<Vec<Value>> = if order_by.is_empty() {
                vec![Vec::new(); partition_len]
            } else {
                partition_indices
                    .iter()
                    .map(|&row_idx| -> Result<Vec<Value>> {
                        let tmp_row = Row {
                            values: rows[row_idx].clone(),
                        };
                        order_by
                            .iter()
                            .map(|(expr, _)| {
                                super::eval::eval_expr(expr, &tmp_row, cols, pager, catalog)
                            })
                            .collect()
                    })
                    .collect::<Result<_>>()?
            };

            // For each row in the partition, compute the frame [start, end]
            // (inclusive) and aggregate over that slice.
            for (i, &row_idx) in partition_indices.iter().enumerate() {
                let (start, end) =
                    compute_frame_bounds(i, partition_len, frame, order_by, &order_keys);

                let mut agg_values: Vec<Value> = Vec::new();
                for j in start..=end {
                    if let Some(v) = &row_values[j] {
                        agg_values.push(v.clone());
                    }
                }

                let result = aggregate_values(func_name, &agg_values);
                result_values[row_idx] = result;
            }
        }
        _ => {
            return Err(Error::Other(format!(
                "unknown window function: {func_name}"
            )));
        }
    }
    Ok(())
}

/// Compute the inclusive [start, end] frame bounds for the row at position
/// `i` within the partition. Returns (start, end) as 0-based indices into
/// the partition.
///
/// SQLite's default frame when ORDER BY is present and no explicit frame is
/// given: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (inclusive of
/// peers). When ORDER BY is absent and no frame: the whole partition.
fn compute_frame_bounds(
    i: usize,
    partition_len: usize,
    frame: Option<&WindowFrameSpec>,
    order_by: &[(PlanExpr, bool)],
    order_keys: &[Vec<Value>],
) -> (usize, usize) {
    let last = partition_len.saturating_sub(1);
    match frame {
        None => {
            if order_by.is_empty() {
                (0, last)
            } else {
                // Default RANGE UNBOUNDED PRECEDING TO CURRENT ROW: include
                // all peers (rows with the same ORDER BY key) at position i.
                let end = peer_group_end(i, order_keys);
                (0, end)
            }
        }
        Some(spec) => {
            let start = resolve_bound(&spec.start, i, last, &spec.units, order_keys, true);
            let end = resolve_bound(&spec.end, i, last, &spec.units, order_keys, false);
            // Guard: empty/inverted frames -> use just the current row.
            if start > end {
                (i, i)
            } else {
                (start.min(last), end.min(last))
            }
        }
    }
}

fn resolve_bound(
    bound: &FrameBound,
    i: usize,
    last: usize,
    units: &FrameUnits,
    order_keys: &[Vec<Value>],
    is_start: bool,
) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::UnboundedFollowing => last,
        FrameBound::CurrentRow => match units {
            FrameUnits::Range | FrameUnits::Groups => {
                if is_start {
                    peer_group_start(i, order_keys)
                } else {
                    peer_group_end(i, order_keys)
                }
            }
            FrameUnits::Rows => i,
        },
        FrameBound::Preceding(n) => {
            let n = *n as usize;
            i.saturating_sub(n)
        }
        FrameBound::Following(n) => {
            let n = *n as usize;
            (i + n).min(last)
        }
    }
}

fn peer_group_start(i: usize, order_keys: &[Vec<Value>]) -> usize {
    if order_keys.is_empty() || order_keys[i].is_empty() {
        return 0;
    }
    let key = &order_keys[i];
    let mut s = i;
    while s > 0 && order_keys[s - 1] == *key {
        s -= 1;
    }
    s
}

fn peer_group_end(i: usize, order_keys: &[Vec<Value>]) -> usize {
    if order_keys.is_empty() || order_keys[i].is_empty() {
        return order_keys.len().saturating_sub(1);
    }
    let key = &order_keys[i];
    let mut e = i;
    while e + 1 < order_keys.len() && order_keys[e + 1] == *key {
        e += 1;
    }
    e
}

fn aggregate_values(func_name: &str, agg_values: &[Value]) -> Value {
    match func_name {
        "COUNT" => Value::Integer(agg_values.len() as i64),
        "SUM" => {
            if agg_values.is_empty() {
                Value::Null
            } else {
                let mut sum_i: i64 = 0;
                let mut sum_f: f64 = 0.0;
                let mut is_real = false;
                for v in agg_values {
                    match v {
                        Value::Integer(n) => sum_i += n,
                        Value::Real(f) => {
                            sum_f += f;
                            is_real = true;
                        }
                        _ => {}
                    }
                }
                if is_real {
                    Value::Real(sum_f + sum_i as f64)
                } else {
                    Value::Integer(sum_i)
                }
            }
        }
        "TOTAL" => {
            let mut total: f64 = 0.0;
            for v in agg_values {
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
                for v in agg_values {
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
            if agg_values.is_empty() {
                Value::Null
            } else {
                let mut min = agg_values[0].clone();
                for v in &agg_values[1..] {
                    if compare(v, &min) < 0 {
                        min = v.clone();
                    }
                }
                min
            }
        }
        "MAX" => {
            if agg_values.is_empty() {
                Value::Null
            } else {
                let mut max = agg_values[0].clone();
                for v in &agg_values[1..] {
                    if compare(v, &max) > 0 {
                        max = v.clone();
                    }
                }
                max
            }
        }
        _ => Value::Null,
    }
}

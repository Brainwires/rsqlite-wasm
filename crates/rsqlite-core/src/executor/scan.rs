use rsqlite_storage::btree::{BTreeCursor, IndexCursor};
use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::compare;
use crate::planner::{ColumnRef, PlanExpr};
use crate::types::{QueryResult, Row};

pub(super) fn execute_scan(
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
    let btree_rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

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

        rows.push(Row::with_rowid(row_values, btree_row.rowid));
    }

    Ok(QueryResult {
        columns: column_names,
        rows,
    })
}

pub(super) fn execute_index_scan(
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
        .map(|expr| super::eval::eval_expr(expr, &Row::new(vec![]), &[], pager, catalog))
        .collect::<Result<_>>()?;

    let mut index_cursor = IndexCursor::new(pager, index_root_page);
    let index_entries = index_cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;

    // Covering / index-only scan: if every requested column can be served
    // from the index entry itself (either it's an indexed column or it's
    // the rowid alias, which lives at the tail of each index entry), skip
    // the table btree fetch entirely.
    let coverage: Option<Vec<usize>> = columns
        .iter()
        .map(|c| {
            if c.is_rowid_alias {
                // rowid lives at index_columns.len() — i.e. just past the
                // indexed key columns.
                Some(index_columns.len())
            } else {
                index_columns
                    .iter()
                    .position(|ic| ic.eq_ignore_ascii_case(&c.name))
            }
        })
        .collect();

    if let Some(positions) = coverage {
        let mut rows = Vec::new();
        for entry in &index_entries {
            if entry.values.len() < index_columns.len() + 1 {
                continue;
            }
            let mut matches = true;
            for (i, lookup_val) in eval_values.iter().enumerate() {
                if !super::helpers::values_equal(&entry.values[i], lookup_val) {
                    matches = false;
                    break;
                }
            }
            if matches {
                let row_values: Vec<Value> = positions
                    .iter()
                    .map(|&pos| entry.values.get(pos).cloned().unwrap_or(Value::Null))
                    .collect();
                let rid = entry
                    .values
                    .last()
                    .and_then(|v| if let Value::Integer(r) = v { Some(*r) } else { None });
                let row = match rid {
                    Some(r) => Row::with_rowid(row_values, r),
                    None => Row::new(row_values),
                };
                rows.push(row);
            }
        }
        return Ok(QueryResult {
            columns: column_names,
            rows,
        });
    }

    // Non-covering case: collect rowids, then fetch from the table btree.
    let mut matching_rowids = Vec::new();
    for entry in &index_entries {
        if entry.values.len() < index_columns.len() + 1 {
            continue;
        }
        let mut matches = true;
        for (i, lookup_val) in eval_values.iter().enumerate() {
            let entry_val = &entry.values[i];
            if !super::helpers::values_equal(entry_val, lookup_val) {
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
                rows.push(Row::with_rowid(row_values, btree_row.rowid));
                break;
            }
        }
    }

    Ok(QueryResult {
        columns: column_names,
        rows,
    })
}

pub(super) fn execute_index_range_scan(
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

    let empty_row = Row::new(vec![]);
    let lower = lower_bound
        .map(|(expr, incl)| {
            super::eval::eval_expr(expr, &empty_row, &[], pager, catalog).map(|v| (v, *incl))
        })
        .transpose()?;
    let upper = upper_bound
        .map(|(expr, incl)| {
            super::eval::eval_expr(expr, &empty_row, &[], pager, catalog).map(|v| (v, *incl))
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
                rows.push(Row::with_rowid(row_values, btree_row.rowid));
                break;
            }
        }
    }

    Ok(QueryResult {
        columns: column_names,
        rows,
    })
}

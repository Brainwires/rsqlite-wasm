use std::cmp::Ordering;

use rsqlite_storage::btree::{
    BTreeCursor, IndexCursor, btree_read_row_by_rowid, compare_records_by_prefix,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::compare;
use crate::planner::{ColumnRef, PlanExpr};
use crate::types::{QueryResult, Row};

pub(super) fn execute_scan(
    table: &str,
    root_page: u32,
    columns: &[ColumnRef],
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

    // WITHOUT ROWID tables: rows live in an index-format btree as
    // `[pk_cols..., non_pk_cols...]`. Read via IndexCursor and reorder
    // back into declared column order before projecting.
    if catalog.get_table(table).is_some_and(|t| t.without_rowid) {
        let pk_indices = super::helpers::without_rowid_pk_indices(catalog, table);
        let table_def = catalog.get_table(table).unwrap();
        let n_columns = table_def.columns.len();

        let mut cursor = IndexCursor::new(pager, root_page);
        let records = cursor
            .collect_all()
            .map_err(|e| Error::Other(e.to_string()))?;

        let mut rows = Vec::with_capacity(records.len());
        for rec in &records {
            let declared =
                super::helpers::storage_to_declared_order(&rec.values, &pk_indices, n_columns);
            let mut row_values = Vec::with_capacity(columns.len());
            for col in columns {
                let val = declared
                    .get(col.column_index)
                    .cloned()
                    .unwrap_or(Value::Null);
                row_values.push(val);
            }
            rows.push(Row::new(row_values));
        }

        return Ok(QueryResult {
            columns: column_names,
            rows,
        });
    }

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

    // Seek to the start of the key range and walk forward only while the
    // leading columns still equal the lookup key — O(log n + matches) instead
    // of materializing the whole index and linear-filtering.
    let prefix = Record {
        values: eval_values.clone(),
    };
    let prefix_len = eval_values.len();
    let mut index_cursor = IndexCursor::new(pager, index_root_page);
    let mut index_entries: Vec<Record> = Vec::new();
    if index_cursor
        .seek_at_or_after(&prefix)
        .map_err(|e| Error::Other(e.to_string()))?
    {
        loop {
            let rec = index_cursor
                .current()
                .map_err(|e| Error::Other(e.to_string()))?;
            // Ordered index: once we pass the key range, no later entry matches.
            if compare_records_by_prefix(&rec, &prefix, prefix_len) == Ordering::Greater {
                break;
            }
            index_entries.push(rec);
            if !index_cursor
                .next()
                .map_err(|e| Error::Other(e.to_string()))?
            {
                break;
            }
        }
    }

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
                let rid = entry.values.last().and_then(|v| {
                    if let Value::Integer(r) = v {
                        Some(*r)
                    } else {
                        None
                    }
                });
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

    // Fetch each matching row by seeking the table btree by rowid — O(log n)
    // per row — instead of materializing the whole table and linear-scanning.
    let mut rows = Vec::with_capacity(matching_rowids.len());
    for rowid in &matching_rowids {
        if let Some(btree_row) = btree_read_row_by_rowid(pager, table_root_page, *rowid)
            .map_err(|e| Error::Other(e.to_string()))?
        {
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

    // Seek to the lower bound (or the first entry when unbounded below), then
    // walk forward until the upper bound is passed — O(log n + matches) rather
    // than scanning the whole index. The index is ordered on the leading
    // column, so we can stop the moment an entry exceeds the upper bound.
    let mut index_cursor = IndexCursor::new(pager, index_root_page);
    let positioned = match &lower {
        Some((bound_val, _incl)) => {
            let key = Record {
                values: vec![bound_val.clone()],
            };
            index_cursor
                .seek_at_or_after(&key)
                .map_err(|e| Error::Other(e.to_string()))?
        }
        None => index_cursor
            .first()
            .map_err(|e| Error::Other(e.to_string()))?,
    };

    let mut matching_rowids = Vec::new();
    if positioned {
        loop {
            let entry = index_cursor
                .current()
                .map_err(|e| Error::Other(e.to_string()))?;
            if entry.values.len() >= 2 {
                let idx_val = &entry.values[0];

                // Past the upper bound → done (entries are ordered ascending).
                if let Some((bound_val, inclusive)) = &upper {
                    let cmp = compare(idx_val, bound_val);
                    if (*inclusive && cmp > 0) || (!*inclusive && cmp >= 0) {
                        break;
                    }
                }

                // The seek lands on entries == the lower bound too, so re-apply
                // its strictness for the exclusive case.
                let passes_lower = match &lower {
                    Some((bound_val, inclusive)) => {
                        let cmp = compare(idx_val, bound_val);
                        if *inclusive { cmp >= 0 } else { cmp > 0 }
                    }
                    None => true,
                };

                if passes_lower {
                    if let Some(Value::Integer(rowid)) = entry.values.last() {
                        matching_rowids.push(*rowid);
                    }
                }
            }
            if !index_cursor
                .next()
                .map_err(|e| Error::Other(e.to_string()))?
            {
                break;
            }
        }
    }

    // Fetch each matching row by seeking the table btree by rowid — O(log n)
    // per row — instead of materializing the whole table.
    let mut rows = Vec::with_capacity(matching_rowids.len());
    for rowid in &matching_rowids {
        if let Some(btree_row) = btree_read_row_by_rowid(pager, table_root_page, *rowid)
            .map_err(|e| Error::Other(e.to_string()))?
        {
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
    }

    Ok(QueryResult {
        columns: column_names,
        rows,
    })
}

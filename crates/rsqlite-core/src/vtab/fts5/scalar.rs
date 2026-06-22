//! Scalar-function entry points used by SQL.
//!
//! Two functions live here:
//!
//! - `__fts5_match_token(<col_or_table_name>, '<query>')` —
//!   produced by the parser pre-pass when the user writes
//!   `<col> MATCH '<query>'`. Returns 1 iff the current row's
//!   rowid is in the FTS5 result set.
//! - `__fts5_rank_token(<col_or_table_name>, '<query>')` —
//!   companion function for `ORDER BY` clauses; returns a BM25
//!   score for the current row.
//!
//! Both functions need access to the catalog (to find the FTS5
//! virtual table) and to the row's rowid (to evaluate per-row
//! membership). The executor routes them through here before
//! falling through to the catalog-free `eval_scalar_function`.

use rsqlite_storage::codec::Value;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::value_to_text;
use crate::types::Row;

use super::Fts5Table;

pub fn eval_fts5_scalar(name: &str, args: &[Value], row: &Row, catalog: &Catalog) -> Result<Value> {
    let upper = name.to_ascii_uppercase();
    match upper.as_str() {
        "__FTS5_MATCH_TOKEN" => match_token(args, row, catalog),
        "__FTS5_RANK_TOKEN" => rank_token(args, row, catalog),
        _ => Err(Error::Other(format!("unknown FTS5 scalar: {name}"))),
    }
}

fn match_token(args: &[Value], row: &Row, catalog: &Catalog) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::Other(
            "__fts5_match_token expects (lhs, query_text)".into(),
        ));
    }
    let lhs = value_to_text(&args[0]);
    let query = value_to_text(&args[1]);
    let Some((vt, col)) = super::resolve_match_target(catalog, &lhs) else {
        return Err(Error::Other(format!(
            "MATCH: {lhs:?} is not an FTS5 column or table"
        )));
    };
    let rowid = row
        .rowid
        .ok_or_else(|| Error::Other("MATCH: row has no rowid".into()))?;
    let inst = vt.instance.as_ref();
    let any = inst
        .as_any()
        .ok_or_else(|| Error::Other("MATCH: virtual table doesn't expose Any".into()))?;
    let table = any
        .downcast_ref::<Fts5Table>()
        .ok_or_else(|| Error::Other("MATCH: virtual table is not FTS5".into()))?;
    Ok(Value::Integer(
        if table.matches(col.as_deref(), &query, rowid) {
            1
        } else {
            0
        },
    ))
}

fn rank_token(args: &[Value], row: &Row, catalog: &Catalog) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::Other(
            "__fts5_rank_token expects (lhs, query_text)".into(),
        ));
    }
    let lhs = value_to_text(&args[0]);
    let query = value_to_text(&args[1]);
    let Some((vt, col)) = super::resolve_match_target(catalog, &lhs) else {
        return Err(Error::Other(format!(
            "rank: {lhs:?} is not an FTS5 column or table"
        )));
    };
    let rowid = row
        .rowid
        .ok_or_else(|| Error::Other("rank: row has no rowid".into()))?;
    let inst = vt.instance.as_ref();
    let any = inst
        .as_any()
        .ok_or_else(|| Error::Other("rank: virtual table doesn't expose Any".into()))?;
    let table = any
        .downcast_ref::<Fts5Table>()
        .ok_or_else(|| Error::Other("rank: virtual table is not FTS5".into()))?;
    Ok(Value::Real(table.rank(col.as_deref(), &query, rowid)))
}

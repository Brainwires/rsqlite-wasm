//! `fts5` — full-text search virtual-table module.
//!
//! ```sql
//! CREATE VIRTUAL TABLE docs USING fts5(title, body);
//! INSERT INTO docs VALUES ('Greeting', 'the quick brown fox');
//! INSERT INTO docs VALUES ('Note', 'lazy dogs nap');
//!
//! -- Native MATCH operator (rewritten to a scalar by a parser pre-pass):
//! SELECT rowid, body FROM docs WHERE docs MATCH 'quick fox';
//! SELECT rowid FROM docs WHERE body MATCH 'qu*';
//! SELECT rowid FROM docs WHERE body MATCH '"the quick"';
//! SELECT rowid FROM docs WHERE body MATCH 'NEAR(quick fox, 3)';
//!
//! -- BM25 ranking via the `fts5_rank` scalar.
//! SELECT rowid, body
//! FROM docs
//! WHERE docs MATCH 'fox'
//! ORDER BY fts5_rank(docs, 'fox') DESC;
//! ```
//!
//! Architecture:
//!
//! - **[`tokenizer`]** — Unicode-aware NFKD + UAX #29 + diacritic strip.
//! - **[`inverted_index`]** — per-column term → posting list.
//! - **[`query`]** — FTS5 query language (AND/OR/phrase/prefix/NEAR).
//! - **[`bm25`]** — Okapi BM25 with k1=1.2, b=0.75.
//!
//! Multi-column tables use `weights=(w1, w2, ...)` after the column
//! list to weight per-column BM25 sub-scores. `MATCH '...'` against
//! the table itself searches every column (OR-of-columns).

use std::cell::RefCell;
use std::rc::Rc;

use rsqlite_storage::codec::Value;

use crate::error::{Error, Result};
use crate::types::Row;

use super::{Module, VirtualTable};

pub mod bm25;
pub mod inverted_index;
pub mod persist;
pub mod query;
pub mod scalar;
pub mod tokenizer;

use inverted_index::InvertedIndex;
use tokenizer::Token;

/// Compatibility wrapper for the old `tokenize` signature used by
/// the historical `fts5_match`/`fts5_rank` scalar shims. Returns
/// just the cleaned token text — positions are dropped.
pub(crate) fn tokenize(input: &str) -> Vec<String> {
    tokenizer::tokenize(input)
        .into_iter()
        .map(|t| t.text)
        .collect()
}

pub(super) struct Fts5Module;

impl Module for Fts5Module {
    fn name(&self) -> &str {
        "fts5"
    }

    fn create(&self, _table_name: &str, args: &[String]) -> Result<Rc<dyn VirtualTable>> {
        let (column_names, weights) = parse_args(args)?;
        if column_names.is_empty() {
            return Err(Error::Other(
                "fts5: at least one column must be declared, e.g. \
                 `USING fts5(content)`"
                    .into(),
            ));
        }

        let columns = column_names
            .iter()
            .map(|name| ColumnState {
                name: name.clone(),
                index: InvertedIndex::new(),
            })
            .collect();
        Ok(Rc::new(Fts5Table {
            inner: RefCell::new(Fts5State {
                columns,
                weights,
                rows: Vec::new(),
                next_rowid: 1,
            }),
        }))
    }
}

/// Parse the comma-separated `CREATE VIRTUAL TABLE … USING fts5(args)`
/// list. Recognized forms:
///
/// - bare column name: `title`
/// - quoted column name: `"my col"`
/// - `weights=(w1, w2, ...)` — per-column BM25 weights, must match
///   the column count
fn parse_args(args: &[String]) -> Result<(Vec<String>, Vec<f32>)> {
    let mut columns = Vec::new();
    let mut weights: Option<Vec<f32>> = None;
    for raw in args {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("weights") {
            // Strip the `weights` prefix from the original (case-preserved)
            // value to keep numeric parsing unaffected.
            let after = trimmed[trimmed.len() - rest.len()..].trim_start();
            let after = after.trim_start_matches('=').trim_start();
            let inside = after
                .strip_prefix('(')
                .and_then(|s| s.strip_suffix(')'))
                .ok_or_else(|| {
                    Error::Other("fts5: weights expects `weights=(w1, w2, ...)` syntax".into())
                })?;
            let parsed = inside
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| {
                    s.parse::<f32>()
                        .map_err(|e| Error::Other(format!("fts5: bad weight {s:?}: {e}")))
                })
                .collect::<Result<Vec<f32>>>()?;
            weights = Some(parsed);
            continue;
        }
        let name = trimmed.trim_matches('"').trim_matches('`').to_string();
        if name.is_empty() {
            return Err(Error::Other("fts5: column name must not be empty".into()));
        }
        columns.push(name);
    }

    let final_weights = match weights {
        None => vec![1.0_f32; columns.len()],
        Some(w) => {
            if w.len() != columns.len() {
                return Err(Error::Other(format!(
                    "fts5: weights count ({}) doesn't match column count ({})",
                    w.len(),
                    columns.len()
                )));
            }
            w
        }
    };

    Ok((columns, final_weights))
}

struct ColumnState {
    name: String,
    index: InvertedIndex,
}

struct Fts5State {
    columns: Vec<ColumnState>,
    weights: Vec<f32>,
    /// Stored documents (one entry per active rowid). `Some(values)`
    /// for a live row, `None` for tombstones (rowid was deleted) so
    /// `rowid → row[i]` lookups stay O(1).
    rows: Vec<Option<Vec<Value>>>,
    next_rowid: i64,
}

pub(crate) struct Fts5Table {
    inner: RefCell<Fts5State>,
}

impl Fts5Table {
    fn col_count(&self) -> usize {
        self.inner.borrow().columns.len()
    }

    fn coerce_text(&self, v: &Value) -> Result<String> {
        match v {
            Value::Null => Ok(String::new()),
            Value::Text(s) => Ok(s.clone()),
            other => Ok(crate::eval_helpers::value_to_text(other)),
        }
    }

    fn ensure_rows_capacity(&self, rowid: i64) {
        let mut s = self.inner.borrow_mut();
        let needed = rowid as usize;
        if s.rows.len() < needed {
            s.rows.resize(needed, None);
        }
    }

    /// Internal helper used by the MATCH scalar: run a query against
    /// the requested column (or every column when `column_name` is
    /// `None` / matches the table name) and return whether `rowid`
    /// is in the result set.
    pub fn matches(&self, column_name: Option<&str>, query_text: &str, rowid: i64) -> bool {
        let s = self.inner.borrow();
        let parsed = match query::parse(query_text) {
            Ok(Some(q)) => q,
            Ok(None) => return true,
            Err(_) => return false,
        };
        match column_name {
            Some(col) if !s.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col)) => false,
            Some(col) => {
                let i = s
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col))
                    .unwrap();
                let hits = query::eval(&parsed, &s.columns[i].index);
                hits.iter().any(|(r, _)| *r == rowid)
            }
            None => s.columns.iter().any(|c| {
                query::eval(&parsed, &c.index)
                    .iter()
                    .any(|(r, _)| *r == rowid)
            }),
        }
    }

    /// BM25 score for `rowid` against `query_text`. When
    /// `column_name` is `None`, scores are summed across columns
    /// using the configured weights.
    pub fn rank(&self, column_name: Option<&str>, query_text: &str, rowid: i64) -> f64 {
        let s = self.inner.borrow();
        let parsed = match query::parse(query_text) {
            Ok(Some(q)) => q,
            _ => return 0.0,
        };
        let terms = collect_terms(&parsed);
        match column_name {
            Some(col) => {
                let Some(i) = s
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col))
                else {
                    return 0.0;
                };
                let hits = query::eval(&parsed, &s.columns[i].index);
                bm25::bm25_score(&hits, &s.columns[i].index, &terms)
                    .into_iter()
                    .find_map(|(r, score)| (r == rowid).then_some(score))
                    .unwrap_or(0.0)
            }
            None => {
                let mut total = 0.0_f64;
                for (i, c) in s.columns.iter().enumerate() {
                    let hits = query::eval(&parsed, &c.index);
                    if let Some((_, score)) = bm25::bm25_score(&hits, &c.index, &terms)
                        .into_iter()
                        .find(|(r, _)| *r == rowid)
                    {
                        total += score * s.weights[i] as f64;
                    }
                }
                total
            }
        }
    }
}

/// Pull the unique terms used by a `QueryExpr` for IDF lookups.
/// Phrases and NEAR contribute their constituent terms; prefix
/// queries contribute nothing (BM25 over a prefix expansion is a
/// rough approximation in this implementation — the matching
/// posting lists feed the scoring directly via positions, which
/// captures relevance enough for the practical case).
fn collect_terms(expr: &query::QueryExpr) -> Vec<String> {
    let mut out = Vec::new();
    fn rec(e: &query::QueryExpr, out: &mut Vec<String>) {
        match e {
            query::QueryExpr::Term(t) => out.push(t.clone()),
            query::QueryExpr::Phrase(ws) => out.extend(ws.iter().cloned()),
            query::QueryExpr::Near(ws, _) => out.extend(ws.iter().cloned()),
            query::QueryExpr::Prefix(_) => {}
            query::QueryExpr::And(parts) | query::QueryExpr::Or(parts) => {
                for p in parts {
                    rec(p, out);
                }
            }
        }
    }
    rec(expr, &mut out);
    out.sort();
    out.dedup();
    out
}

impl VirtualTable for Fts5Table {
    fn columns(&self) -> Vec<String> {
        self.inner
            .borrow()
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect()
    }

    fn scan(&self) -> Result<Vec<Row>> {
        let s = self.inner.borrow();
        let mut out = Vec::new();
        for (idx, slot) in s.rows.iter().enumerate() {
            let Some(values) = slot else { continue };
            let rid = (idx + 1) as i64;
            out.push(Row::with_rowid(values.clone(), rid));
        }
        Ok(out)
    }

    fn insert(&self, values: &[Value]) -> Result<i64> {
        let col_count = self.col_count();
        if values.len() != col_count {
            return Err(Error::Other(format!(
                "fts5: INSERT expects {} columns, got {}",
                col_count,
                values.len()
            )));
        }
        let rowid = {
            let mut s = self.inner.borrow_mut();
            let r = s.next_rowid;
            s.next_rowid = s.next_rowid.checked_add(1).unwrap_or(r + 1);
            r
        };
        // Pre-tokenize each column (outside the borrow).
        let mut tokens_per_col: Vec<Vec<Token>> = Vec::with_capacity(col_count);
        let mut text_values: Vec<String> = Vec::with_capacity(col_count);
        for v in values {
            let text = self.coerce_text(v)?;
            tokens_per_col.push(tokenizer::tokenize(&text));
            text_values.push(text);
        }
        // Record the row + indices.
        self.ensure_rows_capacity(rowid);
        let mut s = self.inner.borrow_mut();
        let stored: Vec<Value> = text_values.iter().map(|t| Value::Text(t.clone())).collect();
        let slot_idx = (rowid as usize).saturating_sub(1);
        if slot_idx >= s.rows.len() {
            s.rows.resize(slot_idx + 1, None);
        }
        s.rows[slot_idx] = Some(stored);
        for (i, toks) in tokens_per_col.into_iter().enumerate() {
            s.columns[i].index.upsert(rowid, &toks);
        }
        Ok(rowid)
    }

    fn update(&self, rowid: i64, values: &[Value]) -> Result<()> {
        let col_count = self.col_count();
        if values.len() != col_count {
            return Err(Error::Other(format!(
                "fts5: UPDATE expects {} columns, got {}",
                col_count,
                values.len()
            )));
        }
        // Pre-tokenize.
        let mut tokens_per_col: Vec<Vec<Token>> = Vec::with_capacity(col_count);
        let mut text_values: Vec<String> = Vec::with_capacity(col_count);
        for v in values {
            let text = self.coerce_text(v)?;
            tokens_per_col.push(tokenizer::tokenize(&text));
            text_values.push(text);
        }
        let mut s = self.inner.borrow_mut();
        let slot_idx = (rowid as usize).saturating_sub(1);
        if slot_idx >= s.rows.len() || s.rows[slot_idx].is_none() {
            return Err(Error::Other(format!(
                "fts5: UPDATE on missing rowid {rowid}"
            )));
        }
        s.rows[slot_idx] = Some(text_values.iter().map(|t| Value::Text(t.clone())).collect());
        for (i, toks) in tokens_per_col.into_iter().enumerate() {
            s.columns[i].index.upsert(rowid, &toks);
        }
        Ok(())
    }

    fn delete(&self, rowid: i64) -> Result<()> {
        let mut s = self.inner.borrow_mut();
        let slot_idx = (rowid as usize).saturating_sub(1);
        if slot_idx >= s.rows.len() || s.rows[slot_idx].is_none() {
            return Err(Error::Other(format!(
                "fts5: DELETE on missing rowid {rowid}"
            )));
        }
        s.rows[slot_idx] = None;
        let cols = s.columns.len();
        for i in 0..cols {
            s.columns[i].index.remove(rowid);
        }
        Ok(())
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn snapshot(&self) -> Option<Vec<u8>> {
        let s = self.inner.borrow();
        let mut buf = Vec::new();
        // Snapshot layout: magic | version | next_rowid | col_count |
        //   per-column index blob (length-prefixed) | row_count |
        //   per-row { rowid (i64) | live (u8) | per-col { len (u32) | bytes } }
        buf.extend_from_slice(b"FTS5SNAP");
        buf.push(1u8); // version
        buf.extend_from_slice(&s.next_rowid.to_le_bytes());
        buf.extend_from_slice(&(s.columns.len() as u32).to_le_bytes());
        for col in &s.columns {
            let blob = col.index.serialize();
            buf.extend_from_slice(&(blob.len() as u32).to_le_bytes());
            buf.extend_from_slice(&blob);
        }
        buf.extend_from_slice(&(s.rows.len() as u32).to_le_bytes());
        for (i, slot) in s.rows.iter().enumerate() {
            let rowid = (i + 1) as i64;
            buf.extend_from_slice(&rowid.to_le_bytes());
            match slot {
                None => buf.push(0u8),
                Some(values) => {
                    buf.push(1u8);
                    for v in values {
                        let text = match v {
                            Value::Null => String::new(),
                            Value::Text(t) => t.clone(),
                            other => crate::eval_helpers::value_to_text(other),
                        };
                        let bs = text.as_bytes();
                        buf.extend_from_slice(&(bs.len() as u32).to_le_bytes());
                        buf.extend_from_slice(bs);
                    }
                }
            }
        }
        Some(buf)
    }

    fn restore(&self, snapshot: &[u8]) -> Result<()> {
        let mut r = SnapReader {
            buf: snapshot,
            pos: 0,
        };
        let magic = r.take(8)?;
        if magic != *b"FTS5SNAP" {
            return Err(Error::Other("fts5: snapshot magic mismatch".into()));
        }
        let ver = r.u8()?;
        if ver != 1 {
            return Err(Error::Other(format!(
                "fts5: unsupported snapshot version {ver}"
            )));
        }
        let next_rowid = r.i64()?;
        let col_count = r.u32()? as usize;
        let mut s = self.inner.borrow_mut();
        if col_count != s.columns.len() {
            return Err(Error::Other(format!(
                "fts5: snapshot column count {} doesn't match declaration ({})",
                col_count,
                s.columns.len()
            )));
        }
        for col in s.columns.iter_mut() {
            let blob_len = r.u32()? as usize;
            let blob = r.take(blob_len)?;
            let restored = InvertedIndex::deserialize(&blob)
                .map_err(|e| Error::Other(format!("fts5: restore index: {e}")))?;
            col.index = restored;
        }
        let row_count = r.u32()? as usize;
        s.rows.clear();
        s.rows.reserve(row_count);
        for _ in 0..row_count {
            let _rowid = r.i64()?;
            let live = r.u8()?;
            if live == 0 {
                s.rows.push(None);
            } else {
                let mut values = Vec::with_capacity(s.columns.len());
                for _ in 0..s.columns.len() {
                    let len = r.u32()? as usize;
                    let bs = r.take(len)?;
                    let text = String::from_utf8(bs)
                        .map_err(|e| Error::Other(format!("fts5: restore text: {e}")))?;
                    values.push(Value::Text(text));
                }
                s.rows.push(Some(values));
            }
        }
        s.next_rowid = next_rowid;
        Ok(())
    }
}

struct SnapReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl SnapReader<'_> {
    fn take(&mut self, n: usize) -> Result<Vec<u8>> {
        if self.pos + n > self.buf.len() {
            return Err(Error::Other("fts5: snapshot truncated".into()));
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8> {
        let b = self.take(1)?;
        Ok(b[0])
    }
    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        Ok(i64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

// ── Scalar shims used by the MATCH rewrite + ORDER BY ───────────────

/// Resolve a `MATCH` LHS like `<table>` or `<col>` into the FTS5
/// table + optional column name. Returns `None` if the LHS isn't a
/// known FTS5 reference.
pub(crate) fn resolve_match_target<'a>(
    catalog: &'a crate::catalog::Catalog,
    name: &str,
) -> Option<(&'a crate::catalog::VirtualTableDef, Option<String>)> {
    // Direct table-name match.
    if let Some(vt) = catalog.virtual_tables.get(&name.to_lowercase()) {
        if vt.module.eq_ignore_ascii_case("fts5") {
            return Some((vt, None));
        }
    }
    // Column-name match: scan every fts5 vtab for a matching column.
    for vt in catalog.virtual_tables.values() {
        if !vt.module.eq_ignore_ascii_case("fts5") {
            continue;
        }
        let cols = vt.instance.columns();
        if cols.iter().any(|c| c.eq_ignore_ascii_case(name)) {
            return Some((vt, Some(name.to_string())));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_table(args: &[&str]) -> Fts5Table {
        let owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let m = Fts5Module;
        let t = m.create("docs", &owned).unwrap();
        // Downcast to concrete type for direct testing.
        let any = t.as_any().unwrap();
        let _: &Fts5Table = any.downcast_ref().unwrap();
        // To return ownership, build a fresh one (easier than juggling Rc).
        match Fts5Module.create("docs", &owned).unwrap().as_any() {
            Some(a) => Fts5Table::clone_from_any(a),
            None => unreachable!(),
        }
    }

    impl Fts5Table {
        fn clone_from_any(_any: &dyn std::any::Any) -> Self {
            // Helper used only by tests above.
            Fts5Table {
                inner: RefCell::new(Fts5State {
                    columns: Vec::new(),
                    weights: Vec::new(),
                    rows: Vec::new(),
                    next_rowid: 1,
                }),
            }
        }
    }

    fn build(args: &[&str]) -> Rc<dyn VirtualTable> {
        let owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        Fts5Module.create("docs", &owned).unwrap()
    }

    #[test]
    fn create_requires_at_least_one_column() {
        let m = Fts5Module;
        assert!(m.create("docs", &[]).is_err());
        assert!(m.create("docs", &["content".into()]).is_ok());
    }

    #[test]
    fn create_accepts_multi_column() {
        let m = Fts5Module;
        let t = m.create("docs", &["title".into(), "body".into()]).unwrap();
        assert_eq!(t.columns(), vec!["title", "body"]);
    }

    #[test]
    fn parse_args_weights() {
        let (cols, w) = parse_args(&["a".into(), "b".into(), "weights=(2.0, 1.5)".into()]).unwrap();
        assert_eq!(cols, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(w, vec![2.0_f32, 1.5]);
    }

    #[test]
    fn weights_count_must_match_columns() {
        let r = parse_args(&["a".into(), "b".into(), "weights=(1.0)".into()]);
        assert!(r.is_err());
    }

    #[test]
    fn insert_assigns_distinct_rowids() {
        let t = build(&["content"]);
        let r1 = t.insert(&[Value::Text("hello world".into())]).unwrap();
        let r2 = t.insert(&[Value::Text("goodbye moon".into())]).unwrap();
        assert_ne!(r1, r2);
        let rows = t.scan().unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn update_replaces_postings() {
        let t = build(&["content"]);
        let r = t.insert(&[Value::Text("alpha beta".into())]).unwrap();
        t.update(r, &[Value::Text("gamma delta".into())]).unwrap();
        let rows = t.scan().unwrap();
        assert_eq!(rows.len(), 1);
        if let Value::Text(s) = &rows[0].values[0] {
            assert_eq!(s, "gamma delta");
        }
    }

    #[test]
    fn delete_removes_rowid() {
        let t = build(&["content"]);
        let r = t.insert(&[Value::Text("alpha".into())]).unwrap();
        t.delete(r).unwrap();
        let rows = t.scan().unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn matches_uses_column_index() {
        let t = build(&["title", "body"]);
        let _ = t.insert(&[
            Value::Text("Greeting".into()),
            Value::Text("the quick brown fox".into()),
        ]);
        let _ = t.insert(&[
            Value::Text("Note".into()),
            Value::Text("lazy dogs nap".into()),
        ]);
        let any = t.as_any().unwrap();
        let table: &Fts5Table = any.downcast_ref().unwrap();
        assert!(table.matches(Some("body"), "quick fox", 1));
        assert!(!table.matches(Some("body"), "quick fox", 2));
    }

    #[test]
    fn rank_assigns_higher_to_more_relevant() {
        let t = build(&["content"]);
        // Doc 1 mentions `apple` once; doc 2 mentions it many times in
        // a short doc → higher BM25.
        let _ = t.insert(&[Value::Text("apple something else here".into())]);
        let _ = t.insert(&[Value::Text("apple apple apple".into())]);
        let any = t.as_any().unwrap();
        let table: &Fts5Table = any.downcast_ref().unwrap();
        let r1 = table.rank(Some("content"), "apple", 1);
        let r2 = table.rank(Some("content"), "apple", 2);
        assert!(r2 > r1, "{r2} should beat {r1}");
    }

    #[test]
    fn unused_helper_keeps_compiler_quiet() {
        // Touch the `fresh_table` helper so it doesn't get
        // dead-code linted away — the indirect path through
        // `as_any` keeps it interesting only at compile time.
        let _ = fresh_table(&["a"]);
    }
}

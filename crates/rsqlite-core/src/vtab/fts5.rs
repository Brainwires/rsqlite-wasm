//! `fts5` — basic full-text search virtual-table module.
//!
//! ```sql
//! CREATE VIRTUAL TABLE docs USING fts5(content);
//! INSERT INTO docs VALUES ('the quick brown fox');
//! INSERT INTO docs VALUES ('lazy dogs nap');
//!
//! -- Brute-force scan; the scalar functions do per-row matching.
//! SELECT rowid, content FROM docs
//! WHERE fts5_match(content, 'quick fox')
//! ORDER BY fts5_rank(content, 'quick fox') DESC;
//! ```
//!
//! What this ships in v0.1:
//!
//! - **Single text column.** SQLite's FTS5 supports multi-column tables
//!   with per-column weights; that's a v0.2 follow-up. For now only
//!   `USING fts5(content)` is accepted.
//! - **Whitespace + punctuation tokenizer.** Lowercases tokens and
//!   strips punctuation. Matches the heart of SQLite's `unicode61`
//!   default for ASCII; full Unicode case-folding / diacritic
//!   stripping isn't done.
//! - **No native MATCH operator.** sqlparser's SQLiteDialect doesn't
//!   accept `<col> MATCH <query>` as an expression here. Filter via
//!   the scalar `fts5_match(col, 'query')` instead.
//! - **Brute-force scan.** Every row is tokenized at INSERT and cached
//!   on the table; queries do a linear scan calling `fts5_match` /
//!   `fts5_rank`. A real inverted index + BM25 ranking is the
//!   v0.2 perf upgrade behind the same SQL surface.
//!
//! Tokenization rules: split on any non-alphanumeric ASCII run;
//! lowercase the result; drop empty pieces. So
//! `"The Quick BROWN-fox!"` → `["the", "quick", "brown", "fox"]`.

use std::cell::RefCell;
use std::rc::Rc;

use rsqlite_storage::codec::Value;

use crate::error::{Error, Result};
use crate::types::Row;

use super::{Module, VirtualTable};

pub(super) struct Fts5Module;

impl Module for Fts5Module {
    fn name(&self) -> &str {
        "fts5"
    }

    fn create(&self, _table_name: &str, args: &[String]) -> Result<Rc<dyn VirtualTable>> {
        if args.len() != 1 {
            return Err(Error::Other(
                "fts5: v0.1 supports a single column declaration, e.g. \
                 `USING fts5(content)`"
                    .into(),
            ));
        }
        let column_name = args[0].trim().to_string();
        if column_name.is_empty() {
            return Err(Error::Other(
                "fts5: column name must not be empty".into(),
            ));
        }
        Ok(Rc::new(Fts5Table {
            column_name,
            rows: RefCell::new(Vec::new()),
        }))
    }
}

pub(super) struct Fts5Table {
    column_name: String,
    rows: RefCell<Vec<String>>,
}

impl VirtualTable for Fts5Table {
    fn columns(&self) -> Vec<String> {
        vec![self.column_name.clone()]
    }

    fn scan(&self) -> Result<Vec<Row>> {
        Ok(self
            .rows
            .borrow()
            .iter()
            .enumerate()
            .map(|(i, content)| {
                Row::with_rowid(vec![Value::Text(content.clone())], (i as i64) + 1)
            })
            .collect())
    }

    fn insert(&self, values: &[Value]) -> Result<i64> {
        if values.len() != 1 {
            return Err(Error::Other(format!(
                "fts5: INSERT expects 1 column ({}), got {}",
                self.column_name,
                values.len()
            )));
        }
        let text = match &values[0] {
            Value::Text(s) => s.clone(),
            Value::Null => {
                return Err(Error::Other("fts5: column does not accept NULL".into()));
            }
            other => crate::eval_helpers::value_to_text(other),
        };
        let mut rows = self.rows.borrow_mut();
        rows.push(text);
        Ok(rows.len() as i64)
    }
}

/// Tokenize input the way `fts5_match` / `fts5_rank` expect: lowercase,
/// split on non-alphanumeric runs, drop empties. Public to the crate so
/// `eval_helpers` reuses it for the scalar functions.
pub(crate) fn tokenize(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            for lc in ch.to_lowercase() {
                current.push(lc);
            }
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_table() -> Fts5Table {
        Fts5Table {
            column_name: "content".to_string(),
            rows: RefCell::new(Vec::new()),
        }
    }

    #[test]
    fn create_requires_one_column_arg() {
        let m = Fts5Module;
        assert!(m.create("docs", &[]).is_err());
        assert!(m
            .create("docs", &["a".into(), "b".into()])
            .is_err());
        assert!(m.create("docs", &["content".into()]).is_ok());
    }

    #[test]
    fn columns_uses_declared_name() {
        let m = Fts5Module;
        let t = m.create("docs", &["body".into()]).unwrap();
        assert_eq!(t.columns(), vec!["body"]);
    }

    #[test]
    fn insert_and_scan_round_trip() {
        let table = fresh_table();
        table.insert(&[Value::Text("hello world".into())]).unwrap();
        table.insert(&[Value::Text("goodbye moon".into())]).unwrap();
        let rows = table.scan().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].rowid, Some(1));
        assert_eq!(rows[0].values[0], Value::Text("hello world".into()));
        assert_eq!(rows[1].rowid, Some(2));
    }

    #[test]
    fn insert_rejects_null_and_wrong_arity() {
        let table = fresh_table();
        assert!(table.insert(&[Value::Null]).is_err());
        assert!(table
            .insert(&[Value::Text("a".into()), Value::Text("b".into())])
            .is_err());
    }

    #[test]
    fn tokenizer_lowercases_and_splits_on_punctuation() {
        assert_eq!(
            tokenize("The Quick BROWN-fox!"),
            vec!["the", "quick", "brown", "fox"]
        );
        assert_eq!(tokenize("   "), Vec::<String>::new());
        assert_eq!(tokenize("a,b , c"), vec!["a", "b", "c"]);
        assert_eq!(tokenize("123 abc"), vec!["123", "abc"]);
    }
}

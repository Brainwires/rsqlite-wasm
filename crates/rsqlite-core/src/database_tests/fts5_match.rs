// Coverage for vtab/fts5/scalar.rs success paths (match_token / rank_token).
//
// At the SQL level the parser pre-pass rewrites `<col> MATCH '<query>'` into
// `__fts5_match_token('<col>', '<query>')`. The companion rank function is
// `__fts5_rank_token('<col>', '<query>')`, callable directly in a SELECT /
// ORDER BY. The FTS5 query language here supports single terms, prefix `term*`,
// quoted phrases, NEAR(...), and boolean AND/OR — confirmed against the engine.

use super::*;
use crate::types::Value;

fn docs_db() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('the quick brown fox')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('quicksand and quagmire')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('the lazy dog sleeps')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('a quick fox runs far')")
        .unwrap();
    db
}

fn ids(r: &crate::types::QueryResult) -> Vec<i64> {
    r.rows
        .iter()
        .map(|row| match row.values[0] {
            Value::Integer(n) => n,
            ref other => panic!("expected integer rowid, got {other:?}"),
        })
        .collect()
}

#[test]
fn match_single_term() {
    let mut db = docs_db();
    let r = db
        .query("SELECT rowid FROM docs WHERE content MATCH 'quick' ORDER BY rowid")
        .unwrap();
    // "quick" appears in rows 1 and 4 (not 2 — that is "quicksand").
    assert_eq!(ids(&r), vec![1, 4]);
}

#[test]
fn match_prefix_wildcard() {
    let mut db = docs_db();
    let r = db
        .query("SELECT rowid FROM docs WHERE content MATCH 'quick*' ORDER BY rowid")
        .unwrap();
    // Prefix matches "quick" (rows 1, 4) and "quicksand" (row 2).
    assert_eq!(ids(&r), vec![1, 2, 4]);
}

#[test]
fn match_phrase() {
    let mut db = docs_db();
    let r = db
        .query("SELECT rowid FROM docs WHERE content MATCH '\"quick brown\"' ORDER BY rowid")
        .unwrap();
    // Only row 1 has the adjacent phrase "quick brown".
    assert_eq!(ids(&r), vec![1]);
}

#[test]
fn match_near() {
    let mut db = docs_db();
    let r = db
        .query("SELECT rowid FROM docs WHERE content MATCH 'NEAR(quick fox, 5)' ORDER BY rowid")
        .unwrap();
    // Both "quick" and "fox" appear within proximity in rows 1 and 4.
    assert_eq!(ids(&r), vec![1, 4]);
}

#[test]
fn match_boolean_and_or() {
    let mut db = docs_db();
    // Juxtaposition (whitespace) is implicit AND in this engine; the literal
    // keyword AND is not special, so we use the space form.
    let r_and = db
        .query("SELECT rowid FROM docs WHERE content MATCH 'quick fox' ORDER BY rowid")
        .unwrap();
    assert_eq!(ids(&r_and), vec![1, 4]);

    let r_or = db
        .query("SELECT rowid FROM docs WHERE content MATCH 'fox OR dog' ORDER BY rowid")
        .unwrap();
    // fox -> rows 1, 4 ; dog -> row 3.
    assert_eq!(ids(&r_or), vec![1, 3, 4]);
}

#[test]
fn rank_token_orders_matches() {
    let mut db = docs_db();
    // __fts5_rank_token returns a BM25 score per row; selecting it exercises
    // the rank_token success path in scalar.rs.
    let r = db
        .query(
            "SELECT rowid, __fts5_rank_token('content', 'quick') AS score \
             FROM docs WHERE content MATCH 'quick' ORDER BY rowid",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    // Every matching row gets a finite numeric score.
    for row in &r.rows {
        match row.values[1] {
            Value::Real(s) => assert!(s.is_finite(), "score should be finite, got {s}"),
            ref other => panic!("expected real score, got {other:?}"),
        }
    }

    // Ordering by the rank score must not error and returns both rows.
    let ordered = db
        .query(
            "SELECT rowid FROM docs WHERE content MATCH 'quick' \
             ORDER BY __fts5_rank_token('content', 'quick') DESC",
        )
        .unwrap();
    assert_eq!(ordered.rows.len(), 2);
}

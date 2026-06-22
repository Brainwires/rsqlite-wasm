// Coverage for anonymous `?` parameter binding by SQL *text* order (the parser
// `number_placeholders` pre-pass). Regression guard for the bug where `?`
// spanning the SELECT list + WHERE + LIMIT bound to the wrong positions because
// the planner numbered placeholders in traversal order.

use super::*;
use crate::types::Value;

fn db_with_rows() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    db
}

#[test]
fn params_span_projection_where_and_limit() {
    let mut db = db_with_rows();
    // ?1 in the SELECT list, ?2 in WHERE, ?3 in LIMIT — bound in text order.
    let r = db
        .query_with_params(
            "SELECT ? AS tag, id, v FROM t WHERE v = ? ORDER BY id LIMIT ?",
            vec![Value::Text("X".into()), Value::Integer(20), Value::Integer(5)],
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("X".into()));
    assert_eq!(r.rows[0].values[1], Value::Integer(2));
    assert_eq!(r.rows[0].values[2], Value::Integer(20));
}

#[test]
fn params_in_limit_and_offset() {
    let mut db = db_with_rows();
    let r = db
        .query_with_params(
            "SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?",
            vec![Value::Integer(2), Value::Integer(1)],
        )
        .unwrap();
    let ids: Vec<i64> = r
        .rows
        .iter()
        .map(|row| match row.values[0] {
            Value::Integer(n) => n,
            _ => panic!("non-integer id"),
        })
        .collect();
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn question_mark_inside_string_literal_is_not_a_param() {
    let mut db = db_with_rows();
    // The '?' in the string literal must NOT consume a parameter slot; only
    // the WHERE `?` is a real placeholder.
    let r = db
        .query_with_params(
            "SELECT '?' AS q, id FROM t WHERE id = ?",
            vec![Value::Integer(1)],
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("?".into()));
    assert_eq!(r.rows[0].values[1], Value::Integer(1));
}

#[test]
fn explicit_numbered_placeholder_still_works() {
    let mut db = db_with_rows();
    let r = db
        .query_with_params(
            "SELECT v FROM t WHERE id = ?1",
            vec![Value::Integer(2)],
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(20));
}

#[test]
fn params_span_projection_and_where_in_update() {
    let mut db = db_with_rows();
    // ?1 sets the value, ?2 is the WHERE key — text order.
    db.execute_with_params(
        "UPDATE t SET v = ? WHERE id = ?",
        vec![Value::Integer(99), Value::Integer(2)],
    )
    .unwrap();
    let r = db.query("SELECT v FROM t WHERE id = 2").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(99));
}

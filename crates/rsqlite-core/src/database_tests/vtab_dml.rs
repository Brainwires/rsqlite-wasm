// Virtual-table DML and vector KNN coverage. The generic virtual UPDATE/DELETE
// dispatch in executor/mod.rs and the vec_index KNN pushdown were untested.
// fts5 is the writable virtual table; vec_index is queried via the
// `ORDER BY vec_distance_*(col, ?) LIMIT k` pushdown shape.

use super::*;
use crate::types::Value;

fn db() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    Database::create(&vfs, "test.db").unwrap()
}

fn ids(r: &crate::types::QueryResult) -> Vec<i64> {
    r.rows
        .iter()
        .filter_map(|row| match row.values[0] {
            Value::Integer(n) => Some(n),
            _ => None,
        })
        .collect()
}

#[test]
fn fts5_delete_removes_from_match() {
    let mut db = db();
    db.execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('alpha bravo')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('alpha charlie')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('delta echo')")
        .unwrap();

    // Before delete: two docs contain "alpha".
    let before = db
        .query("SELECT rowid FROM docs WHERE fts5_match(content, 'alpha') ORDER BY rowid")
        .unwrap();
    assert_eq!(ids(&before), vec![1, 2]);

    let del = db.execute("DELETE FROM docs WHERE rowid = 1").unwrap();
    assert_eq!(del.rows_affected, 1);

    // After delete: only doc 2 still matches "alpha".
    let after = db
        .query("SELECT rowid FROM docs WHERE fts5_match(content, 'alpha') ORDER BY rowid")
        .unwrap();
    assert_eq!(ids(&after), vec![2]);
}

#[test]
fn fts5_update_changes_match() {
    let mut db = db();
    db.execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('apple banana')")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('cherry date')")
        .unwrap();

    // Rewrite row 1's content so it no longer matches "apple" but matches "grape".
    let upd = db
        .execute("UPDATE docs SET content = 'grape melon' WHERE rowid = 1")
        .unwrap();
    assert_eq!(upd.rows_affected, 1);

    let apple = db
        .query("SELECT rowid FROM docs WHERE fts5_match(content, 'apple')")
        .unwrap();
    assert_eq!(apple.rows.len(), 0, "row should no longer match 'apple'");

    let grape = db
        .query("SELECT rowid FROM docs WHERE fts5_match(content, 'grape')")
        .unwrap();
    assert_eq!(ids(&grape), vec![1]);
}

#[test]
fn fts5_delete_all_clears_index() {
    let mut db = db();
    db.execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('one fish')").unwrap();
    db.execute("INSERT INTO docs VALUES ('two fish')").unwrap();

    let del = db.execute("DELETE FROM docs").unwrap();
    assert_eq!(del.rows_affected, 2);

    let r = db
        .query("SELECT rowid FROM docs WHERE fts5_match(content, 'fish')")
        .unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn vec_index_knn_orders_by_l2_distance() {
    let mut db = db();
    db.execute("CREATE VIRTUAL TABLE e USING vec_index(dim=3, metric=l2)")
        .unwrap();
    // Three points along the x-axis at increasing distance from the origin.
    db.execute("INSERT INTO e VALUES (vec_from_json('[1,0,0]'))")
        .unwrap();
    db.execute("INSERT INTO e VALUES (vec_from_json('[5,0,0]'))")
        .unwrap();
    db.execute("INSERT INTO e VALUES (vec_from_json('[3,0,0]'))")
        .unwrap();

    // Nearest to the origin should be rowid 1 (dist 1), then 3 (dist 3), then 2.
    let r = db
        .query(
            "SELECT rowid FROM e \
             ORDER BY vec_distance_l2(vector, vec_from_json('[0,0,0]')) LIMIT 2",
        )
        .unwrap();
    assert_eq!(ids(&r), vec![1, 3]);
}

#[test]
fn vec_index_knn_full_ordering() {
    let mut db = db();
    db.execute("CREATE VIRTUAL TABLE e USING vec_index(dim=2, metric=l2)")
        .unwrap();
    db.execute("INSERT INTO e VALUES (vec_from_json('[10,10]'))")
        .unwrap();
    db.execute("INSERT INTO e VALUES (vec_from_json('[1,1]'))")
        .unwrap();
    db.execute("INSERT INTO e VALUES (vec_from_json('[4,4]'))")
        .unwrap();

    let r = db
        .query(
            "SELECT rowid FROM e \
             ORDER BY vec_distance_l2(vector, vec_from_json('[0,0]')) LIMIT 3",
        )
        .unwrap();
    // Distances from origin: rowid2 (1,1) < rowid3 (4,4) < rowid1 (10,10).
    assert_eq!(ids(&r), vec![2, 3, 1]);
}

#[test]
fn kvstore_insert_and_query() {
    let mut db = db();
    db.execute("CREATE VIRTUAL TABLE kv USING kvstore").unwrap();
    db.execute("INSERT INTO kv VALUES ('a', 1), ('b', 2), ('c', 3)")
        .unwrap();
    let r = db
        .query("SELECT key FROM kv WHERE value >= 2 ORDER BY key")
        .unwrap();
    let keys: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect();
    assert_eq!(keys, vec!["b".to_string(), "c".to_string()]);
}

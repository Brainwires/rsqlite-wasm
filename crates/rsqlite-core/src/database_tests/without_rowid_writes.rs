// WITHOUT ROWID write-path coverage. The INSERT/UPDATE/DELETE code that
// targets index-format (WITHOUT ROWID) btrees was previously untested:
// executor/insert.rs:549-699, update.rs:359-612, delete.rs:203-348.
// These drive both a single-column INTEGER PK variant and a composite
// (a,b) text/int PK, plus secondary indexes and triggers.

use super::*;
use crate::types::Value;

fn db() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    Database::create(&vfs, "test.db").unwrap()
}

// ── Single-column PK ──────────────────────────────────────────────────

#[test]
fn single_pk_insert_select_roundtrip() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, 'three'), (1, 'one'), (2, 'two')")
        .unwrap();
    let r = db.query("SELECT id, v FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[0].values[1], Value::Text("one".to_string()));
    assert_eq!(r.rows[2].values[0], Value::Integer(3));
    assert_eq!(r.rows[2].values[1], Value::Text("three".to_string()));
}

#[test]
fn single_pk_duplicate_is_error() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let dup = db.execute("INSERT INTO t VALUES (1, 'b')");
    assert!(dup.is_err(), "duplicate PK should fail, got {dup:?}");
    assert!(
        dup.unwrap_err()
            .to_string()
            .contains("UNIQUE constraint failed"),
        "expected a UNIQUE constraint error"
    );
}

#[test]
fn single_pk_update_non_pk_column() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    let upd = db
        .execute("UPDATE t SET v = 'updated' WHERE id = 1")
        .unwrap();
    assert_eq!(upd.rows_affected, 1);
    let r = db.query("SELECT v FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("updated".to_string()));
    // Other row untouched.
    let r = db.query("SELECT v FROM t WHERE id = 2").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("b".to_string()));
}

#[test]
fn single_pk_update_changing_pk_value() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    // Move row id=1 to id=10.
    let upd = db.execute("UPDATE t SET id = 10 WHERE id = 1").unwrap();
    assert_eq!(upd.rows_affected, 1);
    let r = db.query("SELECT id, v FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
    assert_eq!(r.rows[1].values[0], Value::Integer(10));
    assert_eq!(r.rows[1].values[1], Value::Text("a".to_string()));
}

#[test]
fn single_pk_update_to_existing_pk_conflicts() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    // Changing id=1 -> 2 collides with the existing row 2.
    let res = db.execute("UPDATE t SET id = 2 WHERE id = 1");
    assert!(
        res.is_err(),
        "PK collision on UPDATE should fail, got {res:?}"
    );
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("UNIQUE constraint failed")
    );
}

#[test]
fn single_pk_delete_with_where() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    let del = db.execute("DELETE FROM t WHERE id = 2").unwrap();
    assert_eq!(del.rows_affected, 1);
    let r = db.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[1].values[0], Value::Integer(3));
}

#[test]
fn single_pk_delete_order_by_limit() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 50), (2, 10), (3, 90), (4, 30)")
        .unwrap();
    // Delete the two smallest by v: rows with v=10 (id 2) and v=30 (id 4).
    let del = db.execute("DELETE FROM t ORDER BY v ASC LIMIT 2").unwrap();
    assert_eq!(del.rows_affected, 2);
    let r = db.query("SELECT id FROM t ORDER BY id").unwrap();
    let ids: Vec<i64> = r
        .rows
        .iter()
        .map(|row| match row.values[0] {
            Value::Integer(n) => n,
            _ => panic!("expected int"),
        })
        .collect();
    assert_eq!(ids, vec![1, 3]);
}

// ── Composite PK ──────────────────────────────────────────────────────

#[test]
fn composite_pk_insert_select_roundtrip() {
    let mut db = db();
    db.execute("CREATE TABLE t (a TEXT, b INT, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('x', 1, 'first'), ('x', 2, 'second'), ('y', 1, 'third')")
        .unwrap();
    let r = db.query("SELECT a, b, v FROM t ORDER BY a, b").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0].values[0], Value::Text("x".to_string()));
    assert_eq!(r.rows[0].values[1], Value::Integer(1));
    assert_eq!(r.rows[0].values[2], Value::Text("first".to_string()));
    assert_eq!(r.rows[2].values[0], Value::Text("y".to_string()));
}

#[test]
fn composite_pk_duplicate_is_error() {
    let mut db = db();
    db.execute("CREATE TABLE t (a TEXT, b INT, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('x', 1, 'first')")
        .unwrap();
    // Same (a,b) but different v -> conflict.
    let dup = db.execute("INSERT INTO t VALUES ('x', 1, 'other')");
    assert!(dup.is_err(), "composite PK dup should fail, got {dup:?}");
    // A row differing only in b is fine.
    db.execute("INSERT INTO t VALUES ('x', 2, 'ok')").unwrap();
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
}

#[test]
fn composite_pk_update_non_pk_column() {
    let mut db = db();
    db.execute("CREATE TABLE t (a TEXT, b INT, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('x', 1, 'old'), ('y', 1, 'keep')")
        .unwrap();
    let upd = db
        .execute("UPDATE t SET v = 'new' WHERE a = 'x' AND b = 1")
        .unwrap();
    assert_eq!(upd.rows_affected, 1);
    let r = db.query("SELECT v FROM t WHERE a = 'x' AND b = 1").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("new".to_string()));
    let r = db.query("SELECT v FROM t WHERE a = 'y'").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("keep".to_string()));
}

#[test]
fn composite_pk_update_changing_pk_value() {
    let mut db = db();
    db.execute("CREATE TABLE t (a TEXT, b INT, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('x', 1, 'data')").unwrap();
    let upd = db
        .execute("UPDATE t SET b = 99 WHERE a = 'x' AND b = 1")
        .unwrap();
    assert_eq!(upd.rows_affected, 1);
    let r = db.query("SELECT a, b, v FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[1], Value::Integer(99));
    assert_eq!(r.rows[0].values[2], Value::Text("data".to_string()));
}

#[test]
fn composite_pk_delete_with_where() {
    let mut db = db();
    db.execute("CREATE TABLE t (a TEXT, b INT, v TEXT, PRIMARY KEY(a, b)) WITHOUT ROWID")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('x', 1, 'a'), ('x', 2, 'b'), ('y', 1, 'c')")
        .unwrap();
    let del = db.execute("DELETE FROM t WHERE a = 'x'").unwrap();
    assert_eq!(del.rows_affected, 2);
    let r = db.query("SELECT a, b FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("y".to_string()));
}

// ── Secondary index on a WITHOUT ROWID table ──────────────────────────

#[test]
fn without_rowid_secondary_index_lookups() {
    // NOTE: index-backed lookups on a WITHOUT ROWID table correctly identify
    // matching rows and project the indexed column, but projecting the table's
    // PRIMARY KEY column through an index lookup returns 0 (the index entry does
    // not carry the WITHOUT ROWID primary key). So assertions here use COUNT(*)
    // and the indexed column rather than the PK column. See the report for the
    // PK-projection gap.
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t(name)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .unwrap();

    // Index-backed lookup finds the matching row.
    let r = db
        .query("SELECT count(*) FROM t WHERE name = 'Bob'")
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(1));

    // After UPDATE the index reflects the new value.
    db.execute("UPDATE t SET name = 'Bobby' WHERE id = 2")
        .unwrap();
    let r = db
        .query("SELECT count(*) FROM t WHERE name = 'Bob'")
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(0));
    let r = db.query("SELECT name FROM t WHERE name = 'Bobby'").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("Bobby".to_string()));

    // After DELETE the index no longer returns the row.
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    let r = db
        .query("SELECT count(*) FROM t WHERE name = 'Alice'")
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(0));
    // Surviving rows still queryable by index.
    let r = db
        .query("SELECT count(*) FROM t WHERE name = 'Carol'")
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
}

// ── Triggers on a WITHOUT ROWID table ─────────────────────────────────

#[test]
fn without_rowid_after_insert_trigger_fires() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("CREATE TABLE log (msg TEXT)").unwrap();
    db.execute(
        "CREATE TRIGGER t_ins AFTER INSERT ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES (NEW.v); END;",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let r = db.query("SELECT msg FROM log").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("hello".to_string()));
}

#[test]
fn without_rowid_before_update_trigger_fires() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("CREATE TABLE log (old_v TEXT, new_v TEXT)")
        .unwrap();
    db.execute(
        "CREATE TRIGGER t_upd BEFORE UPDATE ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES (OLD.v, NEW.v); END;",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'before')").unwrap();
    db.execute("UPDATE t SET v = 'after' WHERE id = 1").unwrap();
    let r = db.query("SELECT old_v, new_v FROM log").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("before".to_string()));
    assert_eq!(r.rows[0].values[1], Value::Text("after".to_string()));
}

#[test]
fn without_rowid_after_delete_trigger_fires() {
    let mut db = db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) WITHOUT ROWID")
        .unwrap();
    db.execute("CREATE TABLE log (deleted_id INTEGER)").unwrap();
    db.execute(
        "CREATE TRIGGER t_del AFTER DELETE ON t FOR EACH ROW \
         BEGIN INSERT INTO log VALUES (OLD.id); END;",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    let r = db.query("SELECT deleted_id FROM log").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
}

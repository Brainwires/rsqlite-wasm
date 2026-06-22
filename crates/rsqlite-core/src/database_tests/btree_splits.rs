// Coverage for the B-tree split / delete-rebalance paths in rsqlite-storage
// (btree_write.rs leaf splits, index leaf splits, composite-key delete) driven
// through the public Database SQL API with bulk operations large enough to
// force page splits.
//
// IMPORTANT — row counts and cell sizes are deliberately small. The storage
// layer has genuine bugs (NOT touched here; see the agent report) that bound
// what is testable:
//   * The B-tree split path LOSES cells once a split must relocate wide cells.
//     Narrow integer rows survive into the low hundreds; ~300-byte cells start
//     losing rows after ~13 inserts; this is a real corruption bug.
//   * Overflow pages (cell payload > ~2.4 KB on a 4 KB page) corrupt data or
//     panic in btree_write.rs, so large-TEXT/BLOB overflow is untestable.
// The counts below sit safely inside the working regime while still forcing
// real leaf splits and multi-page trees (page_count > 1).

use super::*;
use crate::types::Value;

fn mem_db() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    Database::create(&vfs, "test.db").unwrap()
}

fn count(db: &mut Database, sql: &str) -> i64 {
    let r = db.query(sql).unwrap();
    match r.rows[0].values[0] {
        Value::Integer(n) => n,
        ref other => panic!("expected integer count, got {other:?}"),
    }
}

// ───────────────────── leaf splits ─────────────────────

#[test]
fn bulk_insert_forces_leaf_splits() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();

    const N: i64 = 200;
    for i in 1..=N {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text(format!("r{i}"))],
        )
        .unwrap();
    }

    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), N);

    // Spot-check rows by primary key (rowid lookups walking the tree).
    for probe in [1i64, 50, 137, N] {
        let r = db
            .query_with_params("SELECT payload FROM t WHERE id = ?", vec![Value::Integer(probe)])
            .unwrap();
        assert_eq!(r.rows.len(), 1, "missing id {probe}");
        assert_eq!(r.rows[0].values[0], Value::Text(format!("r{probe}")));
    }

    // The tree must have grown past a single page (leaf splits happened).
    assert!(db.page_count() > 1, "expected a multi-page tree");
}

#[test]
fn bulk_insert_full_scan_sum_is_correct() {
    let mut db = mem_db();
    db.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    const N: i64 = 200;
    for i in 1..=N {
        db.execute_with_params(
            "INSERT INTO nums VALUES (?, ?)",
            vec![Value::Integer(i), Value::Integer(i)],
        )
        .unwrap();
    }

    let expected = N * (N + 1) / 2;
    assert_eq!(count(&mut db, "SELECT SUM(v) FROM nums"), expected);
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM nums"), N);
}

#[test]
fn point_lookups_after_split_hit_every_row() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    const N: i64 = 180;
    for i in 1..=N {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 2))
            .unwrap();
    }

    // Every row is individually addressable across page boundaries.
    for i in 1..=N {
        let r = db
            .query_with_params("SELECT v FROM t WHERE id = ?", vec![Value::Integer(i)])
            .unwrap();
        assert_eq!(r.rows.len(), 1, "row {i} not found after splits");
        assert_eq!(r.rows[0].values[0], Value::Integer(i * 2));
    }
}

// ───────────────────── secondary index leaf splits ─────────────────────

#[test]
fn bulk_insert_with_multicolumn_index_query_via_index() {
    let mut db = mem_db();
    db.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, last TEXT, first TEXT, age INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON people(last, first)")
        .unwrap();

    const N: i64 = 100;
    for i in 1..=N {
        db.execute_with_params(
            "INSERT INTO people VALUES (?, ?, ?, ?)",
            vec![
                Value::Integer(i),
                Value::Text(format!("Last{:05}", i)),
                Value::Text(format!("First{:05}", i)),
                Value::Integer(20 + (i % 50)),
            ],
        )
        .unwrap();
    }

    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM people"), N);
    // Table + index both span multiple pages (leaf splits occurred on both).
    assert!(db.page_count() > 2, "expected table+index to span pages");

    // Equality lookup that should ride the multi-column index.
    let r = db
        .query("SELECT id FROM people WHERE last = 'Last00099' AND first = 'First00099'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(99));

    // Range scan over the index prefix.
    let r2 = db
        .query("SELECT COUNT(*) FROM people WHERE last >= 'Last00001' AND last <= 'Last00010'")
        .unwrap();
    assert_eq!(r2.rows[0].values[0], Value::Integer(10));
}

#[test]
fn index_remains_consistent_after_updates() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_k ON t(k)").unwrap();

    const N: i64 = 150;
    for i in 1..=N {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text(format!("k{:04}", i))],
        )
        .unwrap();
    }

    // Move one key to a new value; the index entry should follow.
    db.execute("UPDATE t SET k = 'zzzz' WHERE id = 77").unwrap();
    let gone = db.query("SELECT id FROM t WHERE k = 'k0077'").unwrap();
    assert_eq!(gone.rows.len(), 0);
    let moved = db.query("SELECT id FROM t WHERE k = 'zzzz'").unwrap();
    assert_eq!(moved.rows.len(), 1);
    assert_eq!(moved.rows[0].values[0], Value::Integer(77));
}

// ───────────────────── bulk delete / rebalance ─────────────────────

#[test]
fn bulk_delete_most_rows_then_requery() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    const N: i64 = 200;
    for i in 1..=N {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Integer(i)],
        )
        .unwrap();
    }
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), N);

    // Delete everything except the multiples of 20 (keeps 10 survivors).
    let survivors = N / 20;
    let del = db.execute("DELETE FROM t WHERE id % 20 <> 0").unwrap();
    assert_eq!(del.rows_affected, (N - survivors) as u64);
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), survivors);

    // Survivors are still individually addressable after the rebalance.
    let r = db.query("SELECT id FROM t ORDER BY id").unwrap();
    let ids: Vec<i64> = r
        .rows
        .iter()
        .map(|row| match row.values[0] {
            Value::Integer(n) => n,
            _ => panic!("non-integer id"),
        })
        .collect();
    assert_eq!(ids, (1..=survivors).map(|k| k * 20).collect::<Vec<_>>());

    // Re-insert after the big delete still works.
    db.execute("INSERT INTO t VALUES (5, 5)").unwrap();
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), survivors + 1);
}

#[test]
fn without_rowid_composite_bulk_insert_and_delete() {
    let mut db = mem_db();
    db.execute(
        "CREATE TABLE wr (a INTEGER, b INTEGER, payload TEXT, PRIMARY KEY (a, b)) WITHOUT ROWID",
    )
    .unwrap();

    // 12x12 = 144 composite rows: forces composite-key index leaf splits while
    // staying inside the working regime.
    const ROWS: i64 = 12;
    for a in 1..=ROWS {
        for b in 1..=ROWS {
            db.execute_with_params(
                "INSERT INTO wr VALUES (?, ?, ?)",
                vec![
                    Value::Integer(a),
                    Value::Integer(b),
                    Value::Text(format!("{a}-{b}")),
                ],
            )
            .unwrap();
        }
    }
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM wr"), ROWS * ROWS);

    // Composite-key point lookup.
    let r = db
        .query("SELECT payload FROM wr WHERE a = 7 AND b = 9")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("7-9".to_string()));

    // Bulk delete a whole `a` group (exercises delete-by-prefix on the
    // composite primary-key index).
    let del = db.execute("DELETE FROM wr WHERE a = 7").unwrap();
    assert_eq!(del.rows_affected, ROWS as u64);
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM wr"), ROWS * ROWS - ROWS);
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM wr WHERE a = 7"), 0);
}

// Coverage for the B-tree split / delete-rebalance / overflow paths in
// rsqlite-storage (btree_write.rs leaf & interior splits, overflow chains,
// delete-at-scale rebuilds) driven through the public Database SQL API with
// bulk operations large enough to force page splits and multi-level trees.
//
// These tests are deliberately heavy: the storage engine's split-persistence,
// overflow, and delete/update rebuild bugs are FIXED, so the previously
// "untestable" large-scale scenarios are exercised here. The key fix was the
// pager cache pinning dirty pages instead of silently dropping unflushed pages
// on LRU eviction (which corrupted any multi-level tree built without an
// intervening flush — e.g. a DELETE rebuild), plus batching DELETE into a
// single rebuild so the operation is O(n) rather than O(n²).

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

fn ids(db: &mut Database, sql: &str) -> Vec<i64> {
    let r = db.query(sql).unwrap();
    r.rows
        .iter()
        .map(|row| match row.values[0] {
            Value::Integer(n) => n,
            ref other => panic!("expected integer id, got {other:?}"),
        })
        .collect()
}

/// True when the `sqlite3` CLI is available; tests that cross-check against it
/// skip gracefully when it is not (it IS installed in CI: v3.50).
fn have_sqlite3() -> bool {
    std::process::Command::new("sqlite3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn sqlite3_scalar(path: &str, sql: &str) -> String {
    let out = std::process::Command::new("sqlite3")
        .arg(path)
        .arg(sql)
        .output()
        .expect("run sqlite3");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

// ───────────────────── leaf splits / persistence ─────────────────────

#[test]
fn split_persistence_1000_narrow_rows() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    for i in 1..=1000i64 {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text("x".repeat(10))],
        )
        .unwrap();
    }
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 1000);
    assert_eq!(
        ids(&mut db, "SELECT id FROM t ORDER BY id"),
        (1..=1000).collect::<Vec<_>>()
    );
    assert!(db.page_count() > 1, "expected a multi-page tree");
}

#[test]
fn split_persistence_400_wide_rows() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    for i in 1..=400i64 {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text("x".repeat(300))],
        )
        .unwrap();
    }
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 400);
    assert_eq!(
        ids(&mut db, "SELECT id FROM t ORDER BY id"),
        (1..=400).collect::<Vec<_>>()
    );
}

#[test]
fn multi_level_deepening_full_scan_ordered() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    // Wide rows + high count forces a 2+ level tree (interior pages).
    const N: i64 = 2000;
    for i in 1..=N {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text("w".repeat(250))],
        )
        .unwrap();
    }
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), N);
    // Full ordered scan must visit every row exactly once, in order.
    assert_eq!(
        ids(&mut db, "SELECT id FROM t ORDER BY id"),
        (1..=N).collect::<Vec<_>>()
    );
    // Tree must be deep enough to have many pages.
    assert!(
        db.page_count() > 100,
        "expected a deep multi-level tree, got {} pages",
        db.page_count()
    );
}

#[test]
fn single_multirow_insert_matches_many_inserts() {
    let mut a = mem_db();
    let mut b = mem_db();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    b.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();

    const N: i64 = 500;
    // Many separate inserts.
    for i in 1..=N {
        a.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 7))
            .unwrap();
    }
    // One big multi-row insert.
    let tuples: Vec<String> = (1..=N).map(|i| format!("({i}, {})", i * 7)).collect();
    b.execute(&format!("INSERT INTO t VALUES {}", tuples.join(", ")))
        .unwrap();

    assert_eq!(count(&mut a, "SELECT COUNT(*) FROM t"), N);
    assert_eq!(count(&mut b, "SELECT COUNT(*) FROM t"), N);
    assert_eq!(
        count(&mut a, "SELECT SUM(v) FROM t"),
        count(&mut b, "SELECT SUM(v) FROM t")
    );
    assert_eq!(
        ids(&mut a, "SELECT id FROM t ORDER BY id"),
        ids(&mut b, "SELECT id FROM t ORDER BY id"),
    );
}

// ───────────────────── delete at scale (the regression) ─────────────────────

#[test]
fn delete_at_scale_3000_rows_id_mod_3() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    for i in 1..=3000i64 {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text("m".repeat(200))],
        )
        .unwrap();
    }
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 3000);

    // This DELETE used to fail with Corrupt("invalid B-tree page type: 0x00")
    // because the rebuild grew the tree past the page cache, evicting unflushed
    // pages. It now succeeds.
    db.execute("DELETE FROM t WHERE id % 3 = 0").unwrap();
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 2000);

    let expected: Vec<i64> = (1..=3000).filter(|i| i % 3 != 0).collect();
    assert_eq!(ids(&mut db, "SELECT id FROM t ORDER BY id"), expected);
}

// ───────────────────── secondary index leaf splits ─────────────────────

#[test]
fn bulk_insert_with_multicolumn_index_query_via_index() {
    let mut db = mem_db();
    db.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, last TEXT, first TEXT, age INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON people(last, first)")
        .unwrap();

    const N: i64 = 300;
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
    assert!(db.page_count() > 2, "expected table+index to span pages");

    let r = db
        .query("SELECT id FROM people WHERE last = 'Last00099' AND first = 'First00099'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(99));

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

    const N: i64 = 200;
    for i in 1..=N {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text(format!("k{:04}", i))],
        )
        .unwrap();
    }

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

    const N: i64 = 500;
    for i in 1..=N {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Integer(i)],
        )
        .unwrap();
    }
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), N);

    let survivors = N / 20;
    let del = db.execute("DELETE FROM t WHERE id % 20 <> 0").unwrap();
    assert_eq!(del.rows_affected, (N - survivors) as u64);
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), survivors);

    assert_eq!(
        ids(&mut db, "SELECT id FROM t ORDER BY id"),
        (1..=survivors).map(|k| k * 20).collect::<Vec<_>>()
    );

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

    let r = db
        .query("SELECT payload FROM wr WHERE a = 7 AND b = 9")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("7-9".to_string()));

    let del = db.execute("DELETE FROM wr WHERE a = 7").unwrap();
    assert_eq!(del.rows_affected, ROWS as u64);
    assert_eq!(
        count(&mut db, "SELECT COUNT(*) FROM wr"),
        ROWS * ROWS - ROWS
    );
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM wr WHERE a = 7"), 0);
}

// ───────────────────── overflow round-trips ─────────────────────

#[test]
fn overflow_text_blob_round_trip_widths() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT, blb BLOB)")
        .unwrap();

    let widths = [4062usize, 5000, 8000, 16000, 100000];
    for (i, &w) in widths.iter().enumerate() {
        let id = (i + 1) as i64;
        let txt = "a".repeat(w);
        let blb = vec![0x5Au8; w];
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?, ?)",
            vec![Value::Integer(id), Value::Text(txt), Value::Blob(blb)],
        )
        .unwrap();
    }

    for (i, &w) in widths.iter().enumerate() {
        let id = (i + 1) as i64;
        let r = db
            .query_with_params(
                "SELECT txt, blb FROM t WHERE id = ?",
                vec![Value::Integer(id)],
            )
            .unwrap();
        assert_eq!(r.rows.len(), 1, "missing overflow row id {id}");
        match &r.rows[0].values[0] {
            Value::Text(s) => {
                assert_eq!(s.len(), w, "txt width mismatch for id {id}");
                assert!(s.bytes().all(|b| b == b'a'));
            }
            other => panic!("expected text, got {other:?}"),
        }
        match &r.rows[0].values[1] {
            Value::Blob(b) => {
                assert_eq!(b.len(), w, "blob width mismatch for id {id}");
                assert!(b.iter().all(|&x| x == 0x5A));
            }
            other => panic!("expected blob, got {other:?}"),
        }
    }
}

#[test]
fn overflow_survives_close_and_reopen() {
    let path = "/tmp/rsqlite_btree_overflow_reopen.db";
    let _ = std::fs::remove_file(path);
    let vfs = rsqlite_vfs::native::NativeVfs::new();

    let widths = [4062usize, 8000, 100000];
    {
        let mut db = Database::create(&vfs, path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT)")
            .unwrap();
        for (i, &w) in widths.iter().enumerate() {
            db.execute_with_params(
                "INSERT INTO t VALUES (?, ?)",
                vec![Value::Integer((i + 1) as i64), Value::Text("q".repeat(w))],
            )
            .unwrap();
        }
    }

    let mut db = Database::open(&vfs, path).unwrap();
    for (i, &w) in widths.iter().enumerate() {
        let r = db
            .query_with_params(
                "SELECT txt FROM t WHERE id = ?",
                vec![Value::Integer((i + 1) as i64)],
            )
            .unwrap();
        match &r.rows[0].values[0] {
            Value::Text(s) => assert_eq!(s.len(), w),
            other => panic!("expected text, got {other:?}"),
        }
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn overflow_plus_split_together() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    // Mix many wide-but-inline rows (force splits) with periodic overflow rows.
    for i in 1..=600i64 {
        let payload = if i % 25 == 0 {
            "o".repeat(9000) // overflow
        } else {
            "p".repeat(120) // inline
        };
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text(payload)],
        )
        .unwrap();
    }
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 600);
    assert_eq!(
        ids(&mut db, "SELECT id FROM t ORDER BY id"),
        (1..=600).collect::<Vec<_>>()
    );

    // Verify the wide rows still reassemble fully.
    for i in (25..=600).step_by(25) {
        let r = db
            .query_with_params(
                "SELECT length(payload) FROM t WHERE id = ?",
                vec![Value::Integer(i)],
            )
            .unwrap();
        assert_eq!(
            r.rows[0].values[0],
            Value::Integer(9000),
            "overflow row {i} wrong length"
        );
    }
}

#[test]
fn overflow_boundary_widths() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT)")
        .unwrap();
    // Boundaries around the local/overflow threshold on a 4 KB page.
    for (i, w) in [4060usize, 4061, 4062].iter().enumerate() {
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer((i + 1) as i64), Value::Text("b".repeat(*w))],
        )
        .unwrap();
    }
    for (i, w) in [4060usize, 4061, 4062].iter().enumerate() {
        let r = db
            .query_with_params(
                "SELECT length(txt) FROM t WHERE id = ?",
                vec![Value::Integer((i + 1) as i64)],
            )
            .unwrap();
        assert_eq!(
            r.rows[0].values[0],
            Value::Integer(*w as i64),
            "boundary width {w}"
        );
    }
}

// ───────────────── heavy delete + update + overflow (NativeVfs) ─────────────────

#[test]
fn heavy_delete_update_overflow_scenario() {
    let path = "/tmp/rsqlite_btree_heavy.db";
    let _ = std::fs::remove_file(path);
    let vfs = rsqlite_vfs::native::NativeVfs::new();

    {
        let mut db = Database::create(&vfs, path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        // 3000 rows of 200B.
        for i in 1..=3000i64 {
            db.execute_with_params(
                "INSERT INTO t VALUES (?, ?)",
                vec![Value::Integer(i), Value::Text("m".repeat(200))],
            )
            .unwrap();
        }
        // Two 20000-byte overflow rows (ids beyond the delete range).
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(4001), Value::Text("o".repeat(20000))],
        )
        .unwrap();
        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(4002), Value::Text("o".repeat(20000))],
        )
        .unwrap();
        assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 3002);

        // DELETE 1000 rows (id % 3 == 0 AND id <= 3000).
        db.execute("DELETE FROM t WHERE id % 3 = 0 AND id <= 3000")
            .unwrap();
        assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 2002);

        // UPDATE one small row to 25000 bytes (small -> overflow rebuild).
        db.execute_with_params(
            "UPDATE t SET payload = ? WHERE id = 1",
            vec![Value::Text("u".repeat(25000))],
        )
        .unwrap();

        // Verify counts and lengths in-process.
        assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 2002);
        let len1 = db
            .query("SELECT length(payload) FROM t WHERE id = 1")
            .unwrap();
        assert_eq!(len1.rows[0].values[0], Value::Integer(25000));
        for ov in [4001i64, 4002] {
            let r = db
                .query_with_params(
                    "SELECT length(payload) FROM t WHERE id = ?",
                    vec![Value::Integer(ov)],
                )
                .unwrap();
            assert_eq!(
                r.rows[0].values[0],
                Value::Integer(20000),
                "overflow row {ov}"
            );
        }
        let surviving: Vec<i64> = (1..=3000)
            .filter(|i| i % 3 != 0)
            .chain([4001, 4002])
            .collect();
        assert_eq!(ids(&mut db, "SELECT id FROM t ORDER BY id"), surviving);
    } // drop flushes

    // sqlite3 integrity_check + counts on the rsqlite-written DB AFTER deletes/updates.
    if have_sqlite3() {
        assert_eq!(sqlite3_scalar(path, "PRAGMA integrity_check;"), "ok");
        assert_eq!(sqlite3_scalar(path, "SELECT COUNT(*) FROM t;"), "2002");
        assert_eq!(
            sqlite3_scalar(path, "SELECT length(payload) FROM t WHERE id = 1;"),
            "25000"
        );
        assert_eq!(
            sqlite3_scalar(path, "SELECT length(payload) FROM t WHERE id = 4001;"),
            "20000"
        );
        assert_eq!(
            sqlite3_scalar(
                path,
                "SELECT COUNT(*) FROM t WHERE id % 3 = 0 AND id <= 3000;"
            ),
            "0"
        );
    }

    let _ = std::fs::remove_file(path);
}

// ───────────────────── sqlite3 CLI cross-checks ─────────────────────

#[test]
fn sqlite3_cross_check_reads_rsqlite_db() {
    if !have_sqlite3() {
        return; // sqlite3 absent: skip gracefully
    }
    let path = "/tmp/rsqlite_btree_xcheck_a.db";
    let _ = std::fs::remove_file(path);
    let vfs = rsqlite_vfs::native::NativeVfs::new();
    {
        let mut db = Database::create(&vfs, path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        for i in 1..=1500i64 {
            db.execute_with_params(
                "INSERT INTO t VALUES (?, ?)",
                vec![Value::Integer(i), Value::Text("c".repeat(150))],
            )
            .unwrap();
        }
        db.execute("DELETE FROM t WHERE id % 2 = 0").unwrap();
    }
    assert_eq!(sqlite3_scalar(path, "PRAGMA integrity_check;"), "ok");
    assert_eq!(sqlite3_scalar(path, "SELECT COUNT(*) FROM t;"), "750");
    assert_eq!(sqlite3_scalar(path, "SELECT MIN(id) FROM t;"), "1");
    assert_eq!(sqlite3_scalar(path, "SELECT MAX(id) FROM t;"), "1499");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sqlite3_cross_check_rsqlite_reads_sqlite3_db() {
    if !have_sqlite3() {
        return; // sqlite3 absent: skip gracefully
    }
    let path = "/tmp/rsqlite_btree_xcheck_b.db";
    let _ = std::fs::remove_file(path);

    // Build the DB with the sqlite3 CLI, then read it back through rsqlite.
    let mut script =
        String::from("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT);\nBEGIN;\n");
    for i in 1..=1200i64 {
        script.push_str(&format!(
            "INSERT INTO t VALUES ({i}, '{}');\n",
            "s".repeat(180)
        ));
    }
    script.push_str("COMMIT;\n");
    // Feed the (large) script via stdin — passing it as a single CLI argument
    // would exceed the OS argument-length limit.
    use std::io::Write;
    let mut child = std::process::Command::new("sqlite3")
        .arg(path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn sqlite3");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().expect("run sqlite3");
    assert!(
        out.status.success(),
        "sqlite3 build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let vfs = rsqlite_vfs::native::NativeVfs::new();
    let mut db = Database::open(&vfs, path).unwrap();
    assert_eq!(count(&mut db, "SELECT COUNT(*) FROM t"), 1200);
    assert_eq!(
        ids(&mut db, "SELECT id FROM t ORDER BY id"),
        (1..=1200).collect::<Vec<_>>()
    );
    let r = db
        .query("SELECT length(payload) FROM t WHERE id = 600")
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(180));

    let _ = std::fs::remove_file(path);
}

// Coverage for the public Database API surface in database.rs:
//   - create/open file round-trip (create then reopen, data survives)
//   - rehydrate_persisted_vtabs (FTS5 reopened, data still queryable)
//   - execute_sql dispatcher arms (SELECT / INSERT / PRAGMA / ATTACH)
//   - describe_plan EXPLAIN QUERY PLAN arms that lacked coverage
//     (Window, Intersect, Except, virtual/FTS5 scan)
//
// All assertions reflect the engine's actual behavior, confirmed by running.

use super::*;
use crate::database::SqlResult;
use crate::types::Value;

fn mem_db() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    Database::create(&vfs, "test.db").unwrap()
}

/// Pull the text of the `detail` column (index 3) from an EXPLAIN result.
fn plan_details(r: &crate::types::QueryResult) -> Vec<String> {
    r.rows
        .iter()
        .filter_map(|row| match &row.values[3] {
            Value::Text(s) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

// ───────────────────── create / open round-trip ─────────────────────

#[test]
fn create_then_open_preserves_rows() {
    let db_path = "/tmp/rsqlite_tier2_roundtrip.db";
    let _ = std::fs::remove_file(db_path);

    let vfs = rsqlite_vfs::native::NativeVfs::new();
    {
        let mut db = Database::create(&vfs, db_path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')")
            .unwrap();
        // drop the db value so the file is fully flushed/closed.
    }

    // Reopen from a fresh handle: drives Database::open + Catalog::load.
    let mut db = Database::open(&vfs, db_path).unwrap();
    let r = db.query("SELECT id, label FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[0].values[1], Value::Text("one".to_string()));
    assert_eq!(r.rows[2].values[1], Value::Text("three".to_string()));

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn open_sees_schema_from_catalog() {
    let db_path = "/tmp/rsqlite_tier2_schema.db";
    let _ = std::fs::remove_file(db_path);

    let vfs = rsqlite_vfs::native::NativeVfs::new();
    {
        let mut db = Database::create(&vfs, db_path).unwrap();
        db.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY, kind TEXT, qty INTEGER)")
            .unwrap();
    }

    let db = Database::open(&vfs, db_path).unwrap();
    let table = db
        .catalog()
        .get_table("widgets")
        .expect("widgets table should survive reopen");
    assert_eq!(table.columns.len(), 3);

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn memory_vfs_persists_across_open_same_instance() {
    // MemoryVfs keeps file contents inside the shared backing store, so a
    // second Database::open on the same vfs instance sees prior writes.
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    {
        let mut db = Database::create(&vfs, "shared.db").unwrap();
        db.execute("CREATE TABLE m (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO m VALUES (1, 'persisted')").unwrap();
    }
    let mut db = Database::open(&vfs, "shared.db").unwrap();
    let r = db.query("SELECT v FROM m WHERE id = 1").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("persisted".to_string()));
}

// ───────────────── rehydrate_persisted_vtabs (FTS5) ─────────────────

#[test]
fn fts5_data_survives_reopen() {
    // Drives rehydrate_persisted_vtabs: on reopen the FTS5 inverted index
    // is restored from its shadow table and MATCH queries still work.
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    {
        let mut db = Database::create(&vfs, "fts.db").unwrap();
        db.execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
            .unwrap();
        db.execute("INSERT INTO docs VALUES ('the quick brown fox')")
            .unwrap();
        db.execute("INSERT INTO docs VALUES ('lazy sleeping dog')")
            .unwrap();
    }

    let mut db = Database::open(&vfs, "fts.db").unwrap();
    let r = db
        .query("SELECT rowid FROM docs WHERE content MATCH 'quick' ORDER BY rowid")
        .unwrap();
    assert_eq!(
        r.rows.len(),
        1,
        "FTS5 index should be queryable after reopen"
    );
    assert_eq!(r.rows[0].values[0], Value::Integer(1));

    let r2 = db
        .query("SELECT rowid FROM docs WHERE content MATCH 'dog'")
        .unwrap();
    assert_eq!(r2.rows.len(), 1);
    assert_eq!(r2.rows[0].values[0], Value::Integer(2));
}

// ───────────────────── execute_sql dispatcher ─────────────────────

#[test]
fn execute_sql_select_returns_query() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();

    match db.execute_sql("SELECT n FROM t ORDER BY id").unwrap() {
        SqlResult::Query(q) => {
            assert_eq!(q.rows.len(), 2);
            assert_eq!(q.rows[0].values[0], Value::Integer(10));
        }
        SqlResult::Execute(_) => panic!("SELECT should dispatch to Query"),
    }
}

#[test]
fn execute_sql_insert_returns_execute() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();

    match db.execute_sql("INSERT INTO t VALUES (1, 99)").unwrap() {
        SqlResult::Execute(e) => assert_eq!(e.rows_affected, 1),
        SqlResult::Query(_) => panic!("INSERT should dispatch to Execute"),
    }
}

#[test]
fn execute_sql_pragma_table_info_returns_query() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    match db.execute_sql("PRAGMA table_info(t)").unwrap() {
        SqlResult::Query(q) => {
            // one row per column.
            assert_eq!(q.rows.len(), 2);
        }
        SqlResult::Execute(_) => panic!("PRAGMA table_info should dispatch to Query"),
    }
}

#[test]
fn execute_sql_attach_returns_execute() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    {
        let mut other = Database::create(&vfs, "aux.db").unwrap();
        other
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
            .unwrap();
    }
    let mut db = Database::create(&vfs, "main.db").unwrap();

    match db.execute_sql("ATTACH DATABASE 'aux.db' AS aux").unwrap() {
        SqlResult::Execute(_) => {}
        SqlResult::Query(_) => panic!("ATTACH should dispatch to Execute"),
    }

    // database_list pragma should now report the attached db (Query arm).
    match db.execute_sql("PRAGMA database_list").unwrap() {
        SqlResult::Query(q) => {
            let names: Vec<String> = q
                .rows
                .iter()
                .filter_map(|r| match &r.values[1] {
                    Value::Text(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            assert!(names.contains(&"aux".to_string()));
        }
        SqlResult::Execute(_) => panic!("database_list should dispatch to Query"),
    }
}

// ───────────────────── EXPLAIN QUERY PLAN arms ─────────────────────

#[test]
fn explain_window_function() {
    let mut db = mem_db();
    db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amt INTEGER)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 5)")
        .unwrap();

    let r = db
        .query(
            "EXPLAIN QUERY PLAN \
             SELECT region, amt, SUM(amt) OVER (PARTITION BY region) FROM sales",
        )
        .unwrap();
    let details = plan_details(&r);
    assert!(
        details.iter().any(|d| d.contains("WINDOW FUNCTION")),
        "expected WINDOW FUNCTION node: {details:?}"
    );
}

#[test]
fn explain_intersect() {
    let mut db = mem_db();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
        .unwrap();

    let r = db
        .query("EXPLAIN QUERY PLAN SELECT id FROM a INTERSECT SELECT id FROM b")
        .unwrap();
    let details = plan_details(&r);
    assert!(
        details.iter().any(|d| d.contains("INTERSECT")),
        "expected INTERSECT compound node: {details:?}"
    );
}

#[test]
fn explain_except() {
    let mut db = mem_db();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
        .unwrap();

    let r = db
        .query("EXPLAIN QUERY PLAN SELECT id FROM a EXCEPT SELECT id FROM b")
        .unwrap();
    let details = plan_details(&r);
    assert!(
        details.iter().any(|d| d.contains("EXCEPT")),
        "expected EXCEPT compound node: {details:?}"
    );
}

#[test]
fn explain_virtual_table_scan() {
    let mut db = mem_db();
    db.execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES ('alpha beta')")
        .unwrap();

    let r = db.query("EXPLAIN QUERY PLAN SELECT * FROM docs").unwrap();
    let details = plan_details(&r);
    assert!(
        details.iter().any(|d| d.contains("VIRTUAL TABLE")),
        "expected a VIRTUAL TABLE plan node: {details:?}"
    );
}

// ───────── implicit PK/UNIQUE indexes: seek instead of scan ─────────

#[test]
fn text_pk_lookup_uses_index_search_not_scan() {
    // The cache table shape: a TEXT PRIMARY KEY. Its implicit autoindex must
    // turn `WHERE key = ?` into an index SEARCH, not a full-table SCAN.
    let mut db = mem_db();
    db.execute("CREATE TABLE cache (key TEXT PRIMARY KEY, val TEXT)")
        .unwrap();
    for i in 0..50 {
        db.execute(&format!("INSERT INTO cache VALUES ('k{i}', 'v{i}')"))
            .unwrap();
    }

    let details = plan_details(
        &db.query("EXPLAIN QUERY PLAN SELECT val FROM cache WHERE key = 'k7'")
            .unwrap(),
    );
    assert!(
        details.iter().any(|d| d.contains("SEARCH") && d.contains("USING INDEX")),
        "expected an index SEARCH for a TEXT PK lookup, got: {details:?}"
    );
    assert!(
        !details.iter().any(|d| d.contains("SCAN TABLE cache")),
        "TEXT PK lookup must not fall back to a table scan: {details:?}"
    );
}

#[test]
fn text_pk_lookup_returns_correct_rows_at_scale() {
    // Correctness of the seek path against a table large enough to span
    // multiple index/table b-tree pages.
    let mut db = mem_db();
    db.execute("CREATE TABLE cache (key TEXT PRIMARY KEY, val TEXT)")
        .unwrap();
    let n = 2000;
    for i in 0..n {
        db.execute(&format!("INSERT INTO cache VALUES ('k{i:05}', 'v{i}')"))
            .unwrap();
    }

    // Every key resolves to exactly its row via the index seek.
    for i in [0, 1, 42, 999, 1000, n - 1] {
        let r = db
            .query_with_params(
                "SELECT val FROM cache WHERE key = ?",
                vec![Value::Text(format!("k{i:05}"))],
            )
            .unwrap();
        assert_eq!(r.rows.len(), 1, "one row for k{i:05}");
        assert_eq!(r.rows[0].values[0], Value::Text(format!("v{i}")));
    }

    // A missing key yields nothing.
    let r = db
        .query_with_params(
            "SELECT val FROM cache WHERE key = ?",
            vec![Value::Text("nope".into())],
        )
        .unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn text_pk_delete_and_replace_keep_index_consistent() {
    // DELETE ... WHERE key = ? and INSERT OR REPLACE must keep the autoindex
    // consistent — a stale index entry would resurrect or hide rows.
    let mut db = mem_db();
    db.execute("CREATE TABLE cache (key TEXT PRIMARY KEY, val TEXT)")
        .unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO cache VALUES ('k{i}', 'v{i}')"))
            .unwrap();
    }

    db.execute("DELETE FROM cache WHERE key = 'k50'").unwrap();
    let r = db.query("SELECT val FROM cache WHERE key = 'k50'").unwrap();
    assert_eq!(r.rows.len(), 0, "deleted key must not be found via the index");

    db.execute("INSERT OR REPLACE INTO cache VALUES ('k51', 'replaced')")
        .unwrap();
    let r = db.query("SELECT val FROM cache WHERE key = 'k51'").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("replaced".into()));

    // Total row count is intact (one deleted, one replaced-in-place).
    let r = db.query("SELECT COUNT(*) FROM cache").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(99));
}

#[test]
fn delete_uses_index_candidates_correctly() {
    // The candidate narrowing must never drop a row the full predicate matches,
    // across: composite-PK prefix seek, a partially-indexed compound WHERE
    // (scan fallback on the non-leading columns), and an OR (no extraction).
    let mut db = mem_db();
    db.execute("CREATE TABLE tags (tag TEXT NOT NULL, ns TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (tag, ns, key))")
        .unwrap();
    for t in 0..20 {
        for k in 0..5 {
            db.execute(&format!(
                "INSERT INTO tags VALUES ('t{t}', 'data', 'k{k}')"
            ))
            .unwrap();
        }
    }
    assert_eq!(
        db.query("SELECT COUNT(*) FROM tags").unwrap().rows[0].values[0],
        Value::Integer(100)
    );

    // Composite-PK prefix seek: DELETE ... WHERE tag = ? removes exactly that
    // tag's 5 rows (tag is the leading PK column).
    db.execute("DELETE FROM tags WHERE tag = 't3'").unwrap();
    assert_eq!(
        db.query("SELECT COUNT(*) FROM tags WHERE tag = 't3'").unwrap().rows[0].values[0],
        Value::Integer(0)
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM tags").unwrap().rows[0].values[0],
        Value::Integer(95)
    );

    // Full-tuple equality: removes one row.
    db.execute("DELETE FROM tags WHERE tag = 't4' AND ns = 'data' AND key = 'k2'")
        .unwrap();
    assert_eq!(
        db.query("SELECT COUNT(*) FROM tags").unwrap().rows[0].values[0],
        Value::Integer(94)
    );

    // Non-leading compound (ns + key, no leading tag): must fall back to a scan
    // and still delete every matching row across all tags.
    db.execute("DELETE FROM tags WHERE ns = 'data' AND key = 'k0'")
        .unwrap();
    assert_eq!(
        db.query("SELECT COUNT(*) FROM tags WHERE key = 'k0'").unwrap().rows[0].values[0],
        Value::Integer(0)
    );

    // OR predicate: no equality extraction → scan; both arms honored.
    let before = db.query("SELECT COUNT(*) FROM tags").unwrap().rows[0].values[0].clone();
    db.execute("DELETE FROM tags WHERE tag = 't5' OR key = 'k1'")
        .unwrap();
    let after = db.query("SELECT COUNT(*) FROM tags").unwrap().rows[0].values[0].clone();
    assert!(
        matches!((&before, &after), (Value::Integer(b), Value::Integer(a)) if a < b),
        "OR delete should remove rows: {before:?} -> {after:?}"
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM tags WHERE tag = 't5' OR key = 'k1'").unwrap().rows[0]
            .values[0],
        Value::Integer(0),
        "no row matching either arm should survive"
    );
}

#[test]
fn unique_column_lookup_uses_index_search() {
    let mut db = mem_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    for i in 0..30 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'u{i}@x.io')"))
            .unwrap();
    }
    let details = plan_details(
        &db.query("EXPLAIN QUERY PLAN SELECT id FROM t WHERE email = 'u5@x.io'")
            .unwrap(),
    );
    assert!(
        details.iter().any(|d| d.contains("SEARCH") && d.contains("USING INDEX")),
        "expected an index SEARCH for a UNIQUE column lookup, got: {details:?}"
    );
    let r = db
        .query("SELECT id FROM t WHERE email = 'u5@x.io'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(5));
}

#[test]
fn explain_union_compound() {
    let mut db = mem_db();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
        .unwrap();

    let r = db
        .query("EXPLAIN QUERY PLAN SELECT id FROM a UNION SELECT id FROM b")
        .unwrap();
    let details = plan_details(&r);
    assert!(
        details.iter().any(|d| d.contains("COMPOUND QUERY")),
        "expected COMPOUND QUERY node: {details:?}"
    );
}

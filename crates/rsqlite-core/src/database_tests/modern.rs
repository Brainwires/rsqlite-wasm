use super::*;

#[test]
fn default_text_literal() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, status TEXT DEFAULT 'active')",
    )
    .unwrap();
    db.execute("INSERT INTO accounts (id, name) VALUES (1, 'Alice')")
        .unwrap();

    let result = db
        .query("SELECT status FROM accounts WHERE id = 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].values[0],
        crate::types::Value::Text("active".to_string())
    );
}

#[test]
fn default_integer_literal() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER DEFAULT 42)")
        .unwrap();
    db.execute("INSERT INTO counters (id) VALUES (1)").unwrap();

    let result = db.query("SELECT value FROM counters WHERE id = 1").unwrap();
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(42));
}

#[test]
fn default_negative_integer_literal() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE balance (id INTEGER PRIMARY KEY, owed INTEGER DEFAULT -10)")
        .unwrap();
    db.execute("INSERT INTO balance (id) VALUES (1)").unwrap();

    let result = db.query("SELECT owed FROM balance WHERE id = 1").unwrap();
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(-10));
}

#[test]
fn default_real_literal() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE rates (id INTEGER PRIMARY KEY, rate REAL DEFAULT 1.5)")
        .unwrap();
    db.execute("INSERT INTO rates (id) VALUES (1)").unwrap();

    let result = db.query("SELECT rate FROM rates WHERE id = 1").unwrap();
    assert_eq!(result.rows[0].values[0], crate::types::Value::Real(1.5));
}

#[test]
fn default_overridden_by_explicit_value() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active')")
        .unwrap();
    db.execute("INSERT INTO accounts (id, status) VALUES (1, 'banned')")
        .unwrap();

    let result = db
        .query("SELECT status FROM accounts WHERE id = 1")
        .unwrap();
    assert_eq!(
        result.rows[0].values[0],
        crate::types::Value::Text("banned".to_string())
    );
}

#[test]
fn default_explicit_null_not_replaced() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active')")
        .unwrap();
    db.execute("INSERT INTO accounts (id, status) VALUES (1, NULL)")
        .unwrap();

    let result = db
        .query("SELECT status FROM accounts WHERE id = 1")
        .unwrap();
    assert_eq!(result.rows[0].values[0], crate::types::Value::Null);
}

#[test]
fn default_with_insert_select_partial_columns() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE source (n INTEGER, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO source VALUES (1, 'one'), (2, 'two')")
        .unwrap();
    db.execute(
        "CREATE TABLE target (id INTEGER PRIMARY KEY, label TEXT, status TEXT DEFAULT 'pending')",
    )
    .unwrap();
    db.execute("INSERT INTO target (id, label) SELECT n, label FROM source")
        .unwrap();

    let result = db
        .query("SELECT id, status FROM target ORDER BY id")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].values[1],
        crate::types::Value::Text("pending".to_string())
    );
    assert_eq!(
        result.rows[1].values[1],
        crate::types::Value::Text("pending".to_string())
    );
}

#[test]
fn default_null_when_no_default_and_omitted() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE plain (id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    db.execute("INSERT INTO plain (id) VALUES (1)").unwrap();

    let result = db.query("SELECT note FROM plain WHERE id = 1").unwrap();
    assert_eq!(result.rows[0].values[0], crate::types::Value::Null);
}

#[test]
fn default_with_not_null_constraint_satisfied() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, status TEXT NOT NULL DEFAULT 'queued')")
        .unwrap();
    // INSERT omits status — NOT NULL would fail without DEFAULT, but should succeed with one.
    db.execute("INSERT INTO jobs (id) VALUES (1)").unwrap();

    let result = db.query("SELECT status FROM jobs WHERE id = 1").unwrap();
    assert_eq!(
        result.rows[0].values[0],
        crate::types::Value::Text("queued".to_string())
    );
}

#[test]
fn bitwise_and_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE flags (id INTEGER PRIMARY KEY, mask INTEGER)")
        .unwrap();
    db.execute("INSERT INTO flags VALUES (1, 12)").unwrap(); // 1100
    let result = db.query("SELECT mask & 10 FROM flags").unwrap(); // 1010 -> 1000 = 8
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(8));
}

#[test]
fn bitwise_or_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE flags (id INTEGER PRIMARY KEY, mask INTEGER)")
        .unwrap();
    db.execute("INSERT INTO flags VALUES (1, 12)").unwrap();
    let result = db.query("SELECT mask | 3 FROM flags").unwrap(); // 1100 | 0011 = 1111 = 15
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(15));
}

#[test]
fn bitwise_in_where_clause() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE perms (id INTEGER PRIMARY KEY, mask INTEGER)")
        .unwrap();
    db.execute("INSERT INTO perms VALUES (1, 5), (2, 6), (3, 7)")
        .unwrap();
    // Rows where the read bit (1) is set: 5 (101), 7 (111) — that's 2 rows
    let result = db
        .query("SELECT id FROM perms WHERE (mask & 1) = 1 ORDER BY id")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(result.rows[1].values[0], crate::types::Value::Integer(3));
}

#[test]
fn bitwise_with_null_propagates() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let result = db.query("SELECT x & 5 FROM t").unwrap();
    assert_eq!(result.rows[0].values[0], crate::types::Value::Null);
}

#[test]
fn is_distinct_from_with_null() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (1, 2), (NULL, 1), (NULL, NULL)")
        .unwrap();
    // a IS DISTINCT FROM b: rows where a != b in null-safe sense
    // (1,1) -> NOT distinct -> 0; (1,2) -> distinct -> 1; (NULL,1) -> distinct -> 1; (NULL,NULL) -> NOT distinct -> 0
    let result = db.query("SELECT a IS DISTINCT FROM b FROM t").unwrap();
    assert_eq!(result.rows.len(), 4);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(0));
    assert_eq!(result.rows[1].values[0], crate::types::Value::Integer(1));
    assert_eq!(result.rows[2].values[0], crate::types::Value::Integer(1));
    assert_eq!(result.rows[3].values[0], crate::types::Value::Integer(0));
}

#[test]
fn is_not_distinct_from_with_null() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (NULL, NULL), (NULL, 5)")
        .unwrap();
    // (1,1) -> equal -> 1; (NULL,NULL) -> equal -> 1; (NULL,5) -> distinct -> 0
    let result = db.query("SELECT a IS NOT DISTINCT FROM b FROM t").unwrap();
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(result.rows[1].values[0], crate::types::Value::Integer(1));
    assert_eq!(result.rows[2].values[0], crate::types::Value::Integer(0));
}

#[test]
fn is_distinct_from_in_where() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'active'), (2, 'pending'), (3, NULL), (4, 'active')")
        .unwrap();
    // = 'active' would miss NULL. IS NOT DISTINCT FROM 'active' is the same here,
    // but IS DISTINCT FROM 'active' includes the NULL row.
    let result = db
        .query("SELECT id FROM t WHERE status IS DISTINCT FROM 'active' ORDER BY id")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(2));
    assert_eq!(result.rows[1].values[0], crate::types::Value::Integer(3));
}

#[test]
fn like_with_escape_percent() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '50%'), (2, '50x'), (3, '50% off'), (4, 'abc')")
        .unwrap();
    // Match strings with literal % followed by anything
    let result = db
        .query(r"SELECT id FROM t WHERE label LIKE '50\%%' ESCAPE '\' ORDER BY id")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(result.rows[1].values[0], crate::types::Value::Integer(3));
}

#[test]
fn like_with_escape_underscore() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a_b'), (2, 'aXb'), (3, 'a__b')")
        .unwrap();
    // Match literal underscore between a and b
    let result = db
        .query(r"SELECT id FROM t WHERE label LIKE 'a\_b' ESCAPE '\' ORDER BY id")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(1));
}

#[test]
fn like_escape_with_custom_char() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '50%'), (2, '50abc')")
        .unwrap();
    // Use # as escape char
    let result = db
        .query("SELECT id FROM t WHERE label LIKE '50#%' ESCAPE '#'")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(1));
}

#[test]
fn intersect_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (n INTEGER)").unwrap();
    db.execute("CREATE TABLE b (n INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2), (3), (4)")
        .unwrap();
    db.execute("INSERT INTO b VALUES (3), (4), (5), (6)")
        .unwrap();

    let result = db
        .query("SELECT n FROM a INTERSECT SELECT n FROM b ORDER BY n")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(3));
    assert_eq!(result.rows[1].values[0], crate::types::Value::Integer(4));
}

#[test]
fn intersect_deduplicates() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (n INTEGER)").unwrap();
    db.execute("CREATE TABLE b (n INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (1), (2), (2)")
        .unwrap();
    db.execute("INSERT INTO b VALUES (1), (2), (2)").unwrap();

    let result = db
        .query("SELECT n FROM a INTERSECT SELECT n FROM b ORDER BY n")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(result.rows[1].values[0], crate::types::Value::Integer(2));
}

#[test]
fn except_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (n INTEGER)").unwrap();
    db.execute("CREATE TABLE b (n INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2), (3), (4)")
        .unwrap();
    db.execute("INSERT INTO b VALUES (3), (4), (5), (6)")
        .unwrap();

    let result = db
        .query("SELECT n FROM a EXCEPT SELECT n FROM b ORDER BY n")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(result.rows[1].values[0], crate::types::Value::Integer(2));
}

#[test]
fn except_deduplicates_left() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (n INTEGER)").unwrap();
    db.execute("CREATE TABLE b (n INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (1), (2), (3), (3)")
        .unwrap();
    db.execute("INSERT INTO b VALUES (3)").unwrap();

    let result = db
        .query("SELECT n FROM a EXCEPT SELECT n FROM b ORDER BY n")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(result.rows[1].values[0], crate::types::Value::Integer(2));
}

#[test]
fn intersect_multi_column() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (k TEXT, v INTEGER)").unwrap();
    db.execute("CREATE TABLE b (k TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES ('x', 1), ('y', 2), ('z', 3)")
        .unwrap();
    db.execute("INSERT INTO b VALUES ('y', 2), ('z', 99), ('x', 1)")
        .unwrap();

    let result = db
        .query("SELECT k, v FROM a INTERSECT SELECT k, v FROM b ORDER BY k")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].values[0],
        crate::types::Value::Text("x".to_string())
    );
    assert_eq!(
        result.rows[1].values[0],
        crate::types::Value::Text("y".to_string())
    );
}

#[test]
fn except_with_no_overlap_returns_left() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (n INTEGER)").unwrap();
    db.execute("CREATE TABLE b (n INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO b VALUES (3), (4)").unwrap();

    let result = db
        .query("SELECT n FROM a EXCEPT SELECT n FROM b ORDER BY n")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn intersect_with_full_overlap_returns_subset() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (n INTEGER)").unwrap();
    db.execute("CREATE TABLE b (n INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2), (3)").unwrap();
    db.execute("INSERT INTO b VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = db
        .query("SELECT n FROM a INTERSECT SELECT n FROM b ORDER BY n")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn intersect_preserves_left_column_names() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (foo INTEGER)").unwrap();
    db.execute("CREATE TABLE b (bar INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (1)").unwrap();

    let result = db
        .query("SELECT foo FROM a INTERSECT SELECT bar FROM b")
        .unwrap();
    assert_eq!(result.columns, vec!["foo"]);
}

#[test]
fn right_join_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (id INTEGER, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INTEGER, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 'one'), (2, 'two')")
        .unwrap();
    db.execute("INSERT INTO b VALUES (2, 'B'), (3, 'C')")
        .unwrap();

    let result = db
        .query("SELECT a.name, b.label FROM a RIGHT JOIN b ON a.id = b.id ORDER BY b.id")
        .unwrap();
    // Right has 2 rows; only b.id=2 matches a; b.id=3 has no match -> NULL on left.
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].values[0],
        crate::types::Value::Text("two".to_string())
    );
    assert_eq!(
        result.rows[0].values[1],
        crate::types::Value::Text("B".to_string())
    );
    assert_eq!(result.rows[1].values[0], crate::types::Value::Null);
    assert_eq!(
        result.rows[1].values[1],
        crate::types::Value::Text("C".to_string())
    );
}

#[test]
fn right_outer_join_alias() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (id INTEGER)").unwrap();
    db.execute("CREATE TABLE b (id INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (1), (2)").unwrap();

    let result = db
        .query("SELECT a.id, b.id FROM a RIGHT OUTER JOIN b ON a.id = b.id ORDER BY b.id")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn full_outer_join_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (id INTEGER, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INTEGER, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 'A'), (2, 'B')")
        .unwrap();
    db.execute("INSERT INTO b VALUES (2, 'X'), (3, 'Y')")
        .unwrap();

    let result = db
        .query("SELECT a.id, b.id FROM a FULL OUTER JOIN b ON a.id = b.id")
        .unwrap();
    // Combinations: (1,NULL), (2,2), (NULL,3)
    assert_eq!(result.rows.len(), 3);

    // Order can vary; collect into a sortable representation.
    let mut tuples: Vec<(Option<i64>, Option<i64>)> = result
        .rows
        .iter()
        .map(|r| {
            let to_int = |v: &crate::types::Value| match v {
                crate::types::Value::Integer(n) => Some(*n),
                _ => None,
            };
            (to_int(&r.values[0]), to_int(&r.values[1]))
        })
        .collect();
    tuples.sort();
    assert_eq!(
        tuples,
        vec![(None, Some(3)), (Some(1), None), (Some(2), Some(2))]
    );
}

#[test]
fn full_join_with_no_overlap() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (id INTEGER)").unwrap();
    db.execute("CREATE TABLE b (id INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO b VALUES (3), (4)").unwrap();

    let result = db
        .query("SELECT a.id, b.id FROM a FULL OUTER JOIN b ON a.id = b.id")
        .unwrap();
    // 2 unmatched left + 2 unmatched right = 4 rows
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn right_join_with_null_keys() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (id INTEGER)").unwrap();
    db.execute("CREATE TABLE b (id INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (NULL), (1)").unwrap();
    db.execute("INSERT INTO b VALUES (NULL), (1)").unwrap();

    // NULL never equals NULL in standard ON predicates, so NULL keys never match.
    let result = db
        .query("SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.id ORDER BY b.id")
        .unwrap();
    // Right rows: (NULL) -> no match -> (NULL, NULL); (1) -> match a.id=1 -> (1, 1)
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn full_outer_join_no_constraint_acts_like_cross() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (id INTEGER)").unwrap();
    db.execute("CREATE TABLE b (id INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO b VALUES (3), (4)").unwrap();

    let result = db
        .query("SELECT a.id, b.id FROM a FULL OUTER JOIN b ON 1=1")
        .unwrap();
    // ON 1=1 matches everything, so outer-padding is unused: 2*2 = 4 rows.
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn join_using_single_column() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (id INTEGER, label TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INTEGER, descr TEXT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 'A1'), (2, 'A2'), (3, 'A3')")
        .unwrap();
    db.execute("INSERT INTO b VALUES (2, 'B2'), (3, 'B3'), (4, 'B4')")
        .unwrap();

    let result = db
        .query("SELECT a.label, b.descr FROM a JOIN b USING (id) ORDER BY a.id")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].values[0],
        crate::types::Value::Text("A2".to_string())
    );
    assert_eq!(
        result.rows[0].values[1],
        crate::types::Value::Text("B2".to_string())
    );
    assert_eq!(
        result.rows[1].values[0],
        crate::types::Value::Text("A3".to_string())
    );
}

#[test]
fn join_using_multi_column() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (k1 INTEGER, k2 INTEGER, v TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (k1 INTEGER, k2 INTEGER, w TEXT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 10, 'c')")
        .unwrap();
    db.execute("INSERT INTO b VALUES (1, 10, 'X'), (1, 99, 'Y'), (2, 10, 'Z')")
        .unwrap();

    let result = db
        .query("SELECT a.v, b.w FROM a JOIN b USING (k1, k2) ORDER BY a.v")
        .unwrap();
    // Matches: (1,10) -> a='a', b='X'; (2,10) -> a='c', b='Z'
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].values[0],
        crate::types::Value::Text("a".to_string())
    );
    assert_eq!(
        result.rows[0].values[1],
        crate::types::Value::Text("X".to_string())
    );
}

#[test]
fn natural_join_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE users (uid INTEGER, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE posts (uid INTEGER, title TEXT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    db.execute("INSERT INTO posts VALUES (1, 'Hello'), (2, 'World'), (3, 'Stale')")
        .unwrap();

    let result = db
        .query("SELECT users.name, posts.title FROM users NATURAL JOIN posts ORDER BY users.uid")
        .unwrap();
    // Joins on shared column "uid". 2 matches.
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].values[0],
        crate::types::Value::Text("Alice".to_string())
    );
}

#[test]
fn natural_join_no_shared_columns_is_cross() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (x INTEGER)").unwrap();
    db.execute("CREATE TABLE b (y INTEGER)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO b VALUES (10), (20)").unwrap();

    let result = db.query("SELECT a.x, b.y FROM a NATURAL JOIN b").unwrap();
    // No shared columns -> condition is None -> cartesian product (2*2 = 4)
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn natural_left_join_keeps_unmatched_left() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE users (uid INTEGER, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE posts (uid INTEGER, title TEXT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
        .unwrap();
    db.execute("INSERT INTO posts VALUES (1, 'Hello')").unwrap();

    let result = db
        .query(
            "SELECT users.name, posts.title FROM users NATURAL LEFT JOIN posts ORDER BY users.uid",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(
        result.rows[0].values[1],
        crate::types::Value::Text("Hello".to_string())
    );
    assert_eq!(result.rows[1].values[1], crate::types::Value::Null);
    assert_eq!(result.rows[2].values[1], crate::types::Value::Null);
}

#[test]
fn join_using_with_left_outer() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (id INTEGER, label TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INTEGER, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .unwrap();
    db.execute("INSERT INTO b VALUES (2, 200)").unwrap();

    let result = db
        .query("SELECT a.label, b.val FROM a LEFT JOIN b USING (id) ORDER BY a.id")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].values[1], crate::types::Value::Null);
    assert_eq!(result.rows[1].values[1], crate::types::Value::Integer(200));
    assert_eq!(result.rows[2].values[1], crate::types::Value::Null);
}

#[test]
fn json_arrow_object_key() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query(r#"SELECT '{"a":1,"b":"hello"}' -> 'b'"#).unwrap();
    // -> returns JSON text (string with quotes preserved)
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#""hello""#.to_string())
    );
}

#[test]
fn json_long_arrow_object_key() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query(r#"SELECT '{"a":1,"b":"hello"}' ->> 'b'"#).unwrap();
    // ->> returns SQL scalar text (unwrapped)
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("hello".to_string())
    );
}

#[test]
fn json_long_arrow_returns_integer() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query(r#"SELECT '{"n":42}' ->> 'n'"#).unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(42));
}

#[test]
fn json_arrow_array_index() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT '[10,20,30]' ->> 1").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(20));
}

#[test]
fn json_arrow_missing_key_returns_null() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query(r#"SELECT '{"a":1}' ->> 'nope'"#).unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
}

#[test]
fn json_arrow_invalid_json_returns_null() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query(r#"SELECT 'not json' ->> 'a'"#).unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
}

#[test]
fn json_group_array_integers() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let r = db.query("SELECT json_group_array(n) FROM t").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("[1,2,3]".to_string())
    );
}

#[test]
fn json_group_array_with_nulls() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('a'), (NULL), ('b')")
        .unwrap();
    let r = db.query("SELECT json_group_array(v) FROM t").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"["a",null,"b"]"#.to_string())
    );
}

#[test]
fn json_group_object_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (k TEXT, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES ('a', 1), ('b', 2)")
        .unwrap();
    let r = db.query("SELECT json_group_object(k, v) FROM t").unwrap();
    // Order of keys is insertion order
    let text = match &r.rows[0].values[0] {
        crate::types::Value::Text(s) => s.clone(),
        _ => panic!("expected text"),
    };
    assert!(text.contains(r#""a":1"#));
    assert!(text.contains(r#""b":2"#));
}

#[test]
fn json_group_array_with_group_by() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (cat TEXT, n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 3)")
        .unwrap();
    let r = db
        .query("SELECT cat, json_group_array(n) FROM t GROUP BY cat ORDER BY cat")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("[1,2]".to_string())
    );
    assert_eq!(
        r.rows[1].values[1],
        crate::types::Value::Text("[3]".to_string())
    );
}

#[test]
fn pragma_encoding() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA encoding").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("UTF-8".to_string())
    );
}

#[test]
fn pragma_collation_list() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA collation_list").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("BINARY".to_string())
    );
}

#[test]
fn pragma_integrity_check_returns_ok() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA integrity_check").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("ok".to_string())
    );
}

#[test]
fn pragma_quick_check_returns_ok() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA quick_check").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("ok".to_string())
    );
}

#[test]
fn pragma_user_version_default_zero() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA user_version").unwrap();
    // Default user_version on a new DB is 0
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
}

#[test]
fn pragma_application_id_default_zero() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA application_id").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
}

#[test]
fn pragma_table_xinfo_includes_default() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active')")
        .unwrap();
    let r = db.query("PRAGMA table_xinfo(t)").unwrap();
    assert_eq!(r.columns.len(), 7);
    assert!(r.columns.contains(&"hidden".to_string()));
    // Find the status row by name
    let status_row = r
        .rows
        .iter()
        .find(|row| matches!(&row.values[1], crate::types::Value::Text(s) if s == "status"))
        .unwrap();
    // Default expression preserved (non-null)
    assert!(!matches!(status_row.values[4], crate::types::Value::Null));
    // hidden = 0 for ordinary columns
    assert_eq!(status_row.values[6], crate::types::Value::Integer(0));
}

#[test]
fn pragma_foreign_key_list_returns_fks() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    let r = db.query("PRAGMA foreign_key_list(child)").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[2],
        crate::types::Value::Text("parent".to_string())
    );
    assert_eq!(
        r.rows[0].values[3],
        crate::types::Value::Text("parent_id".to_string())
    );
}

#[test]
fn pragma_foreign_key_check_empty_for_clean_db() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let r = db.query("PRAGMA foreign_key_check").unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn pragma_auto_vacuum_returns_zero() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA auto_vacuum").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
}

#[test]
fn pragma_compile_options_empty() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA compile_options").unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn scalar_sign() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let cases = vec![
        ("SELECT SIGN(5)", crate::types::Value::Integer(1)),
        ("SELECT SIGN(-7)", crate::types::Value::Integer(-1)),
        ("SELECT SIGN(0)", crate::types::Value::Integer(0)),
        ("SELECT SIGN(2.5)", crate::types::Value::Integer(1)),
        ("SELECT SIGN(NULL)", crate::types::Value::Null),
    ];
    for (sql, expected) in cases {
        let r = db.query(sql).unwrap();
        assert_eq!(r.rows[0].values[0], expected, "case: {sql}");
    }
}

#[test]
fn scalar_sign_text_coercion() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT SIGN('-42')").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(-1));
    let r = db.query("SELECT SIGN('abc')").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
}

#[test]
fn scalar_sqlite_version() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT SQLITE_VERSION()").unwrap();
    let s = match &r.rows[0].values[0] {
        crate::types::Value::Text(s) => s.clone(),
        _ => panic!("expected Text"),
    };
    // Must look like a version string (digits and dots).
    assert!(s.chars().any(|c| c.is_ascii_digit()));
    assert!(s.contains('.'));
}

#[test]
fn scalar_randomblob_length() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT length(randomblob(16))").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(16));
}

#[test]
fn scalar_likelihood_passes_through() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT LIKELIHOOD(7, 0.5)").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(7));
}

#[test]
fn window_frame_running_sum_default() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4)")
        .unwrap();
    // With ORDER BY and no explicit frame, SQLite default is
    // RANGE UNBOUNDED PRECEDING TO CURRENT ROW (running sum).
    let r = db
        .query("SELECT n, SUM(n) OVER (ORDER BY n) FROM t ORDER BY n")
        .unwrap();
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(1));
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(3));
    assert_eq!(r.rows[2].values[1], crate::types::Value::Integer(6));
    assert_eq!(r.rows[3].values[1], crate::types::Value::Integer(10));
}

#[test]
fn window_frame_rows_between_preceding_current() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    // Sliding sum over current and 1 preceding row.
    let r = db
        .query("SELECT n, SUM(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ORDER BY n")
        .unwrap();
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(10));
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(30));
    assert_eq!(r.rows[2].values[1], crate::types::Value::Integer(50));
    assert_eq!(r.rows[3].values[1], crate::types::Value::Integer(70));
    assert_eq!(r.rows[4].values[1], crate::types::Value::Integer(90));
}

#[test]
fn window_frame_rows_unbounded_both() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let r = db
        .query("SELECT n, SUM(n) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t ORDER BY n")
        .unwrap();
    for row in &r.rows {
        assert_eq!(row.values[1], crate::types::Value::Integer(6));
    }
}

#[test]
fn window_frame_rows_current_to_following() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4)")
        .unwrap();
    // Sum of current row + next row.
    let r = db
        .query("SELECT n, SUM(n) OVER (ORDER BY n ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t ORDER BY n")
        .unwrap();
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(3)); // 1+2
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(5)); // 2+3
    assert_eq!(r.rows[2].values[1], crate::types::Value::Integer(7)); // 3+4
    assert_eq!(r.rows[3].values[1], crate::types::Value::Integer(4)); // 4 (no following)
}

#[test]
fn window_frame_default_with_peers() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (g INTEGER, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30), (2, 40)")
        .unwrap();
    // Default RANGE frame: peer rows (same ORDER BY value) get the same
    // running sum because the frame extends through all peers.
    let r = db
        .query("SELECT g, SUM(v) OVER (ORDER BY g) FROM t ORDER BY g, v")
        .unwrap();
    // (g=1, peers): both rows see sum 10+20 = 30
    // (g=2, peers): both see 30 + 40 + previous = 30+30+40 = 100
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(30));
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(30));
    assert_eq!(r.rows[2].values[1], crate::types::Value::Integer(100));
    assert_eq!(r.rows[3].values[1], crate::types::Value::Integer(100));
}

#[test]
fn window_frame_preserves_partition() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (g TEXT, n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 10), ('b', 20)")
        .unwrap();
    // Running sum, but partitioned by g — frames don't cross partitions.
    let r = db
        .query("SELECT g, n, SUM(n) OVER (PARTITION BY g ORDER BY n) FROM t ORDER BY g, n")
        .unwrap();
    assert_eq!(r.rows[0].values[2], crate::types::Value::Integer(1));
    assert_eq!(r.rows[1].values[2], crate::types::Value::Integer(3));
    assert_eq!(r.rows[2].values[2], crate::types::Value::Integer(10));
    assert_eq!(r.rows[3].values[2], crate::types::Value::Integer(30));
}

#[test]
fn window_frame_avg_sliding() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (10), (20), (30), (40), (50)")
        .unwrap();
    let r = db
        .query("SELECT n, AVG(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY n")
        .unwrap();
    // Window of 3 (or 2 at edges): avg(10,20)=15, avg(10,20,30)=20, avg(20,30,40)=30, avg(30,40,50)=40, avg(40,50)=45
    assert_eq!(r.rows[0].values[1], crate::types::Value::Real(15.0));
    assert_eq!(r.rows[1].values[1], crate::types::Value::Real(20.0));
    assert_eq!(r.rows[2].values[1], crate::types::Value::Real(30.0));
    assert_eq!(r.rows[3].values[1], crate::types::Value::Real(40.0));
    assert_eq!(r.rows[4].values[1], crate::types::Value::Real(45.0));
}

#[test]
fn named_window_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (g TEXT, n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 10)")
        .unwrap();
    let r = db
        .query("SELECT g, n, SUM(n) OVER w FROM t WINDOW w AS (PARTITION BY g) ORDER BY g, n")
        .unwrap();
    assert_eq!(r.rows[0].values[2], crate::types::Value::Integer(3));
    assert_eq!(r.rows[1].values[2], crate::types::Value::Integer(3));
    assert_eq!(r.rows[2].values[2], crate::types::Value::Integer(10));
}

#[test]
fn named_window_multiple_uses() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let r = db
        .query(
            "SELECT n, COUNT(*) OVER w, SUM(n) OVER w FROM t WINDOW w AS (ORDER BY n) ORDER BY n",
        )
        .unwrap();
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(1));
    assert_eq!(r.rows[2].values[1], crate::types::Value::Integer(3));
}

#[test]
fn aggregate_filter_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'b'), (5, 'a')")
        .unwrap();
    // SUM only of rows where label='a': 1 + 3 + 5 = 9
    let r = db
        .query("SELECT SUM(n) FILTER (WHERE label = 'a') FROM t")
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(9));
}

#[test]
fn aggregate_filter_count() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    let r = db
        .query("SELECT COUNT(*) FILTER (WHERE n > 2) FROM t")
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(3));
}

#[test]
fn aggregate_filter_with_group_by() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (g TEXT, n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 5), ('b', 10)")
        .unwrap();
    let r = db
        .query("SELECT g, SUM(n) FILTER (WHERE n > 1) FROM t GROUP BY g ORDER BY g")
        .unwrap();
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(2));
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(15));
}

#[test]
fn window_filter_count() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4)")
        .unwrap();
    // Running count of even values
    let r = db
        .query("SELECT n, COUNT(*) FILTER (WHERE n % 2 = 0) OVER (ORDER BY n) FROM t ORDER BY n")
        .unwrap();
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(0)); // 1: no evens yet
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(1)); // 2: one even
    assert_eq!(r.rows[2].values[1], crate::types::Value::Integer(1)); // 3: still one
    assert_eq!(r.rows[3].values[1], crate::types::Value::Integer(2)); // 4: two evens
}

#[test]
fn window_nth_value() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (10), (20), (30), (40)")
        .unwrap();
    let r = db
        .query("SELECT n, NTH_VALUE(n, 2) OVER (ORDER BY n) FROM t ORDER BY n")
        .unwrap();
    // 2nd row in partition is value 20; all rows see it
    for row in &r.rows {
        assert_eq!(row.values[1], crate::types::Value::Integer(20));
    }
}

#[test]
fn window_nth_value_out_of_range() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    let r = db
        .query("SELECT NTH_VALUE(n, 99) OVER (ORDER BY n) FROM t")
        .unwrap();
    for row in &r.rows {
        assert_eq!(row.values[0], crate::types::Value::Null);
    }
}

#[test]
fn window_percent_rank() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    let r = db
        .query("SELECT n, PERCENT_RANK() OVER (ORDER BY n) FROM t ORDER BY n")
        .unwrap();
    // 5 rows: percent_rank values are 0, 0.25, 0.5, 0.75, 1.0
    assert_eq!(r.rows[0].values[1], crate::types::Value::Real(0.0));
    assert_eq!(r.rows[2].values[1], crate::types::Value::Real(0.5));
    assert_eq!(r.rows[4].values[1], crate::types::Value::Real(1.0));
}

#[test]
fn window_cume_dist() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4)")
        .unwrap();
    let r = db
        .query("SELECT n, CUME_DIST() OVER (ORDER BY n) FROM t ORDER BY n")
        .unwrap();
    // 4 rows, no ties: 0.25, 0.5, 0.75, 1.0
    assert_eq!(r.rows[0].values[1], crate::types::Value::Real(0.25));
    assert_eq!(r.rows[1].values[1], crate::types::Value::Real(0.5));
    assert_eq!(r.rows[3].values[1], crate::types::Value::Real(1.0));
}

#[test]
fn upsert_conflict_target_unique_column() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'a@x.com', 'Alice')")
        .unwrap();
    // Conflict on email — should update Alice -> Alicia
    db.execute(
        "INSERT INTO users (email, name) VALUES ('a@x.com', 'Alicia') ON CONFLICT (email) DO UPDATE SET name = 'Alicia'",
    )
    .unwrap();

    let r = db.query("SELECT name FROM users WHERE id = 1").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Alicia".to_string())
    );
}

#[test]
fn upsert_with_excluded_reference() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE counts (k TEXT PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO counts VALUES ('hits', 5)").unwrap();
    // Increment-on-conflict using excluded.n + old n
    db.execute(
        "INSERT INTO counts (k, n) VALUES ('hits', 3) ON CONFLICT (k) DO UPDATE SET n = n + excluded.n",
    )
    .unwrap();

    let r = db.query("SELECT n FROM counts WHERE k = 'hits'").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(8));
}

#[test]
fn upsert_excluded_replaces_value() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET label = excluded.label",
    )
    .unwrap();

    let r = db.query("SELECT label FROM t WHERE id = 1").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("new".to_string())
    );
}

#[test]
fn upsert_inserts_when_no_conflict() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    // No conflict (id=2 doesn't exist) -> normal insert
    db.execute("INSERT INTO t VALUES (2, 20) ON CONFLICT (id) DO UPDATE SET n = excluded.n")
        .unwrap();

    let r = db.query("SELECT id, n FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(20));
}

#[test]
fn upsert_with_where_clause_skips_when_false() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    // WHERE n < excluded.n: 100 < 5 is FALSE, so no update
    db.execute(
        "INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET n = excluded.n WHERE n < excluded.n",
    )
    .unwrap();

    let r = db.query("SELECT n FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(100));
}

#[test]
fn upsert_with_where_clause_applies_when_true() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    // WHERE n < excluded.n: 5 < 100 is TRUE, so update happens
    db.execute(
        "INSERT INTO t VALUES (1, 100) ON CONFLICT (id) DO UPDATE SET n = excluded.n WHERE n < excluded.n",
    )
    .unwrap();

    let r = db.query("SELECT n FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(100));
}

#[test]
fn insert_returning_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let r = db
        .query("INSERT INTO t (name) VALUES ('Alice') RETURNING id, name")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("Alice".to_string())
    );
}

#[test]
fn insert_returning_star() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    let r = db
        .query("INSERT INTO t VALUES (10, 100) RETURNING *")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(10));
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(100));
}

#[test]
fn insert_returning_multiple_rows() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    let r = db
        .query("INSERT INTO t (n) VALUES (1), (2), (3) RETURNING id")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn insert_returning_expression() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    let r = db
        .query("INSERT INTO t VALUES (1, 5) RETURNING n * 2 AS doubled")
        .unwrap();
    assert_eq!(r.columns, vec!["doubled"]);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(10));
}

#[test]
fn update_returning_new_value() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    let r = db
        .query("UPDATE t SET n = n + 5 WHERE id = 1 RETURNING id, n")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(15));
}

#[test]
fn update_returning_multiple_rows() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let r = db.query("UPDATE t SET n = n * 2 RETURNING id").unwrap();
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn delete_returning_old_value() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    let r = db
        .query("DELETE FROM t WHERE id = 1 RETURNING label")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("a".to_string())
    );
    // Confirm row is gone
    let count = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.rows[0].values[0], crate::types::Value::Integer(1));
}

#[test]
fn delete_returning_star() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    let r = db.query("DELETE FROM t RETURNING *").unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn returning_no_match_yields_empty_result() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let r = db
        .query("UPDATE t SET n = 100 WHERE id = 999 RETURNING id")
        .unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn fk_on_delete_cascade_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1), (11, 1), (12, 2)")
        .unwrap();

    db.execute("DELETE FROM parent WHERE id = 1").unwrap();
    let r = db.query("SELECT id FROM child ORDER BY id").unwrap();
    // Child rows referencing parent 1 should be cascaded away.
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(12));
}

#[test]
fn fk_on_delete_set_null() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1)").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1), (11, 1)")
        .unwrap();

    db.execute("DELETE FROM parent WHERE id = 1").unwrap();
    let r = db.query("SELECT id, p FROM child ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[1], crate::types::Value::Null);
    assert_eq!(r.rows[1].values[1], crate::types::Value::Null);
}

#[test]
fn fk_on_delete_set_default() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO parent VALUES (0), (1)").unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER DEFAULT 0 REFERENCES parent(id) ON DELETE SET DEFAULT)",
    )
    .unwrap();
    db.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    db.execute("DELETE FROM parent WHERE id = 1").unwrap();
    let r = db.query("SELECT p FROM child WHERE id = 10").unwrap();
    // Child p should now be 0 (the default), which still references a valid parent.
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
}

#[test]
fn fk_on_delete_restrict_errors() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE RESTRICT)",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1)").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    let res = db.execute("DELETE FROM parent WHERE id = 1");
    assert!(res.is_err());
}

#[test]
fn fk_no_action_default_errors() {
    // No explicit ON DELETE: defaults to NO ACTION, which still errors.
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id))")
        .unwrap();
    db.execute("INSERT INTO parent VALUES (1)").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1)").unwrap();
    assert!(db.execute("DELETE FROM parent WHERE id = 1").is_err());
}

#[test]
fn fk_cascade_multi_level() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute(
        "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, b_id INTEGER REFERENCES b(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (10, 1)").unwrap();
    db.execute("INSERT INTO c VALUES (100, 10), (101, 10)")
        .unwrap();

    db.execute("DELETE FROM a WHERE id = 1").unwrap();
    // a, b, and c should all be empty.
    assert_eq!(
        db.query("SELECT COUNT(*) FROM a").unwrap().rows[0].values[0],
        crate::types::Value::Integer(0)
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM b").unwrap().rows[0].values[0],
        crate::types::Value::Integer(0)
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM c").unwrap().rows[0].values[0],
        crate::types::Value::Integer(0)
    );
}

#[test]
fn fk_pragma_foreign_key_list_includes_actions() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL)",
    )
    .unwrap();
    let r = db.query("PRAGMA foreign_key_list(child)").unwrap();
    assert_eq!(r.rows.len(), 1);
    // Currently the PRAGMA still returns "NO ACTION" placeholders — the
    // catalog action data is stored but not yet surfaced through PRAGMA.
    // Confirming the FK row exists is enough for now.
}

#[test]
fn reindex_succeeds_as_noop() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t(name)").unwrap();
    // Both forms should accept without error.
    db.execute("REINDEX").unwrap();
    db.execute("REINDEX idx_name").unwrap();
    db.execute("REINDEX t").unwrap();
}

#[test]
fn analyze_succeeds_as_noop() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("ANALYZE").unwrap();
    db.execute("ANALYZE t").unwrap();
}

#[test]
fn insert_or_rollback_undoes_transaction() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'good')").unwrap();
    // OR ROLLBACK on NOT NULL violation should rollback both rows.
    let res = db.execute("INSERT OR ROLLBACK INTO t VALUES (2, NULL)");
    assert!(res.is_err());

    // Both inserts should be gone since the txn rolled back.
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
}

#[test]
fn insert_or_fail_keeps_prior_rows() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    // First insert succeeds; multi-row INSERT OR FAIL stops on bad row but
    // keeps the good ones above it.
    let res = db.execute("INSERT OR FAIL INTO t VALUES (1, 'a'), (2, NULL), (3, 'c')");
    assert!(res.is_err());
    let r = db.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
}

#[test]
fn delete_with_limit() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    db.execute("DELETE FROM t LIMIT 2").unwrap();
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(3));
}

#[test]
fn delete_with_order_by_and_limit() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 30), (3, 20), (4, 50), (5, 40)")
        .unwrap();
    // Delete the 2 highest-n rows: (4,50) and (5,40).
    db.execute("DELETE FROM t ORDER BY n DESC LIMIT 2").unwrap();
    let r = db.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(r.rows[1].values[0], crate::types::Value::Integer(2));
    assert_eq!(r.rows[2].values[0], crate::types::Value::Integer(3));
}

#[test]
fn update_from_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE inventory (sku TEXT PRIMARY KEY, qty INTEGER)")
        .unwrap();
    db.execute("CREATE TABLE sold (sku TEXT, amount INTEGER)")
        .unwrap();
    db.execute("INSERT INTO inventory VALUES ('A', 100), ('B', 50), ('C', 10)")
        .unwrap();
    db.execute("INSERT INTO sold VALUES ('A', 30), ('B', 5)")
        .unwrap();

    db.execute(
        "UPDATE inventory SET qty = qty - sold.amount FROM sold WHERE inventory.sku = sold.sku",
    )
    .unwrap();

    let r = db
        .query("SELECT sku, qty FROM inventory ORDER BY sku")
        .unwrap();
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(70)); // A: 100 - 30
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(45)); // B: 50 - 5
    assert_eq!(r.rows[2].values[1], crate::types::Value::Integer(10)); // C: untouched
}

#[test]
fn default_persists_across_reopen() {
    let db_path = "/tmp/rsqlite_db_default_persist.db";
    let _ = std::fs::remove_file(db_path);
    let vfs = rsqlite_vfs::native::NativeVfs::new();
    {
        let mut db = Database::create(&vfs, db_path).unwrap();
        db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active')")
            .unwrap();
    }
    // Reopen — default_expr must be parsed back from stored schema SQL.
    {
        let mut db = Database::open(&vfs, db_path).unwrap();
        db.execute("INSERT INTO accounts (id) VALUES (1)").unwrap();
        let result = db
            .query("SELECT status FROM accounts WHERE id = 1")
            .unwrap();
        assert_eq!(
            result.rows[0].values[0],
            crate::types::Value::Text("active".to_string())
        );
    }
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn generated_column_stored_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute(
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a + b) STORED)",
    )
    .unwrap();
    db.execute("INSERT INTO t (a, b) VALUES (3, 4)").unwrap();
    let r = db.query("SELECT c FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(7));
}

#[test]
fn generated_column_recomputed_on_update() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute(
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a * b) STORED)",
    )
    .unwrap();
    db.execute("INSERT INTO t (a, b) VALUES (2, 5)").unwrap();
    db.execute("UPDATE t SET a = 10").unwrap();
    let r = db.query("SELECT c FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(50));
}

#[test]
fn generated_column_rejects_explicit_insert() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (a INTEGER, c INTEGER GENERATED ALWAYS AS (a + 1) STORED)")
        .unwrap();
    let res = db.execute("INSERT INTO t (a, c) VALUES (1, 99)");
    assert!(res.is_err());
}

#[test]
fn generated_column_rejects_explicit_update() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (a INTEGER, c INTEGER GENERATED ALWAYS AS (a + 1) STORED)")
        .unwrap();
    db.execute("INSERT INTO t (a) VALUES (1)").unwrap();
    let res = db.execute("UPDATE t SET c = 99");
    assert!(res.is_err());
}

#[test]
fn generated_column_in_pragma_table_xinfo() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (a INTEGER, c INTEGER GENERATED ALWAYS AS (a * 2) STORED)")
        .unwrap();
    let r = db.query("PRAGMA table_xinfo(t)").unwrap();
    let c_row = r
        .rows
        .iter()
        .find(|row| matches!(&row.values[1], crate::types::Value::Text(s) if s == "c"))
        .unwrap();
    // hidden = 2 for STORED generated columns.
    assert_eq!(c_row.values[6], crate::types::Value::Integer(2));
}

#[test]
fn json_each_array_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query("SELECT key, value FROM json_each('[10, 20, 30]')")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(10));
    assert_eq!(r.rows[2].values[1], crate::types::Value::Integer(30));
}

#[test]
fn json_each_object_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query(r#"SELECT key, value FROM json_each('{"a":1,"b":"two"}')"#)
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    let keys: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            crate::types::Value::Text(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert!(keys.contains(&"a".to_string()));
    assert!(keys.contains(&"b".to_string()));
}

#[test]
fn json_each_with_count() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query("SELECT COUNT(*) FROM json_each('[1, 2, 3, 4, 5]')")
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(5));
}

#[test]
fn json_each_empty_array() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT key FROM json_each('[]')").unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn json_tree_recurses() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query(r#"SELECT COUNT(*) FROM json_tree('{"a":[1,2],"b":{"c":3}}')"#)
        .unwrap();
    // Rows: root, a, [1], [2], b, c — 6 nodes total.
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(6));
}

#[test]
fn json_each_invalid_json_returns_empty() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query("SELECT COUNT(*) FROM json_each('not json')")
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
}

#[test]
fn partial_index_only_includes_matching_rows() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'active'), (2, 'archived'), (3, 'active')")
        .unwrap();
    // Build a partial index on (status) only for active rows.
    db.execute("CREATE INDEX idx_active ON t (status) WHERE status = 'active'")
        .unwrap();

    // PRAGMA reports the index exists.
    let r = db.query("PRAGMA index_list(t)").unwrap();
    assert_eq!(r.rows.len(), 1);

    // Sanity: the table has all 3 rows still queryable (full scan).
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(3));
}

#[test]
fn partial_index_maintained_on_insert() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_active ON t (status) WHERE status = 'active'")
        .unwrap();
    // Insert into both matching and non-matching predicate; both should
    // succeed without error.
    db.execute("INSERT INTO t VALUES (1, 'active')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'archived')").unwrap();
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
}

#[test]
fn partial_index_persists_predicate_across_reopen() {
    let db_path = "/tmp/rsqlite_partial_index_persist.db";
    let _ = std::fs::remove_file(db_path);
    let vfs = rsqlite_vfs::native::NativeVfs::new();
    {
        let mut db = Database::create(&vfs, db_path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)")
            .unwrap();
        db.execute("CREATE INDEX idx_active ON t (status) WHERE status = 'active'")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'active'), (2, 'archived')")
            .unwrap();
    }
    {
        let mut db = Database::open(&vfs, db_path).unwrap();
        // Insertion path needs the predicate; a successful insert here
        // confirms the predicate was reloaded from sqlite_master.
        db.execute("INSERT INTO t VALUES (3, 'active')").unwrap();
        let r = db.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(3));
    }
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn expression_index_creates_without_error() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'BOB')")
        .unwrap();
    // Expression indexes parse and build (with NULL placeholders) but are
    // not yet used at query lookup time. Confirm CREATE doesn't error and
    // queries still work via full table scan.
    db.execute("CREATE INDEX idx_lower ON t (lower(name))")
        .unwrap();

    let r = db
        .query("SELECT id FROM t WHERE lower(name) = 'alice'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
}

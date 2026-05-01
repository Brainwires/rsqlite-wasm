use super::*;

// --- Error handling tests ---

#[test]
fn error_table_not_found() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT * FROM nonexistent");
    assert!(r.is_err());
    assert!(r.unwrap_err().to_string().contains("not found"));
}

#[test]
fn error_column_not_found() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let r = db.query("SELECT nonexistent FROM t");
    assert!(r.is_err());
    assert!(r.unwrap_err().to_string().contains("unknown column"));
}

#[test]
fn error_insert_named_nonexistent_column() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let r = db.execute("INSERT INTO t (id, nonexistent) VALUES (1, 'x')");
    assert!(r.is_err());
}

#[test]
fn error_drop_nonexistent_table() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.execute("DROP TABLE nonexistent");
    assert!(r.is_err());
    // But IF EXISTS should succeed
    let r = db.execute("DROP TABLE IF EXISTS nonexistent");
    assert!(r.is_ok());
}

#[test]
fn error_duplicate_table() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let r = db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    assert!(r.is_err());
    // IF NOT EXISTS should succeed
    let r = db.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)");
    assert!(r.is_ok());
}

#[test]
fn error_division_by_zero() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT 10 / 0").unwrap();
    // SQLite returns 0 for integer division by zero
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
}

// --- NULL handling edge cases ---

#[test]
fn null_comparison_semantics() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    // NULL = NULL should be NULL (falsy), not TRUE
    let r = db.query("SELECT NULL = NULL").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);

    let r = db.query("SELECT NULL IS NULL").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));

    let r = db.query("SELECT NULL IS NOT NULL").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
}

#[test]
fn null_in_aggregate() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let r = db.query("SELECT COUNT(val) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2)); // NULLs excluded

    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(3)); // NULLs counted

    let r = db.query("SELECT SUM(val) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(40)); // NULLs skipped

    let r = db.query("SELECT AVG(val) FROM t").unwrap();
    if let crate::types::Value::Real(f) = &r.rows[0].values[0] {
        assert!((*f - 20.0).abs() < 0.001); // 40/2 = 20
    }
}

#[test]
fn null_in_order_by() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 5)").unwrap();

    let r = db.query("SELECT val FROM t ORDER BY val").unwrap();
    // SQLite sorts NULLs first (smallest)
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
    assert_eq!(r.rows[1].values[0], crate::types::Value::Integer(5));
    assert_eq!(r.rows[2].values[0], crate::types::Value::Integer(10));
}

// --- Type affinity tests ---

#[test]
fn affinity_text_integer_comparison() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    // In SQLite, integers sort before text
    let r = db.query("SELECT 1 < 'a'").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
}

// --- CAST edge cases ---

#[test]
fn cast_edge_cases() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT CAST(NULL AS INTEGER)").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);

    let r = db.query("SELECT CAST('hello' AS INTEGER)").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));

    let r = db.query("SELECT CAST(3.14 AS INTEGER)").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(3));

    let r = db.query("SELECT CAST(42 AS TEXT)").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("42".into()));
}

// --- Transaction edge cases ---

#[test]
fn rollback_restores_data() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("UPDATE t SET val = 'modified' WHERE id = 1")
        .unwrap();
    let r = db.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("modified".into())
    );
    db.execute("ROLLBACK").unwrap();

    let r = db.query("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("original".into())
    );
}

// --- View edge cases ---

#[test]
fn view_with_aggregation_complex() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount INTEGER)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (1, 'A', 100)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (2, 'B', 200)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (3, 'A', 150)")
        .unwrap();
    db.execute("CREATE VIEW product_totals AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product").unwrap();

    let r = db
        .query("SELECT * FROM product_totals ORDER BY product")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("A".into()));
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(250));
}

// --- Large data test ---

#[test]
fn insert_100_rows_and_query() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();

    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 2))
            .unwrap();
    }

    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(100));

    let r = db.query("SELECT val FROM t WHERE id = 50").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(100));

    let r = db.query("SELECT MAX(val) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(198));
}

// --- LIKE edge cases ---

#[test]
fn like_case_sensitivity() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    // SQLite LIKE is case-insensitive for ASCII
    let r = db.query("SELECT 'Hello' LIKE 'hello'").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));

    let r = db.query("SELECT 'Hello' LIKE 'HELLO'").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));

    let r = db.query("SELECT 'Hello' LIKE 'h%'").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
}

// --- CTE edge cases ---

#[test]
fn cte_with_multiple_ctes() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    // Test multiple CTEs in sequence
    let r = db
        .query(
            "WITH doubled AS (SELECT id, val * 2 AS dval FROM t), \
         tripled AS (SELECT id, val * 3 AS tval FROM t) \
         SELECT d.dval FROM doubled AS d WHERE d.id = 1",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(20));
}

// --- Index edge cases ---

#[test]
fn index_with_null_values() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t(name)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'Bob')").unwrap();

    let r = db.query("SELECT id FROM t WHERE name = 'Alice'").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));

    let r = db
        .query("SELECT COUNT(*) FROM t WHERE name IS NULL")
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
}

#[test]
fn index_after_delete() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();

    let r = db.query("SELECT id FROM t WHERE val = 'b'").unwrap();
    assert_eq!(r.rows.len(), 0);

    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
}

// ───────────────────── JSON function tests ─────────────────────

fn setup_json_db() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    Database::create(&vfs, "test.db").unwrap()
}

#[test]
fn json_valid_and_parse() {
    let mut db = setup_json_db();
    let r = db.query("SELECT json_valid('{\"a\":1}')").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));

    let r = db.query("SELECT json_valid('not json')").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));

    let r = db.query("SELECT json_valid(NULL)").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
}

#[test]
fn json_extract_basic() {
    let mut db = setup_json_db();
    let r = db
        .query(r#"SELECT json_extract('{"name":"Alice","age":30}', '$.name')"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Alice".into())
    );

    let r = db
        .query(r#"SELECT json_extract('{"name":"Alice","age":30}', '$.age')"#)
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(30));

    let r = db
        .query(r#"SELECT json_extract('{"a":{"b":[10,20,30]}}', '$.a.b[1]')"#)
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(20));
}

#[test]
fn json_extract_missing_path() {
    let mut db = setup_json_db();
    let r = db
        .query(r#"SELECT json_extract('{"a":1}', '$.b')"#)
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
}

#[test]
fn json_type_function() {
    let mut db = setup_json_db();
    let r = db.query(r#"SELECT json_type('{"a":1}')"#).unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("object".into())
    );

    let r = db.query(r#"SELECT json_type('[1,2]')"#).unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("array".into())
    );

    let r = db.query(r#"SELECT json_type('{"a":1}', '$.a')"#).unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("real".into())
    );

    let r = db
        .query(r#"SELECT json_type('{"a":"hello"}', '$.a')"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("text".into())
    );
}

#[test]
fn json_minify() {
    let mut db = setup_json_db();
    let r = db
        .query(r#"SELECT json('  { "a" : 1 , "b" : [2, 3] }  ')"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"a":1,"b":[2,3]}"#.into())
    );
}

#[test]
fn json_array_function() {
    let mut db = setup_json_db();
    let r = db.query("SELECT json_array(1, 'hello', NULL)").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"[1,"hello",null]"#.into())
    );

    let r = db.query("SELECT json_array()").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("[]".into()));
}

#[test]
fn json_object_function() {
    let mut db = setup_json_db();
    let r = db
        .query("SELECT json_object('name', 'Alice', 'age', 30)")
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"name":"Alice","age":30}"#.into())
    );
}

#[test]
fn json_array_length_function() {
    let mut db = setup_json_db();
    let r = db.query("SELECT json_array_length('[1,2,3,4]')").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(4));

    let r = db.query("SELECT json_array_length('[]')").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));

    let r = db
        .query(r#"SELECT json_array_length('{"a":[10,20]}', '$.a')"#)
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
}

#[test]
fn json_quote_function() {
    let mut db = setup_json_db();
    let r = db.query("SELECT json_quote('hello')").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#""hello""#.into())
    );

    let r = db.query("SELECT json_quote(42)").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("42".into()));

    let r = db.query("SELECT json_quote(NULL)").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("null".into())
    );
}

#[test]
fn json_with_table_data() {
    let mut db = setup_json_db();
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    db.execute(r#"INSERT INTO docs VALUES (1, '{"name":"Alice","scores":[95,87,92]}')"#)
        .unwrap();
    db.execute(r#"INSERT INTO docs VALUES (2, '{"name":"Bob","scores":[78,85,90]}')"#)
        .unwrap();

    let r = db
        .query("SELECT id, json_extract(data, '$.name') AS name FROM docs ORDER BY id")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("Alice".into())
    );
    assert_eq!(r.rows[1].values[1], crate::types::Value::Text("Bob".into()));

    let r = db
        .query("SELECT id, json_extract(data, '$.scores[0]') AS top FROM docs ORDER BY id")
        .unwrap();
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(95));
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(78));
}

#[test]
fn json_extract_nested_object() {
    let mut db = setup_json_db();
    let r = db
        .query(r#"SELECT json_extract('{"user":{"addr":{"city":"NYC"}}}', '$.user.addr.city')"#)
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("NYC".into()));
}

#[test]
fn json_extract_returns_subobject() {
    let mut db = setup_json_db();
    let r = db
        .query(r#"SELECT json_extract('{"a":{"b":1}}', '$.a')"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"b":1}"#.into())
    );
}

#[test]
fn json_invalid_input() {
    let mut db = setup_json_db();
    let r = db.query("SELECT json('not valid json')");
    assert!(r.is_err());

    let r = db.query(r#"SELECT json_extract('not json', '$.a')"#);
    assert!(r.is_err());
}

#[test]
fn json_insert_function() {
    let mut db = setup_json_db();
    let r = db
        .query(r#"SELECT json_insert('{"a":1}', '$.b', 2)"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"a":1,"b":2}"#.into())
    );

    // json_insert does NOT replace existing keys
    let r = db
        .query(r#"SELECT json_insert('{"a":1}', '$.a', 99)"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"a":1}"#.into())
    );
}

#[test]
fn json_replace_function() {
    let mut db = setup_json_db();
    let r = db
        .query(r#"SELECT json_replace('{"a":1}', '$.a', 99)"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"a":99}"#.into())
    );

    // json_replace does NOT insert new keys
    let r = db
        .query(r#"SELECT json_replace('{"a":1}', '$.b', 2)"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"a":1}"#.into())
    );
}

#[test]
fn json_set_function() {
    let mut db = setup_json_db();
    // json_set inserts AND replaces
    let r = db
        .query(r#"SELECT json_set('{"a":1}', '$.a', 99)"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"a":99}"#.into())
    );

    let r = db.query(r#"SELECT json_set('{"a":1}', '$.b', 2)"#).unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"a":1,"b":2}"#.into())
    );
}

#[test]
fn json_remove_function() {
    let mut db = setup_json_db();
    let r = db
        .query(r#"SELECT json_remove('{"a":1,"b":2}', '$.a')"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"b":2}"#.into())
    );

    let r = db.query("SELECT json_remove('[1,2,3]', '$[1]')").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("[1,3]".into())
    );
}

#[test]
fn json_patch_function() {
    let mut db = setup_json_db();
    let r = db
        .query(r#"SELECT json_patch('{"a":1,"b":2}', '{"b":3,"c":4}')"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"a":1,"b":3,"c":4}"#.into())
    );

    // null in patch removes key
    let r = db
        .query(r#"SELECT json_patch('{"a":1,"b":2}', '{"a":null}')"#)
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text(r#"{"b":2}"#.into())
    );
}

// ───────────────────── NOT NULL constraint tests ─────────────────────

#[test]
fn not_null_insert_violation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    let r = db.execute("INSERT INTO t (id) VALUES (1)");
    assert!(r.is_err());
    let err = r.unwrap_err().to_string();
    assert!(
        err.contains("NOT NULL"),
        "Expected NOT NULL error, got: {err}"
    );
}

#[test]
fn not_null_insert_explicit_null() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    let r = db.execute("INSERT INTO t VALUES (1, NULL)");
    assert!(r.is_err());
    let err = r.unwrap_err().to_string();
    assert!(
        err.contains("NOT NULL"),
        "Expected NOT NULL error, got: {err}"
    );
}

#[test]
fn not_null_insert_valid() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    let r = db.query("SELECT name FROM t").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Alice".into())
    );
}

#[test]
fn not_null_update_violation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    let r = db.execute("UPDATE t SET name = NULL WHERE id = 1");
    assert!(r.is_err());
    let err = r.unwrap_err().to_string();
    assert!(
        err.contains("NOT NULL"),
        "Expected NOT NULL error, got: {err}"
    );
}

#[test]
fn not_null_nullable_column_allows_null() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL, NULL)").unwrap();
    let r = db.query("SELECT name, email FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
    assert_eq!(r.rows[0].values[1], crate::types::Value::Null);
}

// ───────────────────── UNIQUE constraint tests ─────────────────────

#[test]
fn unique_insert_violation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')")
        .unwrap();
    let r = db.execute("INSERT INTO t VALUES (2, 'alice@test.com')");
    assert!(r.is_err());
    let err = r.unwrap_err().to_string();
    assert!(err.contains("UNIQUE"), "Expected UNIQUE error, got: {err}");
}

#[test]
fn unique_insert_different_values_ok() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'bob@test.com')")
        .unwrap();
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
}

#[test]
fn unique_null_allowed_multiple() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
}

#[test]
fn unique_update_violation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'bob@test.com')")
        .unwrap();
    let r = db.execute("UPDATE t SET email = 'alice@test.com' WHERE id = 2");
    assert!(r.is_err());
    let err = r.unwrap_err().to_string();
    assert!(err.contains("UNIQUE"), "Expected UNIQUE error, got: {err}");
}

#[test]
fn unique_update_same_row_ok() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice@test.com')")
        .unwrap();
    db.execute("UPDATE t SET email = 'alice@test.com' WHERE id = 1")
        .unwrap();
    let r = db.query("SELECT email FROM t").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("alice@test.com".into())
    );
}

// ───────────────────── CHECK constraint tests ─────────────────────

#[test]
fn check_constraint_insert_violation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 25)").unwrap();
    let r = db.execute("INSERT INTO t VALUES (2, -5)");
    assert!(r.is_err());
    let err = r.unwrap_err().to_string();
    assert!(err.contains("CHECK"), "Expected CHECK error, got: {err}");
}

#[test]
fn check_constraint_insert_valid() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 100)").unwrap();
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
}

#[test]
fn check_constraint_update_violation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 25)").unwrap();
    let r = db.execute("UPDATE t SET age = -1 WHERE id = 1");
    assert!(r.is_err());
    let err = r.unwrap_err().to_string();
    assert!(err.contains("CHECK"), "Expected CHECK error, got: {err}");
}

#[test]
fn check_constraint_null_passes() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let r = db.query("SELECT age FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
}

#[test]
fn check_constraint_table_level() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, lo INTEGER, hi INTEGER, CHECK(lo <= hi))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();
    let r = db.execute("INSERT INTO t VALUES (2, 30, 20)");
    assert!(r.is_err());
    let err = r.unwrap_err().to_string();
    assert!(err.contains("CHECK"), "Expected CHECK error, got: {err}");
}

// ───────────────────── EXPLAIN QUERY PLAN tests ─────────────────────

#[test]
fn explain_query_plan_scan() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let r = db.query("EXPLAIN QUERY PLAN SELECT * FROM users").unwrap();
    assert_eq!(r.columns, vec!["id", "parent", "notused", "detail"]);
    assert!(!r.rows.is_empty());
    let detail = &r.rows[0].values[3];
    if let crate::types::Value::Text(s) = detail {
        assert!(
            s.contains("SCAN TABLE users"),
            "Expected SCAN TABLE users, got: {s}"
        );
    } else {
        panic!("Expected text detail");
    }
}

#[test]
fn explain_query_plan_index() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON users(name)").unwrap();
    let r = db
        .query("EXPLAIN QUERY PLAN SELECT * FROM users WHERE name = 'Alice'")
        .unwrap();
    assert!(!r.rows.is_empty());
    let detail = &r.rows[0].values[3];
    if let crate::types::Value::Text(s) = detail {
        assert!(
            s.contains("SEARCH TABLE") || s.contains("SCAN TABLE"),
            "got: {s}"
        );
    } else {
        panic!("Expected text detail");
    }
}

#[test]
fn explain_query_plan_join() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)")
        .unwrap();
    let r = db
        .query("EXPLAIN QUERY PLAN SELECT * FROM a INNER JOIN b ON a.id = b.a_id")
        .unwrap();
    let details: Vec<String> = r
        .rows
        .iter()
        .filter_map(|row| {
            if let crate::types::Value::Text(s) = &row.values[3] {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();
    assert!(
        details.iter().any(|d| d.contains("JOIN")),
        "Expected JOIN in plan: {details:?}"
    );
}

// ───────────────────── SAVEPOINT tests ─────────────────────

#[test]
fn savepoint_rollback_to() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    db.execute("ROLLBACK TO sp1").unwrap();

    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));

    db.execute("COMMIT").unwrap();
    let r = db.query("SELECT val FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("a".into()));
}

#[test]
fn savepoint_release() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("RELEASE sp1").unwrap();
    db.execute("COMMIT").unwrap();

    let r = db.query("SELECT val FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("a".into()));
}

#[test]
fn savepoint_nested() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    db.execute("SAVEPOINT sp2").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    db.execute("ROLLBACK TO sp2").unwrap();

    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));

    db.execute("ROLLBACK TO sp1").unwrap();
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));

    db.execute("COMMIT").unwrap();
}

#[test]
fn savepoint_without_begin() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    db.execute("ROLLBACK TO sp1").unwrap();

    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
}

// ── COLLATE NOCASE tests ──

#[test]
fn collate_nocase_where_eq() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'BOB')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'charlie')").unwrap();

    let r = db
        .query("SELECT id FROM t WHERE name = 'alice' COLLATE NOCASE")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));

    let r = db
        .query("SELECT id FROM t WHERE name COLLATE NOCASE = 'bob'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
}

#[test]
fn collate_nocase_order_by() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'banana')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Apple')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'CHERRY')").unwrap();

    let r = db
        .query("SELECT name FROM t ORDER BY name COLLATE NOCASE")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0].values[0], Value::Text("Apple".into()));
    assert_eq!(r.rows[1].values[0], Value::Text("banana".into()));
    assert_eq!(r.rows[2].values[0], Value::Text("CHERRY".into()));
}

#[test]
fn collate_nocase_comparison_operators() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alpha')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'beta')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'GAMMA')").unwrap();

    let r = db
        .query("SELECT id FROM t WHERE name < 'beta' COLLATE NOCASE ORDER BY id")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));

    let r = db
        .query("SELECT id FROM t WHERE name >= 'beta' COLLATE NOCASE ORDER BY id")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
    assert_eq!(r.rows[1].values[0], Value::Integer(3));
}

#[test]
fn collate_nocase_in_list() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

    let r = db
        .query("SELECT id FROM t WHERE name COLLATE NOCASE IN ('alice', 'bob') ORDER BY id")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
}

// ── AUTOINCREMENT tests ──

#[test]
fn autoincrement_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (name) VALUES ('Alice')").unwrap();
    db.execute("INSERT INTO t (name) VALUES ('Bob')").unwrap();

    let r = db.query("SELECT id, name FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[1].values[0], Value::Integer(2));
}

#[test]
fn autoincrement_no_reuse_after_delete() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (val) VALUES ('a')").unwrap();
    db.execute("INSERT INTO t (val) VALUES ('b')").unwrap();
    db.execute("INSERT INTO t (val) VALUES ('c')").unwrap();
    db.execute("DELETE FROM t WHERE id = 3").unwrap();
    db.execute("INSERT INTO t (val) VALUES ('d')").unwrap();

    let r = db.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows[2].values[0], Value::Integer(4));
}

#[test]
fn autoincrement_explicit_id() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (id, val) VALUES (100, 'a')")
        .unwrap();
    db.execute("INSERT INTO t (val) VALUES ('b')").unwrap();

    let r = db.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(100));
    assert_eq!(r.rows[1].values[0], Value::Integer(101));
}

#[test]
fn autoincrement_sqlite_sequence_table() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (val) VALUES ('a')").unwrap();
    db.execute("INSERT INTO t (val) VALUES ('b')").unwrap();

    let r = db
        .query("SELECT name, seq FROM sqlite_sequence WHERE name = 't'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("t".into()));
    assert_eq!(r.rows[0].values[1], Value::Integer(2));
}

// ── FOREIGN KEY tests ──

#[test]
fn foreign_key_insert_valid() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1, 'Alice')")
        .unwrap();
    db.execute("INSERT INTO child VALUES (1, 1)").unwrap();

    let r = db.query("SELECT * FROM child").unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn foreign_key_insert_violation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();

    let result = db.execute("INSERT INTO child VALUES (1, 999)");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("FOREIGN KEY"));
}

#[test]
fn foreign_key_delete_violation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1, 'Alice')")
        .unwrap();
    db.execute("INSERT INTO child VALUES (1, 1)").unwrap();

    let result = db.execute("DELETE FROM parent WHERE id = 1");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("FOREIGN KEY"));
}

#[test]
fn foreign_key_off_by_default() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    // Should succeed even without a parent row since FK enforcement is off
    db.execute("INSERT INTO child VALUES (1, 999)").unwrap();
}

#[test]
fn foreign_key_null_allowed() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
    )
    .unwrap();
    // NULL FK values should be allowed (SQL standard behavior)
    db.execute("INSERT INTO child VALUES (1, NULL)").unwrap();

    let r = db.query("SELECT * FROM child").unwrap();
    assert_eq!(r.rows.len(), 1);
}

// ── Recursive CTE tests ──

#[test]
fn recursive_cte_counting() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query(
            "
        WITH RECURSIVE cnt(x) AS (
            SELECT 1
            UNION ALL
            SELECT x + 1 FROM cnt WHERE x < 5
        )
        SELECT x FROM cnt
    ",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[4].values[0], Value::Integer(5));
}

#[test]
fn recursive_cte_hierarchy() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (1, 'CEO', NULL)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (2, 'VP', 1)").unwrap();
    db.execute("INSERT INTO emp VALUES (3, 'Director', 2)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (4, 'Manager', 3)")
        .unwrap();

    let r = db
        .query(
            "
        WITH RECURSIVE chain(id, name, level) AS (
            SELECT id, name, 0 FROM emp WHERE id = 1
            UNION ALL
            SELECT e.id, e.name, c.level + 1
            FROM emp e JOIN chain c ON e.manager_id = c.id
        )
        SELECT name, level FROM chain ORDER BY level
    ",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0].values[0], Value::Text("CEO".into()));
    assert_eq!(r.rows[0].values[1], Value::Integer(0));
    assert_eq!(r.rows[3].values[0], Value::Text("Manager".into()));
    assert_eq!(r.rows[3].values[1], Value::Integer(3));
}

#[test]
fn recursive_cte_fibonacci() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query(
            "
        WITH RECURSIVE fib(a, b) AS (
            SELECT 0, 1
            UNION ALL
            SELECT b, a + b FROM fib WHERE b < 100
        )
        SELECT a FROM fib
    ",
        )
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(0));
    assert_eq!(r.rows[1].values[0], Value::Integer(1));
    assert_eq!(r.rows[2].values[0], Value::Integer(1));
    assert_eq!(r.rows[3].values[0], Value::Integer(2));
    assert_eq!(r.rows[4].values[0], Value::Integer(3));
    assert_eq!(r.rows[5].values[0], Value::Integer(5));
}

#[test]
fn recursive_cte_with_limit() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query(
            "
        WITH RECURSIVE cnt(x) AS (
            SELECT 1
            UNION ALL
            SELECT x + 1 FROM cnt WHERE x < 100
        )
        SELECT x FROM cnt LIMIT 10
    ",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 10);
    assert_eq!(r.rows[9].values[0], Value::Integer(10));
}

#[test]
fn pragma_journal_mode_read() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA journal_mode").unwrap();
    assert_eq!(r.columns, vec!["journal_mode"]);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("delete".to_string()));
}

#[test]
fn pragma_journal_mode_set_wal() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("PRAGMA journal_mode = WAL").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("delete".to_string()));
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r2 = db.query("SELECT * FROM t").unwrap();
    assert_eq!(r2.rows.len(), 1);
}

#[test]
fn vacuum_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    for i in 1..=500 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, '{}')",
            "x".repeat(100)
        ))
        .unwrap();
    }
    let pages_before = db.page_count();
    db.execute("DELETE FROM t WHERE id > 10").unwrap();
    db.execute("VACUUM").unwrap();
    let pages_after = db.page_count();
    assert!(
        pages_after < pages_before,
        "VACUUM should reduce page count: before={pages_before}, after={pages_after}"
    );
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(10));
    let r = db.query("SELECT * FROM t WHERE id = 5").unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn vacuum_empty_db() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("VACUUM").unwrap();
}

#[test]
fn vacuum_preserves_indexes() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t(name)").unwrap();
    for i in 1..=50 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'name_{i}')"))
            .unwrap();
    }
    db.execute("DELETE FROM t WHERE id > 5").unwrap();
    db.execute("VACUUM").unwrap();
    let r = db.query("SELECT * FROM t WHERE name = 'name_3'").unwrap();
    assert_eq!(r.rows.len(), 1);
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(5));
}

#[test]
fn vacuum_fails_in_transaction() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("BEGIN").unwrap();
    let result = db.execute("VACUUM");
    assert!(result.is_err());
    db.execute("ROLLBACK").unwrap();
}

#[test]
fn trigger_create_drop() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE log (msg TEXT)").unwrap();
    db.execute("CREATE TRIGGER t_ins AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES ('inserted'); END;").unwrap();
    db.execute("DROP TRIGGER t_ins").unwrap();
}

#[test]
fn trigger_after_insert() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE log (msg TEXT)").unwrap();
    db.execute("CREATE TRIGGER t_ins AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES ('row inserted'); END;").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    let r = db.query("SELECT * FROM log").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Text("row inserted".to_string()));
}

#[test]
fn trigger_after_insert_with_new() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE log (tid INTEGER, tname TEXT)")
        .unwrap();
    db.execute("CREATE TRIGGER t_ins AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (NEW.id, NEW.name); END;").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    let r = db.query("SELECT * FROM log").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[0].values[1], Value::Text("Alice".to_string()));
}

#[test]
fn trigger_after_delete_with_old() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE log (tid INTEGER, tname TEXT)")
        .unwrap();
    db.execute("CREATE TRIGGER t_del AFTER DELETE ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.id, OLD.name); END;").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    let r = db.query("SELECT * FROM log").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[0].values[1], Value::Text("Alice".to_string()));
}

#[test]
fn trigger_after_update_old_new() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE log (old_name TEXT, new_name TEXT)")
        .unwrap();
    db.execute("CREATE TRIGGER t_upd AFTER UPDATE ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (OLD.name, NEW.name); END;").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("UPDATE t SET name = 'Bob' WHERE id = 1")
        .unwrap();
    let r = db.query("SELECT * FROM log").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("Alice".to_string()));
    assert_eq!(r.rows[0].values[1], Value::Text("Bob".to_string()));
}

#[test]
fn trigger_when_condition() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    db.execute("CREATE TABLE log (msg TEXT)").unwrap();
    db.execute("CREATE TRIGGER t_ins AFTER INSERT ON t FOR EACH ROW WHEN NEW.val > 10 BEGIN INSERT INTO log VALUES ('big value'); END;").unwrap();
    db.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 15)").unwrap();
    let r = db.query("SELECT * FROM log").unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn trigger_persistence() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    {
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("CREATE TABLE log (msg TEXT)").unwrap();
        db.execute("CREATE TRIGGER t_ins AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES ('inserted'); END;").unwrap();
    }
    {
        let mut db = Database::open(&vfs, "test.db").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        let r = db.query("SELECT * FROM log").unwrap();
        assert_eq!(r.rows.len(), 1);
    }
}

#[test]
fn trigger_drop_if_exists() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("DROP TRIGGER IF EXISTS nonexistent").unwrap();
}

#[test]
fn attach_and_query() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    {
        let mut other = Database::create(&vfs, "other.db").unwrap();
        other
            .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        other
            .execute("INSERT INTO items VALUES (1, 'Widget')")
            .unwrap();
        other
            .execute("INSERT INTO items VALUES (2, 'Gadget')")
            .unwrap();
    }
    let mut db = Database::create(&vfs, "main.db").unwrap();
    db.execute("ATTACH DATABASE 'other.db' AS aux").unwrap();
    let r = db.query("PRAGMA database_list").unwrap();
    let names: Vec<String> = r
        .rows
        .iter()
        .map(|row| {
            if let Value::Text(s) = &row.values[1] {
                s.clone()
            } else {
                String::new()
            }
        })
        .collect();
    assert!(names.contains(&"main".to_string()));
    assert!(names.contains(&"aux".to_string()));
}

#[test]
fn attach_detach_roundtrip() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    {
        let mut other = Database::create(&vfs, "other.db").unwrap();
        other
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
    }
    let mut db = Database::create(&vfs, "main.db").unwrap();
    db.execute("ATTACH DATABASE 'other.db' AS aux").unwrap();
    let r = db.query("PRAGMA database_list").unwrap();
    assert_eq!(r.rows.len(), 2);
    db.execute("DETACH aux").unwrap();
    let r = db.query("PRAGMA database_list").unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn detach_nonexistent_error() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "main.db").unwrap();
    assert!(db.execute("DETACH nonexistent").is_err());
}

#[test]
fn attach_reserved_name_error() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "main.db").unwrap();
    assert!(db.execute("ATTACH DATABASE 'other.db' AS main").is_err());
}

#[test]
fn attach_duplicate_error() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    {
        let mut other = Database::create(&vfs, "other.db").unwrap();
        other
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
    }
    let mut db = Database::create(&vfs, "main.db").unwrap();
    db.execute("ATTACH DATABASE 'other.db' AS aux").unwrap();
    assert!(db.execute("ATTACH DATABASE 'other.db' AS aux").is_err());
}

// ── Composite PRIMARY KEY uniqueness ─────────────────────────────────
//
// Regression: composite-PK member columns used to be marked individually
// `is_unique`, so an INSERT that reused only one of the PK column values
// would falsely trip "UNIQUE constraint failed: t.<col>" even though the
// (col_a, col_b) tuple was still distinct. The chat-pwa caught this with
// `messages(conversation_id, message_id)` — second message in a
// conversation rejected because conversation_id repeated. The fix tracks
// `pk_columns` on TableDef and only enforces uniqueness on the tuple.

#[test]
fn composite_pk_distinct_combinations_succeed() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute(
        "CREATE TABLE messages (
             conversation_id TEXT NOT NULL,
             message_id TEXT NOT NULL,
             role TEXT,
             PRIMARY KEY (conversation_id, message_id)
         )",
    )
    .unwrap();
    // Same conversation_id, different message_ids — both should insert.
    db.execute("INSERT INTO messages VALUES ('c1', 'm1', 'user')")
        .unwrap();
    db.execute("INSERT INTO messages VALUES ('c1', 'm2', 'assistant')")
        .unwrap();
    db.execute("INSERT INTO messages VALUES ('c1', 'm3', 'user')")
        .unwrap();
    // Same message_id but different conversation_id — also distinct.
    db.execute("INSERT INTO messages VALUES ('c2', 'm1', 'user')")
        .unwrap();
    let r = db.query("SELECT COUNT(*) FROM messages").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(4));
}

#[test]
fn composite_pk_full_tuple_match_rejected() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute(
        "CREATE TABLE messages (
             conversation_id TEXT NOT NULL,
             message_id TEXT NOT NULL,
             role TEXT,
             PRIMARY KEY (conversation_id, message_id)
         )",
    )
    .unwrap();
    db.execute("INSERT INTO messages VALUES ('c1', 'm1', 'user')")
        .unwrap();
    let r = db.execute("INSERT INTO messages VALUES ('c1', 'm1', 'user')");
    assert!(r.is_err());
    let msg = r.unwrap_err().to_string();
    assert!(
        msg.contains("UNIQUE constraint failed"),
        "expected UNIQUE error, got: {msg}"
    );
    // The error should name BOTH PK columns since the tuple is what's
    // unique — not just the first column.
    assert!(
        msg.contains("conversation_id") && msg.contains("message_id"),
        "expected both PK columns in error, got: {msg}"
    );
}

#[test]
fn single_column_pk_still_enforced() {
    // Sanity: the fix must not regress single-column PK uniqueness.
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id TEXT PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('a', 'x')").unwrap();
    let r = db.execute("INSERT INTO t VALUES ('a', 'y')");
    assert!(r.is_err());
    assert!(
        r.unwrap_err()
            .to_string()
            .contains("UNIQUE constraint failed")
    );
}

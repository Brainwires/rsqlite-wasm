use super::*;

#[test]
fn create_view_and_select() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
        .unwrap();

    db.execute("CREATE VIEW adults AS SELECT id, name, age FROM users WHERE age >= 30")
        .unwrap();

    let r = db.query("SELECT name, age FROM adults").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Alice".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("Charlie".to_string())
    );
}

#[test]
fn view_with_filter() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (2, 'Gadget', 24.99)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (3, 'Doohickey', 4.99)")
        .unwrap();

    db.execute("CREATE VIEW expensive AS SELECT * FROM items WHERE price > 10.0")
        .unwrap();

    let r = db
        .query("SELECT name FROM expensive WHERE name LIKE 'G%'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Gadget".to_string())
    );
}

#[test]
fn view_with_aggregation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount REAL)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (1, 'A', 100.0)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (2, 'B', 200.0)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (3, 'A', 150.0)")
        .unwrap();

    db.execute("CREATE VIEW product_totals AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product").unwrap();

    let r = db
        .query("SELECT * FROM product_totals ORDER BY product")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("A".to_string())
    );
    assert_eq!(r.rows[0].values[1], crate::types::Value::Real(250.0));
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("B".to_string())
    );
    assert_eq!(r.rows[1].values[1], crate::types::Value::Real(200.0));
}

#[test]
fn drop_view() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();

    // View exists
    let r = db.query("SELECT * FROM v");
    assert!(r.is_ok());

    db.execute("DROP VIEW v").unwrap();

    // View gone
    let r = db.query("SELECT * FROM v");
    assert!(r.is_err());
}

#[test]
fn drop_view_if_exists() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    // Should not error
    db.execute("DROP VIEW IF EXISTS nonexistent").unwrap();
}

#[test]
fn view_duplicate_error() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    let r = db.execute("CREATE VIEW v AS SELECT * FROM t");
    assert!(r.is_err());
}

#[test]
fn view_with_join() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 1, 50.0)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (2, 1, 75.0)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (3, 2, 100.0)")
        .unwrap();

    db.execute("CREATE VIEW user_orders AS SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id").unwrap();

    let r = db
        .query("SELECT * FROM user_orders ORDER BY total")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Alice".to_string())
    );
    assert_eq!(r.rows[0].values[1], crate::types::Value::Real(50.0));
}

#[test]
fn cte_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
        .unwrap();

    let r = db.query("WITH adults AS (SELECT * FROM users WHERE age >= 30) SELECT name FROM adults ORDER BY name").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Alice".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("Charlie".to_string())
    );
}

#[test]
fn cte_with_column_names() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

    let r = db
        .query("WITH renamed(x, y) AS (SELECT id, val FROM t) SELECT x, y FROM renamed")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("hello".to_string())
    );
}

#[test]
fn cte_multiple() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'A', 10.0)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (2, 'B', 20.0)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (3, 'C', 30.0)")
        .unwrap();

    let r = db
        .query(
            "WITH cheap AS (SELECT * FROM items WHERE price <= 15.0), \
         expensive AS (SELECT * FROM items WHERE price >= 25.0) \
         SELECT name FROM cheap UNION ALL SELECT name FROM expensive ORDER BY name",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("A".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("C".to_string())
    );
}

#[test]
fn cte_with_aggregation() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount REAL)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (1, 'North', 100.0)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (2, 'South', 200.0)")
        .unwrap();
    db.execute("INSERT INTO sales VALUES (3, 'North', 150.0)")
        .unwrap();

    let r = db
        .query(
            "WITH totals AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) \
         SELECT * FROM totals WHERE total > 200.0",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("North".to_string())
    );
    assert_eq!(r.rows[0].values[1], crate::types::Value::Real(250.0));
}

#[test]
fn glob_function_star() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'alice')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'Bob')").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'ALICE')").unwrap();

    // GLOB is case-sensitive, * matches any sequence
    let r = db
        .query("SELECT name FROM t WHERE glob('A*', name) ORDER BY name")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("ALICE".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("Alice".to_string())
    );
}

#[test]
fn glob_function_question_mark() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'cat')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'car')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'card')").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'ca')").unwrap();

    // ? matches exactly one character
    let r = db
        .query("SELECT name FROM t WHERE glob('ca?', name) ORDER BY name")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("car".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("cat".to_string())
    );
}

#[test]
fn glob_function_char_class() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a1')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b2')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'c3')").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'd4')").unwrap();

    // [a-c] matches a, b, or c
    let r = db
        .query("SELECT val FROM t WHERE glob('[a-c]*', val) ORDER BY val")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("a1".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("b2".to_string())
    );
    assert_eq!(
        r.rows[2].values[0],
        crate::types::Value::Text("c3".to_string())
    );
}

// ── Index range scan tests ─────────────────────────────

fn setup_range_scan_db() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, price INTEGER, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_price ON items(price)")
        .unwrap();
    db.execute("INSERT INTO items VALUES (1, 10, 'apple')")
        .unwrap();
    db.execute("INSERT INTO items VALUES (2, 20, 'banana')")
        .unwrap();
    db.execute("INSERT INTO items VALUES (3, 30, 'cherry')")
        .unwrap();
    db.execute("INSERT INTO items VALUES (4, 40, 'date')")
        .unwrap();
    db.execute("INSERT INTO items VALUES (5, 50, 'elderberry')")
        .unwrap();
    db
}

#[test]
fn index_range_scan_greater_than() {
    let mut db = setup_range_scan_db();
    let r = db
        .query("SELECT name FROM items WHERE price > 30 ORDER BY price")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("date".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("elderberry".to_string())
    );
}

#[test]
fn index_range_scan_greater_equal() {
    let mut db = setup_range_scan_db();
    let r = db
        .query("SELECT name FROM items WHERE price >= 30 ORDER BY price")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("cherry".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("date".to_string())
    );
    assert_eq!(
        r.rows[2].values[0],
        crate::types::Value::Text("elderberry".to_string())
    );
}

#[test]
fn index_range_scan_less_than() {
    let mut db = setup_range_scan_db();
    let r = db
        .query("SELECT name FROM items WHERE price < 30 ORDER BY price")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("apple".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("banana".to_string())
    );
}

#[test]
fn index_range_scan_less_equal() {
    let mut db = setup_range_scan_db();
    let r = db
        .query("SELECT name FROM items WHERE price <= 30 ORDER BY price")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("apple".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("banana".to_string())
    );
    assert_eq!(
        r.rows[2].values[0],
        crate::types::Value::Text("cherry".to_string())
    );
}

#[test]
fn index_range_scan_both_bounds() {
    let mut db = setup_range_scan_db();
    let r = db
        .query("SELECT name FROM items WHERE price > 10 AND price < 50 ORDER BY price")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("banana".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("cherry".to_string())
    );
    assert_eq!(
        r.rows[2].values[0],
        crate::types::Value::Text("date".to_string())
    );
}

#[test]
fn index_range_scan_between() {
    let mut db = setup_range_scan_db();
    let r = db
        .query("SELECT name FROM items WHERE price BETWEEN 20 AND 40 ORDER BY price")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("banana".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("cherry".to_string())
    );
    assert_eq!(
        r.rows[2].values[0],
        crate::types::Value::Text("date".to_string())
    );
}

#[test]
fn index_range_scan_no_matches() {
    let mut db = setup_range_scan_db();
    let r = db
        .query("SELECT name FROM items WHERE price > 100")
        .unwrap();
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn index_range_scan_all_match() {
    let mut db = setup_range_scan_db();
    let r = db
        .query("SELECT name FROM items WHERE price >= 10 AND price <= 50 ORDER BY price")
        .unwrap();
    assert_eq!(r.rows.len(), 5);
}

// ── Parameter binding tests ────────────────────────────

#[test]
fn param_binding_select() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();

    let r = db
        .query_with_params(
            "SELECT name FROM t WHERE id = ?",
            vec![rsqlite_storage::codec::Value::Integer(2)],
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("bob".to_string())
    );
}

#[test]
fn param_binding_insert() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    db.execute_with_params(
        "INSERT INTO t VALUES (?, ?)",
        vec![
            rsqlite_storage::codec::Value::Integer(1),
            rsqlite_storage::codec::Value::Text("charlie".to_string()),
        ],
    )
    .unwrap();

    let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("charlie".to_string())
    );
}

#[test]
fn param_binding_multiple() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let r = db
        .query_with_params(
            "SELECT id FROM t WHERE val > ? AND val < ? ORDER BY id",
            vec![
                rsqlite_storage::codec::Value::Integer(10),
                rsqlite_storage::codec::Value::Integer(30),
            ],
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
}

#[test]
fn insert_select_all_columns() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .unwrap();
    db.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO src VALUES (1, 'a', 10)").unwrap();
    db.execute("INSERT INTO src VALUES (2, 'b', 20)").unwrap();
    db.execute("INSERT INTO src VALUES (3, 'c', 30)").unwrap();
    db.execute("INSERT INTO dst SELECT * FROM src WHERE val > 10")
        .unwrap();
    let r = db.query("SELECT name, val FROM dst ORDER BY val").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("b".into()));
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(20));
    assert_eq!(r.rows[1].values[0], crate::types::Value::Text("c".into()));
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(30));
}

#[test]
fn insert_select_subset_columns() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .unwrap();
    db.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO src VALUES (1, 'alpha', 10)")
        .unwrap();
    db.execute("INSERT INTO src VALUES (2, 'beta', 20)")
        .unwrap();
    db.execute("INSERT INTO dst (label) SELECT name FROM src ORDER BY name")
        .unwrap();
    let r = db.query("SELECT label FROM dst ORDER BY label").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("alpha".into())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("beta".into())
    );
}

#[test]
fn insert_select_with_expression() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    db.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, doubled INTEGER)")
        .unwrap();
    db.execute("INSERT INTO src VALUES (1, 5)").unwrap();
    db.execute("INSERT INTO src VALUES (2, 10)").unwrap();
    db.execute("INSERT INTO dst (doubled) SELECT val * 2 FROM src")
        .unwrap();
    let r = db
        .query("SELECT doubled FROM dst ORDER BY doubled")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(10));
    assert_eq!(r.rows[1].values[0], crate::types::Value::Integer(20));
}

#[test]
fn group_concat_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, grp TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice', 'a')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'bob', 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'carol', 'b')")
        .unwrap();
    let r = db
        .query("SELECT grp, GROUP_CONCAT(name) FROM t GROUP BY grp ORDER BY grp")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("alice,bob".into())
    );
    assert_eq!(
        r.rows[1].values[1],
        crate::types::Value::Text("carol".into())
    );
}

#[test]
fn group_concat_custom_separator() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    let r = db.query("SELECT GROUP_CONCAT(name, ' | ') FROM t").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("a | b | c".into())
    );
}

#[test]
fn group_concat_distinct() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'y')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'x')").unwrap();
    let r = db
        .query("SELECT GROUP_CONCAT(DISTINCT val) FROM t")
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("x,y".into()));
}

#[test]
fn total_aggregate() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    let r = db.query("SELECT TOTAL(val) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Real(30.0));

    let r2 = db.query("SELECT TOTAL(val) FROM t WHERE id > 100").unwrap();
    assert_eq!(r2.rows[0].values[0], crate::types::Value::Real(0.0));
}

#[test]
#[allow(clippy::approx_constant)] // testing ROUND, not π
fn round_function() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT ROUND(3.14159)").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Real(3.0));

    let r2 = db.query("SELECT ROUND(3.14159, 2)").unwrap();
    assert_eq!(r2.rows[0].values[0], crate::types::Value::Real(3.14));
}

#[test]
fn scalar_min_max() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT MIN(3, 1, 2)").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));

    let r2 = db.query("SELECT MAX(3, 1, 2)").unwrap();
    assert_eq!(r2.rows[0].values[0], crate::types::Value::Integer(3));
}

#[test]
fn last_insert_rowid_function() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (42, 'test')").unwrap();
    let r = db.query("SELECT LAST_INSERT_ROWID()").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(42));
}

#[test]
fn changes_function() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();
    db.execute("DELETE FROM t WHERE val > 10").unwrap();
    let r = db.query("SELECT CHANGES()").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
}

#[test]
fn insert_or_replace() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();
    db.execute("INSERT OR REPLACE INTO t VALUES (1, 'replaced')")
        .unwrap();
    let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("replaced".into())
    );
}

#[test]
fn replace_into() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    db.execute("REPLACE INTO t VALUES (1, 'second')").unwrap();
    db.execute("REPLACE INTO t VALUES (2, 'new')").unwrap();
    let r = db.query("SELECT id, name FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("second".into())
    );
    assert_eq!(r.rows[1].values[1], crate::types::Value::Text("new".into()));
}

#[test]
fn insert_or_replace_text_pk() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)")
        .unwrap();
    db.execute("INSERT INTO settings VALUES ('theme', 'light')")
        .unwrap();
    db.execute("INSERT OR REPLACE INTO settings VALUES ('theme', 'dark')")
        .unwrap();
    let r = db
        .query("SELECT value FROM settings WHERE key = 'theme'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("dark".into())
    );
    let count = db.query("SELECT COUNT(*) FROM settings").unwrap();
    assert_eq!(count.rows[0].values[0], crate::types::Value::Integer(1));
}

#[test]
fn replace_into_text_pk() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO kv VALUES ('a', 'one')").unwrap();
    db.execute("INSERT INTO kv VALUES ('b', 'two')").unwrap();
    db.execute("REPLACE INTO kv VALUES ('a', 'updated')")
        .unwrap();
    db.execute("REPLACE INTO kv VALUES ('c', 'three')").unwrap();
    let r = db.query("SELECT k, v FROM kv ORDER BY k").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("updated".into())
    );
    assert_eq!(r.rows[1].values[1], crate::types::Value::Text("two".into()));
    assert_eq!(
        r.rows[2].values[1],
        crate::types::Value::Text("three".into())
    );
}

#[test]
fn insert_or_replace_text_pk_with_index() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, score INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_score ON items(score)")
        .unwrap();
    db.execute("INSERT INTO items VALUES ('x1', 'alpha', 10)")
        .unwrap();
    db.execute("INSERT OR REPLACE INTO items VALUES ('x1', 'beta', 20)")
        .unwrap();
    let r = db
        .query("SELECT name, score FROM items WHERE id = 'x1'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("beta".into())
    );
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(20));
    let count = db.query("SELECT COUNT(*) FROM items").unwrap();
    assert_eq!(count.rows[0].values[0], crate::types::Value::Integer(1));
}

#[test]
fn printf_function() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query("SELECT PRINTF('hello %s, you are %d', 'world', 42)")
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("hello world, you are 42".into())
    );
}

#[test]
fn insert_or_ignore() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    db.execute("INSERT OR IGNORE INTO t VALUES (1, 'ignored')")
        .unwrap();
    db.execute("INSERT OR IGNORE INTO t VALUES (2, 'second')")
        .unwrap();
    let r = db.query("SELECT id, name FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("first".into())
    );
    assert_eq!(
        r.rows[1].values[1],
        crate::types::Value::Text("second".into())
    );
}

#[test]
fn order_by_column_number() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'c', 30)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'a', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'b', 20)").unwrap();
    let r = db.query("SELECT name, val FROM t ORDER BY 2").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("a".into()));
    assert_eq!(r.rows[1].values[0], crate::types::Value::Text("b".into()));
    assert_eq!(r.rows[2].values[0], crate::types::Value::Text("c".into()));
}

#[test]
fn create_table_as_select() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO src VALUES (1, 'alice', 100)")
        .unwrap();
    db.execute("INSERT INTO src VALUES (2, 'bob', 200)")
        .unwrap();
    db.execute("CREATE TABLE dst AS SELECT name, val FROM src WHERE val > 100")
        .unwrap();
    let r = db.query("SELECT name, val FROM dst").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("bob".into()));
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(200));
}

#[test]
fn create_table_as_select_if_not_exists() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'test')").unwrap();
    db.execute("CREATE TABLE IF NOT EXISTS t AS SELECT * FROM t")
        .unwrap();
    let r = db.query("SELECT * FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn date_function_literal() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT DATE('2024-06-15')").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("2024-06-15".into())
    );
}

#[test]
fn date_function_with_modifier() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT DATE('2024-01-31', '+1 month')").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("2024-02-29".into())
    );
}

#[test]
fn datetime_function() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query("SELECT DATETIME('2024-06-15 10:30:00', '+2 hours')")
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("2024-06-15 12:30:00".into())
    );
}

#[test]
fn time_function() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT TIME('2024-06-15 10:30:45')").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("10:30:45".into())
    );
}

#[test]
fn strftime_function() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query("SELECT STRFTIME('%Y/%m/%d', '2024-06-15')")
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("2024/06/15".into())
    );
}

#[test]
fn date_start_of_year() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query("SELECT DATE('2024-06-15', 'start of year')")
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("2024-01-01".into())
    );
}

#[test]
fn unixepoch_function() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT UNIXEPOCH('1970-01-01 00:00:00')").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));
}

#[test]
fn iif_function() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db.query("SELECT IIF(1 > 0, 'yes', 'no')").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("yes".into()));
    let r2 = db.query("SELECT IIF(1 < 0, 'yes', 'no')").unwrap();
    assert_eq!(r2.rows[0].values[0], crate::types::Value::Text("no".into()));
}

#[test]
fn date_in_table_context() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, event_date TEXT)")
        .unwrap();
    db.execute("INSERT INTO events VALUES (1, 'start', '2024-01-15')")
        .unwrap();
    db.execute("INSERT INTO events VALUES (2, 'middle', '2024-06-15')")
        .unwrap();
    db.execute("INSERT INTO events VALUES (3, 'end', '2024-12-15')")
        .unwrap();
    let r = db
        .query("SELECT name FROM events WHERE event_date > DATE('2024-06-01') ORDER BY event_date")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("middle".into())
    );
    assert_eq!(r.rows[1].values[0], crate::types::Value::Text("end".into()));
}

#[test]
fn group_by_column_number() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b', 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'a', 30)").unwrap();
    let r = db
        .query("SELECT grp, SUM(val) FROM t GROUP BY 1 ORDER BY 1")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("a".into()));
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(40));
    assert_eq!(r.rows[1].values[0], crate::types::Value::Text("b".into()));
    assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(20));
}

// --- Vector functions ---

fn make_f32_blob(floats: &[f32]) -> Vec<u8> {
    floats.iter().flat_map(|f| f.to_le_bytes()).collect()
}

#[test]
fn vec_distance_cosine_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE v (id INTEGER PRIMARY KEY, vec BLOB)")
        .unwrap();
    let v1 = make_f32_blob(&[1.0, 0.0, 0.0]);
    let v2 = make_f32_blob(&[0.0, 1.0, 0.0]);
    let v3 = make_f32_blob(&[1.0, 0.0, 0.0]);
    db.execute_with_params(
        "INSERT INTO v VALUES (1, ?)",
        vec![crate::types::Value::Blob(v1)],
    )
    .unwrap();
    db.execute_with_params(
        "INSERT INTO v VALUES (2, ?)",
        vec![crate::types::Value::Blob(v2)],
    )
    .unwrap();
    db.execute_with_params(
        "INSERT INTO v VALUES (3, ?)",
        vec![crate::types::Value::Blob(v3)],
    )
    .unwrap();

    let query_vec = crate::types::Value::Blob(make_f32_blob(&[1.0, 0.0, 0.0]));
    let r = db
        .query_with_params(
            "SELECT id, vec_distance_cosine(vec, ?) AS dist FROM v ORDER BY dist",
            vec![query_vec],
        )
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    // IDs 1 and 3 should have distance ~0.0 (identical vectors)
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
    if let crate::types::Value::Real(d) = &r.rows[0].values[1] {
        assert!(*d < 0.001, "expected ~0.0, got {d}");
    }
    // ID 2 should have distance ~1.0 (orthogonal vectors)
    assert_eq!(r.rows[2].values[0], crate::types::Value::Integer(2));
    if let crate::types::Value::Real(d) = &r.rows[2].values[1] {
        assert!((*d - 1.0).abs() < 0.001, "expected ~1.0, got {d}");
    }
}

#[test]
fn vec_distance_l2_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let v1 = crate::types::Value::Blob(make_f32_blob(&[0.0, 0.0]));
    let v2 = crate::types::Value::Blob(make_f32_blob(&[3.0, 4.0]));
    let r = db
        .query_with_params("SELECT vec_distance_l2(?, ?)", vec![v1, v2])
        .unwrap();
    if let crate::types::Value::Real(d) = &r.rows[0].values[0] {
        assert!((*d - 5.0).abs() < 0.001, "expected 5.0, got {d}");
    } else {
        panic!("expected Real");
    }
}

#[test]
fn vec_distance_dot_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let v1 = crate::types::Value::Blob(make_f32_blob(&[1.0, 2.0, 3.0]));
    let v2 = crate::types::Value::Blob(make_f32_blob(&[4.0, 5.0, 6.0]));
    // dot = 1*4 + 2*5 + 3*6 = 32, returned as -32
    let r = db
        .query_with_params("SELECT vec_distance_dot(?, ?)", vec![v1, v2])
        .unwrap();
    if let crate::types::Value::Real(d) = &r.rows[0].values[0] {
        assert!((*d - (-32.0)).abs() < 0.001, "expected -32.0, got {d}");
    } else {
        panic!("expected Real");
    }
}

#[test]
fn vec_length_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let v = crate::types::Value::Blob(make_f32_blob(&[1.0, 2.0, 3.0, 4.0]));
    let r = db
        .query_with_params("SELECT vec_length(?)", vec![v])
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(4));
}

#[test]
fn vec_normalize_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let v = crate::types::Value::Blob(make_f32_blob(&[3.0, 4.0]));
    let r = db
        .query_with_params("SELECT vec_normalize(?)", vec![v])
        .unwrap();
    if let crate::types::Value::Blob(b) = &r.rows[0].values[0] {
        assert_eq!(b.len(), 8);
        let x = f32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        let y = f32::from_le_bytes([b[4], b[5], b[6], b[7]]);
        let norm = (x * x + y * y).sqrt();
        assert!((norm - 1.0).abs() < 0.001, "expected unit norm, got {norm}");
        assert!((x - 0.6).abs() < 0.001, "expected 0.6, got {x}");
        assert!((y - 0.8).abs() < 0.001, "expected 0.8, got {y}");
    } else {
        panic!("expected Blob");
    }
}

#[test]
fn vec_from_json_to_json_roundtrip() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let r = db
        .query("SELECT vec_to_json(vec_from_json('[1.5, 2.5, 3.5]'))")
        .unwrap();
    if let crate::types::Value::Text(s) = &r.rows[0].values[0] {
        assert_eq!(s, "[1.5,2.5,3.5]");
    } else {
        panic!("expected Text");
    }
}

#[test]
fn vec_knn_query() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, text TEXT, vector BLOB)")
        .unwrap();
    let cat_vec = crate::types::Value::Blob(make_f32_blob(&[1.0, 0.0, 0.0]));
    let dog_vec = crate::types::Value::Blob(make_f32_blob(&[0.9, 0.1, 0.0]));
    let car_vec = crate::types::Value::Blob(make_f32_blob(&[0.0, 0.0, 1.0]));
    db.execute_with_params("INSERT INTO embeddings VALUES (1, 'cat', ?)", vec![cat_vec])
        .unwrap();
    db.execute_with_params("INSERT INTO embeddings VALUES (2, 'dog', ?)", vec![dog_vec])
        .unwrap();
    db.execute_with_params("INSERT INTO embeddings VALUES (3, 'car', ?)", vec![car_vec])
        .unwrap();

    let query_vec = crate::types::Value::Blob(make_f32_blob(&[1.0, 0.0, 0.0]));
    let r = db.query_with_params(
        "SELECT text, vec_distance_cosine(vector, ?) AS dist FROM embeddings ORDER BY dist LIMIT 2",
        vec![query_vec],
    ).unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("cat".into()));
    assert_eq!(r.rows[1].values[0], crate::types::Value::Text("dog".into()));
}

#[test]
fn vec_mismatched_dimensions() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let v1 = crate::types::Value::Blob(make_f32_blob(&[1.0, 2.0]));
    let v2 = crate::types::Value::Blob(make_f32_blob(&[1.0, 2.0, 3.0]));
    let r = db.query_with_params("SELECT vec_distance_cosine(?, ?)", vec![v1, v2]);
    assert!(r.is_err());
    let err = r.unwrap_err().to_string();
    assert!(err.contains("dimension mismatch"), "got: {err}");
}

#[test]
fn vec_invalid_blob() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    let v = crate::types::Value::Blob(vec![1, 2, 3]); // 3 bytes, not multiple of 4
    let r = db.query_with_params("SELECT vec_length(?)", vec![v]);
    assert!(r.is_err());
}

// --- Window functions ---

fn setup_employees() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();
    db.execute("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 'eng', 100)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (2, 'Bob', 'eng', 120)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (3, 'Carol', 'sales', 90)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (4, 'Dave', 'sales', 110)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (5, 'Eve', 'eng', 100)")
        .unwrap();
    db
}

#[test]
fn window_row_number() {
    let mut db = setup_employees();
    let r = db
        .query("SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM emp ORDER BY rn")
        .unwrap();
    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Bob".into()));
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(1));
    assert_eq!(r.rows[4].values[1], crate::types::Value::Integer(5));
}

#[test]
fn window_rank_with_ties() {
    let mut db = setup_employees();
    let r = db.query(
        "SELECT name, salary, RANK() OVER (ORDER BY salary DESC) AS rnk FROM emp ORDER BY rnk, name"
    ).unwrap();
    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.rows[0].values[2], crate::types::Value::Integer(1)); // Bob=120
    assert_eq!(r.rows[1].values[2], crate::types::Value::Integer(2)); // Dave=110
    // Alice=100 and Eve=100 -> rank 3
    assert_eq!(r.rows[2].values[2], crate::types::Value::Integer(3));
    assert_eq!(r.rows[3].values[2], crate::types::Value::Integer(3));
    // Carol=90 -> rank 5
    assert_eq!(r.rows[4].values[2], crate::types::Value::Integer(5));
}

#[test]
fn window_dense_rank() {
    let mut db = setup_employees();
    let r = db.query(
        "SELECT name, salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS drnk FROM emp ORDER BY drnk, name"
    ).unwrap();
    assert_eq!(r.rows.len(), 5);
    assert_eq!(r.rows[0].values[2], crate::types::Value::Integer(1)); // Bob=120
    assert_eq!(r.rows[1].values[2], crate::types::Value::Integer(2)); // Dave=110
    assert_eq!(r.rows[2].values[2], crate::types::Value::Integer(3)); // Alice=100
    assert_eq!(r.rows[3].values[2], crate::types::Value::Integer(3)); // Eve=100
    assert_eq!(r.rows[4].values[2], crate::types::Value::Integer(4)); // Carol=90
}

#[test]
fn window_partition_by() {
    let mut db = setup_employees();
    let r = db
        .query(
            "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn \
         FROM emp ORDER BY dept, rn",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 5);
    // eng partition first (alphabetical): Bob(120)=1, then Alice/Eve
    let eng_rows: Vec<_> = r
        .rows
        .iter()
        .filter(|row| row.values[1] == crate::types::Value::Text("eng".into()))
        .collect();
    assert_eq!(eng_rows.len(), 3);
    assert_eq!(eng_rows[0].values[2], crate::types::Value::Integer(1));
    assert_eq!(eng_rows[1].values[2], crate::types::Value::Integer(2));
    assert_eq!(eng_rows[2].values[2], crate::types::Value::Integer(3));
}

#[test]
fn window_lag_lead() {
    let mut db = setup_employees();
    let r = db
        .query(
            "SELECT name, salary, LAG(salary, 1, 0) OVER (ORDER BY salary) AS prev_sal, \
         LEAD(salary, 1, 0) OVER (ORDER BY salary) AS next_sal FROM emp ORDER BY salary",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 5);
    // First row by salary (Carol, 90): prev=0 (default)
    assert_eq!(r.rows[0].values[2], crate::types::Value::Integer(0));
    // Last row by salary (Bob, 120): next=0 (default)
    assert_eq!(r.rows[4].values[3], crate::types::Value::Integer(0));
}

#[test]
fn window_aggregate_over() {
    let mut db = setup_employees();
    let r = db.query(
        "SELECT name, dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM emp"
    ).unwrap();
    assert_eq!(r.rows.len(), 5);
    // eng total: 100+120+100=320, sales total: 90+110=200
    for row in &r.rows {
        let dept = &row.values[1];
        let total = &row.values[3];
        if *dept == crate::types::Value::Text("eng".into()) {
            assert_eq!(*total, crate::types::Value::Integer(320));
        } else {
            assert_eq!(*total, crate::types::Value::Integer(200));
        }
    }
}

#[test]
fn window_no_partition() {
    let mut db = setup_employees();
    let r = db
        .query("SELECT name, COUNT(*) OVER () AS total FROM emp")
        .unwrap();
    assert_eq!(r.rows.len(), 5);
    for row in &r.rows {
        assert_eq!(row.values[1], crate::types::Value::Integer(5));
    }
}

#[test]
fn window_ntile() {
    let mut db = setup_employees();
    let r = db
        .query("SELECT name, NTILE(2) OVER (ORDER BY salary) AS bucket FROM emp")
        .unwrap();
    assert_eq!(r.rows.len(), 5);
    // 5 rows in 2 buckets: 3 in bucket 1, 2 in bucket 2
    let bucket1_count = r
        .rows
        .iter()
        .filter(|r| r.values[1] == crate::types::Value::Integer(1))
        .count();
    let bucket2_count = r
        .rows
        .iter()
        .filter(|r| r.values[1] == crate::types::Value::Integer(2))
        .count();
    assert!(
        bucket1_count > 0 && bucket2_count > 0,
        "both buckets should have rows"
    );
}

#[test]
fn window_first_last_value() {
    let mut db = setup_employees();
    let r = db
        .query(
            "SELECT name, FIRST_VALUE(name) OVER (ORDER BY salary DESC) AS first_n, \
         LAST_VALUE(name) OVER (ORDER BY salary DESC) AS last_n FROM emp",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 5);
    // First value should be Bob (highest salary)
    assert_eq!(r.rows[0].values[1], crate::types::Value::Text("Bob".into()));
}

#[test]
fn window_multiple_functions() {
    let mut db = setup_employees();
    let r = db
        .query(
            "SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn, \
         COUNT(*) OVER () AS total FROM emp",
        )
        .unwrap();
    assert_eq!(r.rows.len(), 5);
    for row in &r.rows {
        assert_eq!(row.values[2], crate::types::Value::Integer(5));
    }
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(1));
}

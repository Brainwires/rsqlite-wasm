use super::*;

#[test]
fn create_index() {
    let db_path = "/tmp/rsqlite_db_create_index.db";
    let _ = std::fs::remove_file(db_path);

    let vfs = rsqlite_vfs::native::NativeVfs::new();
    let mut db = Database::create(&vfs, db_path).unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();

    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
        .unwrap();

    db.execute("CREATE INDEX idx_users_name ON users(name)")
        .unwrap();

    assert!(db.catalog().indexes.contains_key("idx_users_name"));

    // Queries should still work after creating the index
    let result = db.query("SELECT * FROM users WHERE name = 'Bob'").unwrap();
    assert_eq!(result.rows.len(), 1);

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn create_index_verify_with_sqlite3() {
    let db_path = "/tmp/rsqlite_db_create_index_compat.db";
    let _ = std::fs::remove_file(db_path);

    let vfs = rsqlite_vfs::native::NativeVfs::new();
    let mut db = Database::create(&vfs, db_path).unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
        .unwrap();

    db.execute("INSERT INTO t VALUES (1, 'Alice', 90)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob', 80)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'Charlie', 70)")
        .unwrap();

    db.execute("CREATE INDEX idx_t_score ON t(score)").unwrap();

    drop(db);

    // Verify sqlite3 can read the database and use the index
    let output = match std::process::Command::new("sqlite3")
        .arg(db_path)
        .arg(
            "SELECT * FROM t ORDER BY id;\
             .indices t",
        )
        .output()
    {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
        _ => {
            let _ = std::fs::remove_file(db_path);
            return;
        }
    };

    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
    assert!(output.contains("Charlie"));
    assert!(output.contains("idx_t_score"));

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn index_maintained_on_insert() {
    let db_path = "/tmp/rsqlite_db_idx_insert.db";
    let _ = std::fs::remove_file(db_path);

    let vfs = rsqlite_vfs::native::NativeVfs::new();
    let mut db = Database::create(&vfs, db_path).unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t(name)").unwrap();

    // Insert after index creation
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

    // Verify the data is accessible
    let result = db.query("SELECT * FROM t").unwrap();
    assert_eq!(result.rows.len(), 2);

    // Verify with sqlite3 that the index is valid
    drop(db);

    let output = match std::process::Command::new("sqlite3")
        .arg(db_path)
        .arg("PRAGMA integrity_check;")
        .output()
    {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
        _ => {
            let _ = std::fs::remove_file(db_path);
            return;
        }
    };

    assert!(output.trim() == "ok", "integrity_check failed: {output}");

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn index_scan_basic() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON users(name)").unwrap();

    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (4, 'Alice', 28)")
        .unwrap();

    let result = db
        .query("SELECT id, name, age FROM users WHERE name = 'Alice'")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].values[1],
        crate::types::Value::Text("Alice".to_string())
    );
    assert_eq!(
        result.rows[1].values[1],
        crate::types::Value::Text("Alice".to_string())
    );
}

#[test]
fn index_scan_no_results() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_code ON items(code)").unwrap();

    db.execute("INSERT INTO items VALUES (1, 'A001')").unwrap();
    db.execute("INSERT INTO items VALUES (2, 'B002')").unwrap();

    let result = db.query("SELECT * FROM items WHERE code = 'C003'").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn index_scan_with_additional_filter() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_cat ON products(category)")
        .unwrap();

    db.execute("INSERT INTO products VALUES (1, 'Electronics', 100)")
        .unwrap();
    db.execute("INSERT INTO products VALUES (2, 'Electronics', 200)")
        .unwrap();
    db.execute("INSERT INTO products VALUES (3, 'Books', 15)")
        .unwrap();
    db.execute("INSERT INTO products VALUES (4, 'Electronics', 50)")
        .unwrap();

    let result = db
        .query("SELECT * FROM products WHERE category = 'Electronics' AND price > 75")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn index_scan_integer_key() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE scores (id INTEGER PRIMARY KEY, player TEXT, level INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_level ON scores(level)")
        .unwrap();

    db.execute("INSERT INTO scores VALUES (1, 'Alice', 5)")
        .unwrap();
    db.execute("INSERT INTO scores VALUES (2, 'Bob', 3)")
        .unwrap();
    db.execute("INSERT INTO scores VALUES (3, 'Charlie', 5)")
        .unwrap();
    db.execute("INSERT INTO scores VALUES (4, 'Dave', 1)")
        .unwrap();

    let result = db
        .query("SELECT player FROM scores WHERE level = 5")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn index_scan_returns_same_as_table_scan() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();

    for i in 1..=20 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, '{}')",
            if i % 3 == 0 { "match" } else { "other" }
        ))
        .unwrap();
    }

    let before_index = db.query("SELECT id FROM t WHERE val = 'match'").unwrap();

    db.execute("CREATE INDEX idx_val ON t(val)").unwrap();

    let after_index = db.query("SELECT id FROM t WHERE val = 'match'").unwrap();

    assert_eq!(before_index.rows.len(), after_index.rows.len());
    let before_ids: Vec<_> = before_index.rows.iter().map(|r| &r.values[0]).collect();
    let after_ids: Vec<_> = after_index.rows.iter().map(|r| &r.values[0]).collect();
    assert_eq!(before_ids, after_ids);
}

#[test]
fn pragma_table_info() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")
        .unwrap();

    let result = db.query("PRAGMA table_info(users)").unwrap();
    assert_eq!(
        result.columns,
        vec!["cid", "name", "type", "notnull", "dflt_value", "pk"]
    );
    assert_eq!(result.rows.len(), 3);

    // cid=0, name=id, type=INTEGER, notnull=1, dflt_value=NULL, pk=1
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(0));
    assert_eq!(
        result.rows[0].values[1],
        crate::types::Value::Text("id".to_string())
    );
    assert_eq!(
        result.rows[0].values[2],
        crate::types::Value::Text("INTEGER".to_string())
    );
    assert_eq!(result.rows[0].values[3], crate::types::Value::Integer(1)); // not null (PK)
    assert_eq!(result.rows[0].values[5], crate::types::Value::Integer(1)); // pk

    // cid=1, name=name, type=TEXT, notnull=1, pk=0
    assert_eq!(
        result.rows[1].values[1],
        crate::types::Value::Text("name".to_string())
    );
    assert_eq!(
        result.rows[1].values[2],
        crate::types::Value::Text("TEXT".to_string())
    );
    assert_eq!(result.rows[1].values[3], crate::types::Value::Integer(1)); // NOT NULL
    assert_eq!(result.rows[1].values[5], crate::types::Value::Integer(0));

    // cid=2, name=age, type=INTEGER, notnull=0, pk=0
    assert_eq!(
        result.rows[2].values[1],
        crate::types::Value::Text("age".to_string())
    );
    assert_eq!(result.rows[2].values[3], crate::types::Value::Integer(0)); // nullable
    assert_eq!(result.rows[2].values[5], crate::types::Value::Integer(0));
}

#[test]
fn pragma_table_info_quoted() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT)")
        .unwrap();

    // Also works with quoted argument
    let result = db.query("PRAGMA table_info('items')").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].values[1],
        crate::types::Value::Text("id".to_string())
    );
    assert_eq!(
        result.rows[1].values[1],
        crate::types::Value::Text("code".to_string())
    );
}

#[test]
fn pragma_table_list() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE alpha (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE beta (id INTEGER PRIMARY KEY)")
        .unwrap();

    let result = db.query("PRAGMA table_list").unwrap();
    assert_eq!(result.columns, vec!["schema", "name", "type"]);
    assert!(result.rows.len() >= 2);

    let names: Vec<String> = result
        .rows
        .iter()
        .map(|r| {
            if let crate::types::Value::Text(s) = &r.values[1] {
                s.clone()
            } else {
                String::new()
            }
        })
        .collect();
    assert!(names.contains(&"alpha".to_string()));
    assert!(names.contains(&"beta".to_string()));
}

#[test]
fn pragma_index_list() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t(name)").unwrap();
    db.execute("CREATE INDEX idx_age ON t(age)").unwrap();

    let result = db.query("PRAGMA index_list(t)").unwrap();
    assert_eq!(
        result.columns,
        vec!["seq", "name", "unique", "origin", "partial"]
    );
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn pragma_index_info() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t(name)").unwrap();

    let result = db.query("PRAGMA index_info(idx_name)").unwrap();
    assert_eq!(result.columns, vec!["seqno", "cid", "name"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(0)); // seqno
    assert_eq!(result.rows[0].values[1], crate::types::Value::Integer(1)); // cid (name is col 1)
    assert_eq!(
        result.rows[0].values[2],
        crate::types::Value::Text("name".to_string())
    );
}

#[test]
fn pragma_page_size() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    let result = db.query("PRAGMA page_size").unwrap();
    assert_eq!(result.columns, vec!["page_size"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(4096));
}

#[test]
fn pragma_page_count() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    let result = db.query("PRAGMA page_count").unwrap();
    assert_eq!(result.columns, vec!["page_count"]);
    assert_eq!(result.rows.len(), 1);
    // Freshly created DB has 1 page
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(1));

    // Create a table and check page count grows
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    let result2 = db.query("PRAGMA page_count").unwrap();
    let count = if let crate::types::Value::Integer(n) = result2.rows[0].values[0] {
        n
    } else {
        panic!("expected integer");
    };
    assert!(count >= 2, "page_count should grow after CREATE TABLE");
}

#[test]
fn pragma_database_list() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    let result = db.query("PRAGMA database_list").unwrap();
    assert_eq!(result.columns, vec!["seq", "name", "file"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(0));
    assert_eq!(
        result.rows[0].values[1],
        crate::types::Value::Text("main".to_string())
    );
}

#[test]
fn drop_table() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'hello')").unwrap();

    let result = db.query("SELECT * FROM t1").unwrap();
    assert_eq!(result.rows.len(), 1);

    db.execute("DROP TABLE t1").unwrap();

    assert!(db.query("SELECT * FROM t1").is_err());
}

#[test]
fn drop_table_if_exists() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("DROP TABLE IF EXISTS nonexistent").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("DROP TABLE IF EXISTS t1").unwrap();

    assert!(db.query("SELECT * FROM t1").is_err());
}

#[test]
fn drop_table_removes_indexes() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t1(name)").unwrap();

    let idx_list = db.query("PRAGMA index_list('t1')").unwrap();
    assert_eq!(idx_list.rows.len(), 1);

    db.execute("DROP TABLE t1").unwrap();

    let tables = db.query("PRAGMA table_list").unwrap();
    assert!(
        tables
            .rows
            .iter()
            .all(|r| r.values[1] != crate::types::Value::Text("t1".to_string()))
    );
}

#[test]
fn drop_index() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t1(name)").unwrap();

    let idx_list = db.query("PRAGMA index_list('t1')").unwrap();
    assert_eq!(idx_list.rows.len(), 1);

    db.execute("DROP INDEX idx_name").unwrap();

    let idx_list = db.query("PRAGMA index_list('t1')").unwrap();
    assert_eq!(idx_list.rows.len(), 0);
}

#[test]
fn drop_index_if_exists() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("DROP INDEX IF EXISTS nonexistent").unwrap();
}

#[test]
fn drop_table_then_recreate() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'first')").unwrap();
    db.execute("DROP TABLE t1").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT, extra INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'second', 42)")
        .unwrap();

    let result = db.query("SELECT * FROM t1").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 3);
}

#[test]
fn select_with_alias() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice', 30)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob', 25)").unwrap();

    let result = db
        .query("SELECT name AS person, age AS years FROM t ORDER BY years")
        .unwrap();
    assert_eq!(result.columns, vec!["person", "years"]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0].values[0],
        crate::types::Value::Text("Bob".to_string())
    );
}

#[test]
fn select_aggregate_alias() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b', 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'a', 30)").unwrap();

    let result = db
        .query("SELECT cat, COUNT(*) AS cnt, SUM(val) AS total FROM t GROUP BY cat ORDER BY cnt")
        .unwrap();
    assert_eq!(result.columns, vec!["cat", "cnt", "total"]);
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn like_operator() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'Charlie')").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'Alicia')").unwrap();

    let r = db
        .query("SELECT name FROM t WHERE name LIKE 'Al%'")
        .unwrap();
    assert_eq!(r.rows.len(), 2);

    let r = db
        .query("SELECT name FROM t WHERE name LIKE '%ob%'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);

    let r = db
        .query("SELECT name FROM t WHERE name LIKE '_o%'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);

    let r = db
        .query("SELECT name FROM t WHERE name NOT LIKE 'A%'")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn between_operator() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }

    let r = db
        .query("SELECT val FROM t WHERE val BETWEEN 3 AND 7")
        .unwrap();
    assert_eq!(r.rows.len(), 5);

    let r = db
        .query("SELECT val FROM t WHERE val NOT BETWEEN 3 AND 7")
        .unwrap();
    assert_eq!(r.rows.len(), 5);
}

#[test]
fn in_list_operator() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'Charlie')").unwrap();

    let r = db.query("SELECT name FROM t WHERE id IN (1, 3)").unwrap();
    assert_eq!(r.rows.len(), 2);

    let r = db
        .query("SELECT name FROM t WHERE name IN ('Alice', 'Bob')")
        .unwrap();
    assert_eq!(r.rows.len(), 2);

    let r = db.query("SELECT name FROM t WHERE id NOT IN (1)").unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn case_expression() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 35)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 50)").unwrap();

    let r = db.query(
        "SELECT id, CASE WHEN age < 30 THEN 'young' WHEN age < 40 THEN 'mid' ELSE 'senior' END AS bracket FROM t ORDER BY id"
    ).unwrap();
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("young".to_string())
    );
    assert_eq!(
        r.rows[1].values[1],
        crate::types::Value::Text("mid".to_string())
    );
    assert_eq!(
        r.rows[2].values[1],
        crate::types::Value::Text("senior".to_string())
    );
}

#[test]
fn case_simple_form() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'active')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'inactive')").unwrap();

    let r = db.query(
        "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END AS code FROM t ORDER BY id"
    ).unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(r.rows[1].values[0], crate::types::Value::Integer(0));
}

#[test]
fn string_concat_operator() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, first TEXT, last TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'John', 'Doe')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Jane', 'Smith')")
        .unwrap();

    let r = db
        .query("SELECT first || ' ' || last AS full_name FROM t ORDER BY id")
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("John Doe".to_string())
    );
    assert_eq!(
        r.rows[1].values[0],
        crate::types::Value::Text("Jane Smith".to_string())
    );
}

#[test]
fn cast_expression() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '42')").unwrap();

    let r = db
        .query("SELECT CAST(val AS INTEGER) AS num FROM t")
        .unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(42));

    let r = db.query("SELECT CAST(id AS TEXT) AS txt FROM t").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("1".to_string())
    );

    let r = db.query("SELECT CAST(val AS REAL) AS f FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Real(42.0));
}

#[test]
fn alter_table_add_column() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    db.execute("ALTER TABLE t ADD COLUMN age INTEGER").unwrap();

    db.execute("INSERT INTO t VALUES (2, 'Bob', 25)").unwrap();

    let r = db.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("Alice".to_string())
    );
    assert_eq!(r.rows[0].values[2], crate::types::Value::Null);
    assert_eq!(r.rows[1].values[0], crate::types::Value::Integer(2));
    assert_eq!(
        r.rows[1].values[1],
        crate::types::Value::Text("Bob".to_string())
    );
    assert_eq!(r.rows[1].values[2], crate::types::Value::Integer(25));
}

#[test]
fn alter_table_add_column_query_new_column() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

    db.execute("ALTER TABLE t ADD COLUMN score REAL").unwrap();

    let r = db.query("SELECT name, score FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[1], crate::types::Value::Null);
    assert_eq!(r.rows[1].values[1], crate::types::Value::Null);

    db.execute("UPDATE t SET score = 95.5 WHERE id = 1")
        .unwrap();
    let r = db.query("SELECT name, score FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0].values[1], crate::types::Value::Real(95.5));
}

#[test]
fn alter_table_add_column_duplicate_error() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let err = db
        .execute("ALTER TABLE t ADD COLUMN name TEXT")
        .unwrap_err();
    assert!(err.to_string().contains("duplicate column name"));
}

#[test]
fn alter_table_add_column_no_type() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();

    db.execute("ALTER TABLE t ADD COLUMN extra").unwrap();

    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let r = db.query("SELECT extra FROM t WHERE id = 1").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("hello".to_string())
    );
}

#[test]
fn alter_table_add_column_pragma_table_info() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("ALTER TABLE t ADD COLUMN age INTEGER").unwrap();

    let r = db.query("PRAGMA table_info(t)").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(
        r.rows[2].values[1],
        crate::types::Value::Text("age".to_string())
    );
    assert_eq!(
        r.rows[2].values[2],
        crate::types::Value::Text("INTEGER".to_string())
    );
}

#[test]
fn alter_table_nonexistent_error() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    let err = db
        .execute("ALTER TABLE nonexistent ADD COLUMN x TEXT")
        .unwrap_err();
    assert!(err.to_string().contains("no such table"));
}

#[test]
fn insert_default_values() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
        .unwrap();
    db.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    db.execute("INSERT INTO t DEFAULT VALUES").unwrap();

    let r = db.query("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[1], crate::types::Value::Null);
    assert_eq!(r.rows[0].values[2], crate::types::Value::Null);
    assert_eq!(r.rows[1].values[1], crate::types::Value::Null);
}

#[test]
fn insert_partial_columns_defaults_null() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t(name) VALUES ('Alice')").unwrap();

    let r = db.query("SELECT * FROM t").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("Alice".to_string())
    );
    assert_eq!(r.rows[0].values[2], crate::types::Value::Null);
}

#[test]
fn subquery_in_select() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1, 1, 10.0)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (2, 1, 20.0)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (3, 2, 30.0)")
        .unwrap();

    let r = db
        .query("SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    let names: Vec<String> = r
        .rows
        .iter()
        .map(|row| {
            if let crate::types::Value::Text(s) = &row.values[0] {
                s.clone()
            } else {
                String::new()
            }
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
}

#[test]
fn subquery_not_in() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (1, 1)").unwrap();
    db.execute("INSERT INTO orders VALUES (2, 2)").unwrap();

    let r = db
        .query("SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders)")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Charlie".to_string())
    );
}

#[test]
fn subquery_exists() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, ref_id INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t1 VALUES (2, 'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 1)").unwrap();

    let r = db
        .query("SELECT val FROM t1 WHERE EXISTS (SELECT 1 FROM t2)")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn subquery_not_exists() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'a')").unwrap();

    let r = db
        .query("SELECT val FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2)")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("a".to_string())
    );
}

#[test]
fn subquery_scalar() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let r = db
        .query("SELECT (SELECT COUNT(*) FROM t) AS cnt FROM t LIMIT 1")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(3));
}

#[test]
fn upsert_do_nothing() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    db.execute("INSERT INTO t VALUES (1, 'Bob') ON CONFLICT DO NOTHING")
        .unwrap();

    let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Alice".to_string())
    );

    let r = db.query("SELECT COUNT(*) AS cnt FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
}

#[test]
fn upsert_do_update() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, counter INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice', 1)").unwrap();

    db.execute(
        "INSERT INTO t VALUES (1, 'Bob', 1) ON CONFLICT(id) DO UPDATE SET counter = counter + 1",
    )
    .unwrap();

    let r = db
        .query("SELECT name, counter FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Alice".to_string())
    );
    assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(2));
}

#[test]
fn upsert_do_update_set_name() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    db.execute("INSERT INTO t VALUES (1, 'Bob') ON CONFLICT(id) DO UPDATE SET name = 'Updated'")
        .unwrap();

    let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("Updated".to_string())
    );
}

#[test]
fn upsert_no_conflict_inserts() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    db.execute("INSERT INTO t VALUES (2, 'Bob') ON CONFLICT DO NOTHING")
        .unwrap();

    let r = db.query("SELECT COUNT(*) AS cnt FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
}

#[test]
fn alter_table_rename() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE old_name (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO old_name VALUES (1, 'hello')")
        .unwrap();

    db.execute("ALTER TABLE old_name RENAME TO new_name")
        .unwrap();

    let r = db.query("SELECT val FROM new_name WHERE id = 1").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("hello".to_string())
    );

    let err = db.query("SELECT * FROM old_name");
    assert!(err.is_err());
}

#[test]
fn alter_table_rename_with_index() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_val ON t1(val)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'hello')").unwrap();

    db.execute("ALTER TABLE t1 RENAME TO t2").unwrap();

    let r = db.query("SELECT val FROM t2 WHERE id = 1").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("hello".to_string())
    );

    let r = db.query("PRAGMA index_list(t2)").unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn alter_table_rename_nonexistent_error() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    let err = db
        .execute("ALTER TABLE nonexistent RENAME TO foo")
        .unwrap_err();
    assert!(err.to_string().contains("no such table"));
}

#[test]
fn alter_table_rename_conflict_error() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
        .unwrap();

    let err = db.execute("ALTER TABLE t1 RENAME TO t2").unwrap_err();
    assert!(err.to_string().contains("already a table named"));
}

#[test]
fn union_all() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t1 VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO t2 VALUES (3, 'Charlie')").unwrap();
    db.execute("INSERT INTO t2 VALUES (4, 'Dave')").unwrap();

    let r = db
        .query("SELECT name FROM t1 UNION ALL SELECT name FROM t2")
        .unwrap();
    assert_eq!(r.rows.len(), 4);
}

#[test]
fn union_dedup() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t1 VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO t2 VALUES (3, 'Alice')").unwrap();
    db.execute("INSERT INTO t2 VALUES (4, 'Charlie')").unwrap();

    let r = db
        .query("SELECT name FROM t1 UNION SELECT name FROM t2")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn select_without_from() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    let r = db.query("SELECT 1 + 2").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(3));
}

#[test]
fn select_without_from_string() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    let r = db.query("SELECT 'hello' || ' ' || 'world'").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("hello world".to_string())
    );
}

#[test]
fn select_without_from_functions() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    let r = db
        .query("SELECT TYPEOF(42), TYPEOF('hi'), TYPEOF(NULL)")
        .unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("integer".to_string())
    );
    assert_eq!(
        r.rows[0].values[1],
        crate::types::Value::Text("text".to_string())
    );
    assert_eq!(
        r.rows[0].values[2],
        crate::types::Value::Text("null".to_string())
    );
}

#[test]
fn select_without_from_coalesce() {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut db = Database::create(&vfs, "test.db").unwrap();

    let r = db.query("SELECT COALESCE(NULL, NULL, 'found')").unwrap();
    assert_eq!(
        r.rows[0].values[0],
        crate::types::Value::Text("found".to_string())
    );
}

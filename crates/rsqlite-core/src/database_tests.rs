    use super::*;

    fn setup_test_db(path: &str, sql: &str) -> bool {
        let _ = std::fs::remove_file(path);
        match std::process::Command::new("sqlite3")
            .arg(path)
            .arg(sql)
            .status()
        {
            Ok(s) if s.success() => true,
            _ => {
                eprintln!("sqlite3 not available, skipping test");
                false
            }
        }
    }

    #[test]
    fn select_star() {
        let db_path = "/tmp/rsqlite_db_select_star.db";
        if !setup_test_db(
            db_path,
            "CREATE TABLE colors (id INTEGER PRIMARY KEY, name TEXT, hex TEXT);\
             INSERT INTO colors VALUES (1, 'red', '#FF0000');\
             INSERT INTO colors VALUES (2, 'green', '#00FF00');\
             INSERT INTO colors VALUES (3, 'blue', '#0000FF');",
        ) {
            return;
        }

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::open(&vfs, db_path).unwrap();
        let result = db.query("SELECT * FROM colors").unwrap();

        assert_eq!(result.columns, vec!["id", "name", "hex"]);
        assert_eq!(result.rows.len(), 3);

        // First row
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Integer(1));
        assert_eq!(result.rows[0].values[1], Value::Text("red".to_string()));
        assert_eq!(
            result.rows[0].values[2],
            Value::Text("#FF0000".to_string())
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn select_with_where() {
        let db_path = "/tmp/rsqlite_db_select_where.db";
        if !setup_test_db(
            db_path,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);\
             INSERT INTO users VALUES (1, 'Alice', 30);\
             INSERT INTO users VALUES (2, 'Bob', 25);\
             INSERT INTO users VALUES (3, 'Charlie', 35);\
             INSERT INTO users VALUES (4, 'Diana', 28);",
        ) {
            return;
        }

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::open(&vfs, db_path).unwrap();

        // Filter by equality
        let result = db.query("SELECT * FROM users WHERE name = 'Bob'").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].values[1],
            rsqlite_storage::codec::Value::Text("Bob".to_string())
        );

        // Filter by comparison
        let result = db.query("SELECT * FROM users WHERE age > 28").unwrap();
        assert_eq!(result.rows.len(), 2); // Alice(30) and Charlie(35)

        // Filter by AND
        let result = db
            .query("SELECT * FROM users WHERE age >= 28 AND age <= 30")
            .unwrap();
        assert_eq!(result.rows.len(), 2); // Alice(30) and Diana(28)

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn select_specific_columns() {
        let db_path = "/tmp/rsqlite_db_select_cols.db";
        if !setup_test_db(
            db_path,
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER);\
             INSERT INTO products VALUES (1, 'Widget', 9.99, 100);\
             INSERT INTO products VALUES (2, 'Gadget', 24.99, 50);",
        ) {
            return;
        }

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::open(&vfs, db_path).unwrap();

        let result = db.query("SELECT name, price FROM products").unwrap();
        assert_eq!(result.columns, vec!["name", "price"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values.len(), 2);
        assert_eq!(
            result.rows[0].values[0],
            rsqlite_storage::codec::Value::Text("Widget".to_string())
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn select_with_null_handling() {
        let db_path = "/tmp/rsqlite_db_null.db";
        if !setup_test_db(
            db_path,
            "CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);\
             INSERT INTO data VALUES (1, 'hello');\
             INSERT INTO data VALUES (2, NULL);\
             INSERT INTO data VALUES (3, 'world');",
        ) {
            return;
        }

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::open(&vfs, db_path).unwrap();

        let result = db.query("SELECT * FROM data WHERE val IS NOT NULL").unwrap();
        assert_eq!(result.rows.len(), 2);

        let result = db.query("SELECT * FROM data WHERE val IS NULL").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].values[0],
            rsqlite_storage::codec::Value::Integer(2)
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn select_200_rows_with_filter() {
        let db_path = "/tmp/rsqlite_db_200_filter.db";
        let mut sql = String::from(
            "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER);",
        );
        for i in 1..=200 {
            sql.push_str(&format!("INSERT INTO nums VALUES ({i}, {});", i * 10));
        }
        if !setup_test_db(db_path, &sql) {
            return;
        }

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::open(&vfs, db_path).unwrap();

        let result = db.query("SELECT * FROM nums WHERE val > 1900").unwrap();
        assert_eq!(result.rows.len(), 10); // ids 191-200, vals 1910-2000

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn create_table_and_insert() {
        let db_path = "/tmp/rsqlite_db_create_insert.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        )
        .unwrap();

        assert!(db.catalog().get_table("users").is_some());
        let table = db.catalog().get_table("users").unwrap();
        assert_eq!(table.columns.len(), 3);

        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
            .unwrap();

        let result = db.query("SELECT * FROM users").unwrap();
        assert_eq!(result.columns, vec!["id", "name", "age"]);
        assert_eq!(result.rows.len(), 3);

        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Integer(1));
        assert_eq!(result.rows[0].values[1], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[0].values[2], Value::Integer(30));

        assert_eq!(result.rows[2].values[1], Value::Text("Charlie".to_string()));
        assert_eq!(result.rows[2].values[2], Value::Integer(35));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn create_table_insert_and_verify_with_sqlite3() {
        let db_path = "/tmp/rsqlite_db_create_verify.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
        )
        .unwrap();

        db.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)")
            .unwrap();
        db.execute("INSERT INTO items VALUES (2, 'Gadget', 24.50)")
            .unwrap();
        db.execute("INSERT INTO items VALUES (3, 'Doohickey', 3.14)")
            .unwrap();

        drop(db);

        let output = match std::process::Command::new("sqlite3")
            .arg(db_path)
            .arg("SELECT * FROM items ORDER BY id;")
            .output()
        {
            Ok(o) if o.status.success() => {
                String::from_utf8_lossy(&o.stdout).to_string()
            }
            _ => {
                eprintln!("sqlite3 not available, skipping verification");
                let _ = std::fs::remove_file(db_path);
                return;
            }
        };

        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("Widget"));
        assert!(lines[1].contains("Gadget"));
        assert!(lines[2].contains("Doohickey"));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn create_table_if_not_exists() {
        let db_path = "/tmp/rsqlite_db_if_not_exists.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();

        // Should fail without IF NOT EXISTS
        assert!(db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").is_err());

        // Should succeed with IF NOT EXISTS
        db.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)")
            .unwrap();

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn insert_with_column_list() {
        let db_path = "/tmp/rsqlite_db_insert_cols.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
        )
        .unwrap();

        db.execute("INSERT INTO t (name, score) VALUES ('Alice', 100)")
            .unwrap();

        let result = db.query("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 1);

        use rsqlite_storage::codec::Value;
        // id should get auto-assigned rowid
        assert_eq!(result.rows[0].values[1], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[0].values[2], Value::Integer(100));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn insert_multiple_values() {
        let db_path = "/tmp/rsqlite_db_insert_multi.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        db.execute(
            "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        )
        .unwrap();

        let result = db.query("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 3);

        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[1], Value::Text("one".to_string()));
        assert_eq!(result.rows[1].values[1], Value::Text("two".to_string()));
        assert_eq!(result.rows[2].values[1], Value::Text("three".to_string()));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn create_insert_query_with_where() {
        let db_path = "/tmp/rsqlite_db_create_query_where.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)",
        )
        .unwrap();

        db.execute("INSERT INTO employees VALUES (1, 'Alice', 'eng', 120000)")
            .unwrap();
        db.execute("INSERT INTO employees VALUES (2, 'Bob', 'sales', 80000)")
            .unwrap();
        db.execute("INSERT INTO employees VALUES (3, 'Charlie', 'eng', 150000)")
            .unwrap();
        db.execute("INSERT INTO employees VALUES (4, 'Diana', 'sales', 95000)")
            .unwrap();

        let result = db
            .query("SELECT name, salary FROM employees WHERE dept = 'eng'")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.columns, vec!["name", "salary"]);

        let result = db
            .query("SELECT * FROM employees WHERE salary > 100000")
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn update_rows() {
        let db_path = "/tmp/rsqlite_db_update.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice', 80)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob', 90)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'Charlie', 70)").unwrap();

        let result = db.execute("UPDATE t SET score = 100 WHERE name = 'Bob'").unwrap();
        assert_eq!(result.rows_affected, 1);

        let result = db.query("SELECT * FROM t WHERE name = 'Bob'").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[2], Value::Integer(100));

        // Other rows unchanged
        let result = db.query("SELECT score FROM t WHERE name = 'Alice'").unwrap();
        assert_eq!(result.rows[0].values[0], Value::Integer(80));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn update_all_rows() {
        let db_path = "/tmp/rsqlite_db_update_all.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

        let result = db.execute("UPDATE t SET val = 0").unwrap();
        assert_eq!(result.rows_affected, 3);

        let result = db.query("SELECT * FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        for row in &result.rows {
            assert_eq!(row.values[1], Value::Integer(0));
        }

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn delete_rows() {
        let db_path = "/tmp/rsqlite_db_delete.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'Charlie')").unwrap();

        let result = db.execute("DELETE FROM t WHERE name = 'Bob'").unwrap();
        assert_eq!(result.rows_affected, 1);

        let result = db.query("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 2);

        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[1], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[1].values[1], Value::Text("Charlie".to_string()));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn delete_all_rows() {
        let db_path = "/tmp/rsqlite_db_delete_all.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

        let result = db.execute("DELETE FROM t").unwrap();
        assert_eq!(result.rows_affected, 2);

        let result = db.query("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 0);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn full_crud_cycle() {
        let db_path = "/tmp/rsqlite_db_crud.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        // Create
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
            .unwrap();

        // Insert
        db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (2, 'Gadget', 24.99)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (3, 'Doohickey', 3.14)")
            .unwrap();

        // Read
        let result = db.query("SELECT * FROM products").unwrap();
        assert_eq!(result.rows.len(), 3);

        // Update
        db.execute("UPDATE products SET price = 19.99 WHERE name = 'Widget'")
            .unwrap();
        let result = db
            .query("SELECT price FROM products WHERE name = 'Widget'")
            .unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Real(19.99));

        // Delete
        db.execute("DELETE FROM products WHERE name = 'Doohickey'")
            .unwrap();
        let result = db.query("SELECT * FROM products").unwrap();
        assert_eq!(result.rows.len(), 2);

        // Verify with sqlite3
        drop(db);
        let output = match std::process::Command::new("sqlite3")
            .arg(db_path)
            .arg("SELECT * FROM products ORDER BY id;")
            .output()
        {
            Ok(o) if o.status.success() => {
                String::from_utf8_lossy(&o.stdout).to_string()
            }
            _ => {
                let _ = std::fs::remove_file(db_path);
                return;
            }
        };

        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("Widget"));
        assert!(lines[0].contains("19.99"));
        assert!(lines[1].contains("Gadget"));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn transaction_commit() {
        let db_path = "/tmp/rsqlite_db_txn_commit.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'world')").unwrap();
        db.execute("COMMIT").unwrap();

        let result = db.query("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 2);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn transaction_rollback() {
        let db_path = "/tmp/rsqlite_db_txn_rollback.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'keep')").unwrap();

        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'discard')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'also discard')").unwrap();
        db.execute("ROLLBACK").unwrap();

        let result = db.query("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 1);
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[1], Value::Text("keep".to_string()));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn transaction_rollback_update() {
        let db_path = "/tmp/rsqlite_db_txn_rollback_upd.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 200)").unwrap();

        db.execute("BEGIN").unwrap();
        db.execute("UPDATE t SET val = 999 WHERE id = 1").unwrap();
        db.execute("DELETE FROM t WHERE id = 2").unwrap();
        db.execute("ROLLBACK").unwrap();

        let result = db.query("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 2);
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[1], Value::Integer(100));
        assert_eq!(result.rows[1].values[1], Value::Integer(200));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn transaction_commit_persists_to_disk() {
        let db_path = "/tmp/rsqlite_db_txn_persist.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();

        {
            let mut db = Database::create(&vfs, db_path).unwrap();
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap();
            db.execute("BEGIN").unwrap();
            db.execute("INSERT INTO t VALUES (1, 'persisted')").unwrap();
            db.execute("COMMIT").unwrap();
        }

        // Reopen and verify
        {
            let mut db = Database::open(&vfs, db_path).unwrap();
            let result = db.query("SELECT * FROM t").unwrap();
            assert_eq!(result.rows.len(), 1);
            use rsqlite_storage::codec::Value;
            assert_eq!(
                result.rows[0].values[1],
                Value::Text("persisted".to_string())
            );
        }

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn transaction_rollback_not_persisted() {
        let db_path = "/tmp/rsqlite_db_txn_no_persist.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();

        {
            let mut db = Database::create(&vfs, db_path).unwrap();
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();

            db.execute("BEGIN").unwrap();
            db.execute("INSERT INTO t VALUES (2, 'rolled_back')").unwrap();
            db.execute("ROLLBACK").unwrap();
        }

        // Reopen and verify only original data
        {
            let mut db = Database::open(&vfs, db_path).unwrap();
            let result = db.query("SELECT * FROM t").unwrap();
            assert_eq!(result.rows.len(), 1);
            use rsqlite_storage::codec::Value;
            assert_eq!(
                result.rows[0].values[1],
                Value::Text("original".to_string())
            );
        }

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn order_by_asc() {
        let db_path = "/tmp/rsqlite_db_order_asc.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Charlie', 70)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Alice', 90)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'Bob', 80)").unwrap();

        let result = db.query("SELECT name, score FROM t ORDER BY name").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[1].values[0], Value::Text("Bob".to_string()));
        assert_eq!(result.rows[2].values[0], Value::Text("Charlie".to_string()));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn order_by_desc() {
        let db_path = "/tmp/rsqlite_db_order_desc.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 70)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 90)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 80)").unwrap();

        let result = db.query("SELECT * FROM t ORDER BY score DESC").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[1], Value::Integer(90));
        assert_eq!(result.rows[1].values[1], Value::Integer(80));
        assert_eq!(result.rows[2].values[1], Value::Integer(70));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn limit_and_offset() {
        let db_path = "/tmp/rsqlite_db_limit.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        for i in 1..=10 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .unwrap();
        }

        // LIMIT only
        let result = db.query("SELECT * FROM t LIMIT 3").unwrap();
        assert_eq!(result.rows.len(), 3);

        // LIMIT with OFFSET
        let result = db.query("SELECT * FROM t LIMIT 3 OFFSET 5").unwrap();
        assert_eq!(result.rows.len(), 3);
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[1], Value::Integer(6));
        assert_eq!(result.rows[2].values[1], Value::Integer(8));

        // ORDER BY + LIMIT
        let result = db.query("SELECT * FROM t ORDER BY val DESC LIMIT 2").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values[1], Value::Integer(10));
        assert_eq!(result.rows[1].values[1], Value::Integer(9));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn select_distinct() {
        let db_path = "/tmp/rsqlite_db_distinct.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, color TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'red')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'blue')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'red')").unwrap();
        db.execute("INSERT INTO t VALUES (4, 'green')").unwrap();
        db.execute("INSERT INTO t VALUES (5, 'blue')").unwrap();

        let result = db.query("SELECT DISTINCT color FROM t").unwrap();
        assert_eq!(result.rows.len(), 3);

        let colors: Vec<String> = result
            .rows
            .iter()
            .map(|r| match &r.values[0] {
                rsqlite_storage::codec::Value::Text(s) => s.clone(),
                _ => panic!("expected text"),
            })
            .collect();
        assert!(colors.contains(&"red".to_string()));
        assert!(colors.contains(&"blue".to_string()));
        assert!(colors.contains(&"green".to_string()));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn order_by_multiple_keys() {
        let db_path = "/tmp/rsqlite_db_order_multi.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, dept TEXT, score INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'eng', 90)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'sales', 80)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'eng', 70)").unwrap();
        db.execute("INSERT INTO t VALUES (4, 'sales', 95)").unwrap();

        let result = db
            .query("SELECT * FROM t ORDER BY dept ASC, score DESC")
            .unwrap();
        use rsqlite_storage::codec::Value;
        // eng first (alphabetical), within eng: 90 then 70
        assert_eq!(result.rows[0].values[1], Value::Text("eng".to_string()));
        assert_eq!(result.rows[0].values[2], Value::Integer(90));
        assert_eq!(result.rows[1].values[1], Value::Text("eng".to_string()));
        assert_eq!(result.rows[1].values[2], Value::Integer(70));
        // sales second, within sales: 95 then 80
        assert_eq!(result.rows[2].values[1], Value::Text("sales".to_string()));
        assert_eq!(result.rows[2].values[2], Value::Integer(95));
        assert_eq!(result.rows[3].values[1], Value::Text("sales".to_string()));
        assert_eq!(result.rows[3].values[2], Value::Integer(80));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn inner_join() {
        let db_path = "/tmp/rsqlite_db_inner_join.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)")
            .unwrap();

        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie')").unwrap();

        db.execute("INSERT INTO orders VALUES (1, 1, 'Widget')").unwrap();
        db.execute("INSERT INTO orders VALUES (2, 1, 'Gadget')").unwrap();
        db.execute("INSERT INTO orders VALUES (3, 2, 'Doohickey')").unwrap();

        let result = db
            .query(
                "SELECT users.name, orders.product FROM users \
                 INNER JOIN orders ON users.id = orders.user_id \
                 ORDER BY orders.product",
            )
            .unwrap();

        assert_eq!(result.rows.len(), 3);
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[1], Value::Text("Doohickey".to_string()));
        assert_eq!(result.rows[0].values[0], Value::Text("Bob".to_string()));
        assert_eq!(result.rows[1].values[1], Value::Text("Gadget".to_string()));
        assert_eq!(result.rows[1].values[0], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[2].values[1], Value::Text("Widget".to_string()));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn left_join() {
        let db_path = "/tmp/rsqlite_db_left_join.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)")
            .unwrap();

        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie')").unwrap();

        db.execute("INSERT INTO orders VALUES (1, 1, 'Widget')").unwrap();

        let result = db
            .query(
                "SELECT users.name, orders.product FROM users \
                 LEFT JOIN orders ON users.id = orders.user_id \
                 ORDER BY users.name",
            )
            .unwrap();

        assert_eq!(result.rows.len(), 3);
        use rsqlite_storage::codec::Value;
        // Alice has an order
        assert_eq!(result.rows[0].values[0], Value::Text("Alice".to_string()));
        assert_eq!(result.rows[0].values[1], Value::Text("Widget".to_string()));
        // Bob has no order
        assert_eq!(result.rows[1].values[0], Value::Text("Bob".to_string()));
        assert_eq!(result.rows[1].values[1], Value::Null);
        // Charlie has no order
        assert_eq!(result.rows[2].values[0], Value::Text("Charlie".to_string()));
        assert_eq!(result.rows[2].values[1], Value::Null);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn cross_join() {
        let db_path = "/tmp/rsqlite_db_cross_join.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, x TEXT)")
            .unwrap();
        db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, y TEXT)")
            .unwrap();

        db.execute("INSERT INTO a VALUES (1, 'a1')").unwrap();
        db.execute("INSERT INTO a VALUES (2, 'a2')").unwrap();
        db.execute("INSERT INTO b VALUES (1, 'b1')").unwrap();
        db.execute("INSERT INTO b VALUES (2, 'b2')").unwrap();
        db.execute("INSERT INTO b VALUES (3, 'b3')").unwrap();

        let result = db
            .query("SELECT a.x, b.y FROM a CROSS JOIN b")
            .unwrap();

        // 2 * 3 = 6 rows
        assert_eq!(result.rows.len(), 6);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn implicit_cross_join() {
        let db_path = "/tmp/rsqlite_db_implicit_cross.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, x TEXT)")
            .unwrap();
        db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, y TEXT)")
            .unwrap();

        db.execute("INSERT INTO a VALUES (1, 'a1')").unwrap();
        db.execute("INSERT INTO a VALUES (2, 'a2')").unwrap();
        db.execute("INSERT INTO b VALUES (1, 'b1')").unwrap();
        db.execute("INSERT INTO b VALUES (2, 'b2')").unwrap();

        let result = db
            .query("SELECT a.x, b.y FROM a, b WHERE a.id = b.id")
            .unwrap();

        assert_eq!(result.rows.len(), 2);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn count_star() {
        let db_path = "/tmp/rsqlite_db_count_star.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'Charlie')").unwrap();

        let result = db.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(result.rows.len(), 1);
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Integer(3));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn count_column() {
        let db_path = "/tmp/rsqlite_db_count_col.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();

        let result = db.query("SELECT COUNT(val) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Integer(2));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn sum_and_avg() {
        let db_path = "/tmp/rsqlite_db_sum_avg.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

        let result = db.query("SELECT SUM(val) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Integer(60));

        let result = db.query("SELECT AVG(val) FROM t").unwrap();
        assert_eq!(result.rows[0].values[0], Value::Real(20.0));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn min_and_max() {
        let db_path = "/tmp/rsqlite_db_min_max.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 50)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 90)").unwrap();
        db.execute("INSERT INTO t VALUES (4, 30)").unwrap();

        let result = db.query("SELECT MIN(val), MAX(val) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Integer(10));
        assert_eq!(result.rows[0].values[1], Value::Integer(90));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn group_by() {
        let db_path = "/tmp/rsqlite_db_group_by.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute(
            "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)",
        )
        .unwrap();
        db.execute("INSERT INTO emp VALUES (1, 'eng', 100)").unwrap();
        db.execute("INSERT INTO emp VALUES (2, 'eng', 120)").unwrap();
        db.execute("INSERT INTO emp VALUES (3, 'sales', 80)").unwrap();
        db.execute("INSERT INTO emp VALUES (4, 'sales', 90)").unwrap();
        db.execute("INSERT INTO emp VALUES (5, 'eng', 110)").unwrap();

        let result = db
            .query("SELECT dept, COUNT(*), SUM(salary) FROM emp GROUP BY dept ORDER BY dept")
            .unwrap();

        assert_eq!(result.rows.len(), 2);
        use rsqlite_storage::codec::Value;
        // eng: 3 employees, sum=330
        assert_eq!(result.rows[0].values[0], Value::Text("eng".to_string()));
        assert_eq!(result.rows[0].values[1], Value::Integer(3));
        assert_eq!(result.rows[0].values[2], Value::Integer(330));
        // sales: 2 employees, sum=170
        assert_eq!(result.rows[1].values[0], Value::Text("sales".to_string()));
        assert_eq!(result.rows[1].values[1], Value::Integer(2));
        assert_eq!(result.rows[1].values[2], Value::Integer(170));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn group_by_having() {
        let db_path = "/tmp/rsqlite_db_having.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute(
            "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)",
        )
        .unwrap();
        db.execute("INSERT INTO emp VALUES (1, 'eng', 100)").unwrap();
        db.execute("INSERT INTO emp VALUES (2, 'eng', 120)").unwrap();
        db.execute("INSERT INTO emp VALUES (3, 'sales', 80)").unwrap();
        db.execute("INSERT INTO emp VALUES (4, 'sales', 90)").unwrap();
        db.execute("INSERT INTO emp VALUES (5, 'eng', 110)").unwrap();

        let result = db
            .query(
                "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 2",
            )
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Text("eng".to_string()));
        assert_eq!(result.rows[0].values[1], Value::Integer(3));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn aggregate_empty_table() {
        let db_path = "/tmp/rsqlite_db_agg_empty.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();

        let result = db.query("SELECT COUNT(*) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Integer(0));

        let result = db.query("SELECT SUM(val) FROM t").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Null);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn aggregate_with_where() {
        let db_path = "/tmp/rsqlite_db_agg_where.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'a', 20)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'b', 30)").unwrap();
        db.execute("INSERT INTO t VALUES (4, 'b', 40)").unwrap();

        let result = db
            .query("SELECT SUM(val) FROM t WHERE category = 'a'")
            .unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Integer(30));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn scalar_length() {
        let db_path = "/tmp/rsqlite_db_length.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        db.execute("INSERT INTO t VALUES (2, '')").unwrap();
        db.execute("INSERT INTO t VALUES (3, NULL)").unwrap();

        let result = db.query("SELECT LENGTH(s) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Integer(5));
        assert_eq!(result.rows[1].values[0], Value::Integer(0));
        assert_eq!(result.rows[2].values[0], Value::Null);

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn scalar_upper_lower() {
        let db_path = "/tmp/rsqlite_db_upper_lower.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Hello World')").unwrap();

        let result = db.query("SELECT UPPER(s), LOWER(s) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(
            result.rows[0].values[0],
            Value::Text("HELLO WORLD".to_string())
        );
        assert_eq!(
            result.rows[0].values[1],
            Value::Text("hello world".to_string())
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn scalar_substr() {
        let db_path = "/tmp/rsqlite_db_substr.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Hello World')").unwrap();

        let result = db.query("SELECT SUBSTR(s, 1, 5) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Text("Hello".to_string()));

        let result = db.query("SELECT SUBSTR(s, 7) FROM t").unwrap();
        assert_eq!(result.rows[0].values[0], Value::Text("World".to_string()));

        // Negative index: from end
        let result = db.query("SELECT SUBSTR(s, -5) FROM t").unwrap();
        assert_eq!(result.rows[0].values[0], Value::Text("World".to_string()));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn scalar_coalesce_ifnull() {
        let db_path = "/tmp/rsqlite_db_coalesce.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, NULL, 'fallback')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'primary', 'fallback')").unwrap();

        let result = db.query("SELECT COALESCE(a, b) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(
            result.rows[0].values[0],
            Value::Text("fallback".to_string())
        );
        assert_eq!(
            result.rows[1].values[0],
            Value::Text("primary".to_string())
        );

        let result = db.query("SELECT IFNULL(a, b) FROM t").unwrap();
        assert_eq!(
            result.rows[0].values[0],
            Value::Text("fallback".to_string())
        );
        assert_eq!(
            result.rows[1].values[0],
            Value::Text("primary".to_string())
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn scalar_typeof() {
        let db_path = "/tmp/rsqlite_db_typeof.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'hello')").unwrap();
        db.execute("INSERT INTO t VALUES (3, NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (4, 3.14)").unwrap();

        let result = db.query("SELECT TYPEOF(val) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(
            result.rows[0].values[0],
            Value::Text("integer".to_string())
        );
        assert_eq!(result.rows[1].values[0], Value::Text("text".to_string()));
        assert_eq!(result.rows[2].values[0], Value::Text("null".to_string()));
        assert_eq!(result.rows[3].values[0], Value::Text("real".to_string()));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn scalar_abs() {
        let db_path = "/tmp/rsqlite_db_abs.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, -42)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 42)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 0)").unwrap();

        let result = db.query("SELECT ABS(val) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Integer(42));
        assert_eq!(result.rows[1].values[0], Value::Integer(42));
        assert_eq!(result.rows[2].values[0], Value::Integer(0));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn scalar_replace_instr() {
        let db_path = "/tmp/rsqlite_db_replace_instr.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello world')").unwrap();

        let result = db
            .query("SELECT REPLACE(s, 'world', 'rust') FROM t")
            .unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(
            result.rows[0].values[0],
            Value::Text("hello rust".to_string())
        );

        let result = db.query("SELECT INSTR(s, 'world') FROM t").unwrap();
        assert_eq!(result.rows[0].values[0], Value::Integer(7));

        let result = db.query("SELECT INSTR(s, 'xyz') FROM t").unwrap();
        assert_eq!(result.rows[0].values[0], Value::Integer(0));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn scalar_trim() {
        let db_path = "/tmp/rsqlite_db_trim.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, '  hello  ')").unwrap();

        let result = db.query("SELECT TRIM(s) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Text("hello".to_string()));

        let result = db.query("SELECT LTRIM(s) FROM t").unwrap();
        assert_eq!(
            result.rows[0].values[0],
            Value::Text("hello  ".to_string())
        );

        let result = db.query("SELECT RTRIM(s) FROM t").unwrap();
        assert_eq!(
            result.rows[0].values[0],
            Value::Text("  hello".to_string())
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn scalar_nullif() {
        let db_path = "/tmp/rsqlite_db_nullif.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 5, 5)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 5, 3)").unwrap();

        let result = db.query("SELECT NULLIF(a, b) FROM t").unwrap();
        use rsqlite_storage::codec::Value;
        assert_eq!(result.rows[0].values[0], Value::Null);
        assert_eq!(result.rows[1].values[0], Value::Integer(5));

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn create_index() {
        let db_path = "/tmp/rsqlite_db_create_index.db";
        let _ = std::fs::remove_file(db_path);

        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut db = Database::create(&vfs, db_path).unwrap();

        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        )
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

        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
        )
        .unwrap();

        db.execute("INSERT INTO t VALUES (1, 'Alice', 90)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob', 80)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (3, 'Charlie', 70)")
            .unwrap();

        db.execute("CREATE INDEX idx_t_score ON t(score)")
            .unwrap();

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
            Ok(o) if o.status.success() => {
                String::from_utf8_lossy(&o.stdout).to_string()
            }
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
            Ok(o) if o.status.success() => {
                String::from_utf8_lossy(&o.stdout).to_string()
            }
            _ => {
                let _ = std::fs::remove_file(db_path);
                return;
            }
        };

        assert!(
            output.trim() == "ok",
            "integrity_check failed: {output}"
        );

        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn index_scan_basic() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
            .unwrap();
        db.execute("CREATE INDEX idx_name ON users(name)")
            .unwrap();

        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (4, 'Alice', 28)")
            .unwrap();

        let result = db.query("SELECT id, name, age FROM users WHERE name = 'Alice'").unwrap();
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
        db.execute("CREATE INDEX idx_code ON items(code)")
            .unwrap();

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

        let result = db.query("SELECT player FROM scores WHERE level = 5").unwrap();
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
        assert_eq!(result.columns, vec!["cid", "name", "type", "notnull", "dflt_value", "pk"]);
        assert_eq!(result.rows.len(), 3);

        // cid=0, name=id, type=INTEGER, notnull=1, dflt_value=NULL, pk=1
        assert_eq!(result.rows[0].values[0], crate::types::Value::Integer(0));
        assert_eq!(result.rows[0].values[1], crate::types::Value::Text("id".to_string()));
        assert_eq!(result.rows[0].values[2], crate::types::Value::Text("INTEGER".to_string()));
        assert_eq!(result.rows[0].values[3], crate::types::Value::Integer(1)); // not null (PK)
        assert_eq!(result.rows[0].values[5], crate::types::Value::Integer(1)); // pk

        // cid=1, name=name, type=TEXT, notnull=1, pk=0
        assert_eq!(result.rows[1].values[1], crate::types::Value::Text("name".to_string()));
        assert_eq!(result.rows[1].values[2], crate::types::Value::Text("TEXT".to_string()));
        assert_eq!(result.rows[1].values[3], crate::types::Value::Integer(1)); // NOT NULL
        assert_eq!(result.rows[1].values[5], crate::types::Value::Integer(0));

        // cid=2, name=age, type=INTEGER, notnull=0, pk=0
        assert_eq!(result.rows[2].values[1], crate::types::Value::Text("age".to_string()));
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
        assert_eq!(result.rows[0].values[1], crate::types::Value::Text("id".to_string()));
        assert_eq!(result.rows[1].values[1], crate::types::Value::Text("code".to_string()));
    }

    #[test]
    fn pragma_table_list() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE alpha (id INTEGER PRIMARY KEY)").unwrap();
        db.execute("CREATE TABLE beta (id INTEGER PRIMARY KEY)").unwrap();

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
        assert_eq!(result.columns, vec!["seq", "name", "unique", "origin", "partial"]);
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
        assert_eq!(result.rows[0].values[2], crate::types::Value::Text("name".to_string()));
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)").unwrap();
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
        assert_eq!(result.rows[0].values[1], crate::types::Value::Text("main".to_string()));
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
        assert!(tables.rows.iter().all(|r| r.values[1] != crate::types::Value::Text("t1".to_string())));
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

        let result = db.query("SELECT name AS person, age AS years FROM t ORDER BY years").unwrap();
        assert_eq!(result.columns, vec!["person", "years"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values[0], crate::types::Value::Text("Bob".to_string()));
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

        let result = db.query("SELECT cat, COUNT(*) AS cnt, SUM(val) AS total FROM t GROUP BY cat ORDER BY cnt").unwrap();
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

        let r = db.query("SELECT name FROM t WHERE name LIKE 'Al%'").unwrap();
        assert_eq!(r.rows.len(), 2);

        let r = db.query("SELECT name FROM t WHERE name LIKE '%ob%'").unwrap();
        assert_eq!(r.rows.len(), 1);

        let r = db.query("SELECT name FROM t WHERE name LIKE '_o%'").unwrap();
        assert_eq!(r.rows.len(), 1);

        let r = db.query("SELECT name FROM t WHERE name NOT LIKE 'A%'").unwrap();
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

        let r = db.query("SELECT val FROM t WHERE val BETWEEN 3 AND 7").unwrap();
        assert_eq!(r.rows.len(), 5);

        let r = db.query("SELECT val FROM t WHERE val NOT BETWEEN 3 AND 7").unwrap();
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

        let r = db.query("SELECT name FROM t WHERE name IN ('Alice', 'Bob')").unwrap();
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
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("young".to_string()));
        assert_eq!(r.rows[1].values[1], crate::types::Value::Text("mid".to_string()));
        assert_eq!(r.rows[2].values[1], crate::types::Value::Text("senior".to_string()));
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
        db.execute("INSERT INTO t VALUES (1, 'John', 'Doe')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Jane', 'Smith')").unwrap();

        let r = db.query("SELECT first || ' ' || last AS full_name FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("John Doe".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("Jane Smith".to_string()));
    }

    #[test]
    fn cast_expression() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, '42')").unwrap();

        let r = db.query("SELECT CAST(val AS INTEGER) AS num FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(42));

        let r = db.query("SELECT CAST(id AS TEXT) AS txt FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("1".to_string()));

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
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("Alice".to_string()));
        assert_eq!(r.rows[0].values[2], crate::types::Value::Null);
        assert_eq!(r.rows[1].values[0], crate::types::Value::Integer(2));
        assert_eq!(r.rows[1].values[1], crate::types::Value::Text("Bob".to_string()));
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

        db.execute("UPDATE t SET score = 95.5 WHERE id = 1").unwrap();
        let r = db.query("SELECT name, score FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows[0].values[1], crate::types::Value::Real(95.5));
    }

    #[test]
    fn alter_table_add_column_duplicate_error() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();

        let err = db.execute("ALTER TABLE t ADD COLUMN name TEXT").unwrap_err();
        assert!(err.to_string().contains("duplicate column name"));
    }

    #[test]
    fn alter_table_add_column_no_type() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

        db.execute("ALTER TABLE t ADD COLUMN extra").unwrap();

        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        let r = db.query("SELECT extra FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("hello".to_string()));
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
        assert_eq!(r.rows[2].values[1], crate::types::Value::Text("age".to_string()));
        assert_eq!(r.rows[2].values[2], crate::types::Value::Text("INTEGER".to_string()));
    }

    #[test]
    fn alter_table_nonexistent_error() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        let err = db.execute("ALTER TABLE nonexistent ADD COLUMN x TEXT").unwrap_err();
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
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("Alice".to_string()));
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
        db.execute("INSERT INTO users VALUES (3, 'Charlie')").unwrap();
        db.execute("INSERT INTO orders VALUES (1, 1, 10.0)").unwrap();
        db.execute("INSERT INTO orders VALUES (2, 1, 20.0)").unwrap();
        db.execute("INSERT INTO orders VALUES (3, 2, 30.0)").unwrap();

        let r = db.query("SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)").unwrap();
        assert_eq!(r.rows.len(), 2);
        let names: Vec<String> = r.rows.iter().map(|row| {
            if let crate::types::Value::Text(s) = &row.values[0] { s.clone() } else { String::new() }
        }).collect();
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
        db.execute("INSERT INTO users VALUES (3, 'Charlie')").unwrap();
        db.execute("INSERT INTO orders VALUES (1, 1)").unwrap();
        db.execute("INSERT INTO orders VALUES (2, 2)").unwrap();

        let r = db.query("SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders)").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Charlie".to_string()));
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

        let r = db.query("SELECT val FROM t1 WHERE EXISTS (SELECT 1 FROM t2)").unwrap();
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

        let r = db.query("SELECT val FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2)").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("a".to_string()));
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

        let r = db.query("SELECT (SELECT COUNT(*) FROM t) AS cnt FROM t LIMIT 1").unwrap();
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
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Alice".to_string()));

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

        let r = db.query("SELECT name, counter FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Alice".to_string()));
        assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(2));
    }

    #[test]
    fn upsert_do_update_set_name() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();

        db.execute(
            "INSERT INTO t VALUES (1, 'Bob') ON CONFLICT(id) DO UPDATE SET name = 'Updated'",
        )
        .unwrap();

        let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Updated".to_string()));
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
        db.execute("INSERT INTO old_name VALUES (1, 'hello')").unwrap();

        db.execute("ALTER TABLE old_name RENAME TO new_name").unwrap();

        let r = db.query("SELECT val FROM new_name WHERE id = 1").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("hello".to_string()));

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
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("hello".to_string()));

        let r = db.query("PRAGMA index_list(t2)").unwrap();
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn alter_table_rename_nonexistent_error() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        let err = db.execute("ALTER TABLE nonexistent RENAME TO foo").unwrap_err();
        assert!(err.to_string().contains("no such table"));
    }

    #[test]
    fn alter_table_rename_conflict_error() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)").unwrap();
        db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)").unwrap();

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
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("hello world".to_string()));
    }

    #[test]
    fn select_without_from_functions() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        let r = db.query("SELECT TYPEOF(42), TYPEOF('hi'), TYPEOF(NULL)").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("integer".to_string()));
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("text".to_string()));
        assert_eq!(r.rows[0].values[2], crate::types::Value::Text("null".to_string()));
    }

    #[test]
    fn select_without_from_coalesce() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        let r = db.query("SELECT COALESCE(NULL, NULL, 'found')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("found".to_string()));
    }

    #[test]
    fn create_view_and_select() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)").unwrap();

        db.execute("CREATE VIEW adults AS SELECT id, name, age FROM users WHERE age >= 30").unwrap();

        let r = db.query("SELECT name, age FROM adults").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Alice".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("Charlie".to_string()));
    }

    #[test]
    fn view_with_filter() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)").unwrap();
        db.execute("INSERT INTO items VALUES (1, 'Widget', 9.99)").unwrap();
        db.execute("INSERT INTO items VALUES (2, 'Gadget', 24.99)").unwrap();
        db.execute("INSERT INTO items VALUES (3, 'Doohickey', 4.99)").unwrap();

        db.execute("CREATE VIEW expensive AS SELECT * FROM items WHERE price > 10.0").unwrap();

        let r = db.query("SELECT name FROM expensive WHERE name LIKE 'G%'").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Gadget".to_string()));
    }

    #[test]
    fn view_with_aggregation() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount REAL)").unwrap();
        db.execute("INSERT INTO sales VALUES (1, 'A', 100.0)").unwrap();
        db.execute("INSERT INTO sales VALUES (2, 'B', 200.0)").unwrap();
        db.execute("INSERT INTO sales VALUES (3, 'A', 150.0)").unwrap();

        db.execute("CREATE VIEW product_totals AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product").unwrap();

        let r = db.query("SELECT * FROM product_totals ORDER BY product").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("A".to_string()));
        assert_eq!(r.rows[0].values[1], crate::types::Value::Real(250.0));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("B".to_string()));
        assert_eq!(r.rows[1].values[1], crate::types::Value::Real(200.0));
    }

    #[test]
    fn drop_view() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
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

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
        db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
        let r = db.execute("CREATE VIEW v AS SELECT * FROM t");
        assert!(r.is_err());
    }

    #[test]
    fn view_with_join() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)").unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
        db.execute("INSERT INTO orders VALUES (1, 1, 50.0)").unwrap();
        db.execute("INSERT INTO orders VALUES (2, 1, 75.0)").unwrap();
        db.execute("INSERT INTO orders VALUES (3, 2, 100.0)").unwrap();

        db.execute("CREATE VIEW user_orders AS SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id").unwrap();

        let r = db.query("SELECT * FROM user_orders ORDER BY total").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Alice".to_string()));
        assert_eq!(r.rows[0].values[1], crate::types::Value::Real(50.0));
    }

    #[test]
    fn cte_basic() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)").unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)").unwrap();

        let r = db.query("WITH adults AS (SELECT * FROM users WHERE age >= 30) SELECT name FROM adults ORDER BY name").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Alice".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("Charlie".to_string()));
    }

    #[test]
    fn cte_with_column_names() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

        let r = db.query("WITH renamed(x, y) AS (SELECT id, val FROM t) SELECT x, y FROM renamed").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("hello".to_string()));
    }

    #[test]
    fn cte_multiple() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)").unwrap();
        db.execute("INSERT INTO items VALUES (1, 'A', 10.0)").unwrap();
        db.execute("INSERT INTO items VALUES (2, 'B', 20.0)").unwrap();
        db.execute("INSERT INTO items VALUES (3, 'C', 30.0)").unwrap();

        let r = db.query(
            "WITH cheap AS (SELECT * FROM items WHERE price <= 15.0), \
             expensive AS (SELECT * FROM items WHERE price >= 25.0) \
             SELECT name FROM cheap UNION ALL SELECT name FROM expensive ORDER BY name"
        ).unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("A".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("C".to_string()));
    }

    #[test]
    fn cte_with_aggregation() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount REAL)").unwrap();
        db.execute("INSERT INTO sales VALUES (1, 'North', 100.0)").unwrap();
        db.execute("INSERT INTO sales VALUES (2, 'South', 200.0)").unwrap();
        db.execute("INSERT INTO sales VALUES (3, 'North', 150.0)").unwrap();

        let r = db.query(
            "WITH totals AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) \
             SELECT * FROM totals WHERE total > 200.0"
        ).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("North".to_string()));
        assert_eq!(r.rows[0].values[1], crate::types::Value::Real(250.0));
    }

    #[test]
    fn glob_function_star() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'alice')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'Bob')").unwrap();
        db.execute("INSERT INTO t VALUES (4, 'ALICE')").unwrap();

        // GLOB is case-sensitive, * matches any sequence
        let r = db.query("SELECT name FROM t WHERE glob('A*', name) ORDER BY name").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("ALICE".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("Alice".to_string()));
    }

    #[test]
    fn glob_function_question_mark() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'cat')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'car')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'card')").unwrap();
        db.execute("INSERT INTO t VALUES (4, 'ca')").unwrap();

        // ? matches exactly one character
        let r = db.query("SELECT name FROM t WHERE glob('ca?', name) ORDER BY name").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("car".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("cat".to_string()));
    }

    #[test]
    fn glob_function_char_class() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a1')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b2')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'c3')").unwrap();
        db.execute("INSERT INTO t VALUES (4, 'd4')").unwrap();

        // [a-c] matches a, b, or c
        let r = db.query("SELECT val FROM t WHERE glob('[a-c]*', val) ORDER BY val").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("a1".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("b2".to_string()));
        assert_eq!(r.rows[2].values[0], crate::types::Value::Text("c3".to_string()));
    }

    // ── Index range scan tests ─────────────────────────────

    fn setup_range_scan_db() -> Database {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, price INTEGER, name TEXT)").unwrap();
        db.execute("CREATE INDEX idx_price ON items(price)").unwrap();
        db.execute("INSERT INTO items VALUES (1, 10, 'apple')").unwrap();
        db.execute("INSERT INTO items VALUES (2, 20, 'banana')").unwrap();
        db.execute("INSERT INTO items VALUES (3, 30, 'cherry')").unwrap();
        db.execute("INSERT INTO items VALUES (4, 40, 'date')").unwrap();
        db.execute("INSERT INTO items VALUES (5, 50, 'elderberry')").unwrap();
        db
    }

    #[test]
    fn index_range_scan_greater_than() {
        let mut db = setup_range_scan_db();
        let r = db.query("SELECT name FROM items WHERE price > 30 ORDER BY price").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("date".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("elderberry".to_string()));
    }

    #[test]
    fn index_range_scan_greater_equal() {
        let mut db = setup_range_scan_db();
        let r = db.query("SELECT name FROM items WHERE price >= 30 ORDER BY price").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("cherry".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("date".to_string()));
        assert_eq!(r.rows[2].values[0], crate::types::Value::Text("elderberry".to_string()));
    }

    #[test]
    fn index_range_scan_less_than() {
        let mut db = setup_range_scan_db();
        let r = db.query("SELECT name FROM items WHERE price < 30 ORDER BY price").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("apple".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("banana".to_string()));
    }

    #[test]
    fn index_range_scan_less_equal() {
        let mut db = setup_range_scan_db();
        let r = db.query("SELECT name FROM items WHERE price <= 30 ORDER BY price").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("apple".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("banana".to_string()));
        assert_eq!(r.rows[2].values[0], crate::types::Value::Text("cherry".to_string()));
    }

    #[test]
    fn index_range_scan_both_bounds() {
        let mut db = setup_range_scan_db();
        let r = db.query("SELECT name FROM items WHERE price > 10 AND price < 50 ORDER BY price").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("banana".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("cherry".to_string()));
        assert_eq!(r.rows[2].values[0], crate::types::Value::Text("date".to_string()));
    }

    #[test]
    fn index_range_scan_between() {
        let mut db = setup_range_scan_db();
        let r = db.query("SELECT name FROM items WHERE price BETWEEN 20 AND 40 ORDER BY price").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("banana".to_string()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("cherry".to_string()));
        assert_eq!(r.rows[2].values[0], crate::types::Value::Text("date".to_string()));
    }

    #[test]
    fn index_range_scan_no_matches() {
        let mut db = setup_range_scan_db();
        let r = db.query("SELECT name FROM items WHERE price > 100").unwrap();
        assert_eq!(r.rows.len(), 0);
    }

    #[test]
    fn index_range_scan_all_match() {
        let mut db = setup_range_scan_db();
        let r = db.query("SELECT name FROM items WHERE price >= 10 AND price <= 50 ORDER BY price").unwrap();
        assert_eq!(r.rows.len(), 5);
    }

    // ── Parameter binding tests ────────────────────────────

    #[test]
    fn param_binding_select() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();

        let r = db.query_with_params(
            "SELECT name FROM t WHERE id = ?",
            vec![rsqlite_storage::codec::Value::Integer(2)],
        ).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("bob".to_string()));
    }

    #[test]
    fn param_binding_insert() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();

        db.execute_with_params(
            "INSERT INTO t VALUES (?, ?)",
            vec![
                rsqlite_storage::codec::Value::Integer(1),
                rsqlite_storage::codec::Value::Text("charlie".to_string()),
            ],
        ).unwrap();

        let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("charlie".to_string()));
    }

    #[test]
    fn param_binding_multiple() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

        let r = db.query_with_params(
            "SELECT id FROM t WHERE val > ? AND val < ? ORDER BY id",
            vec![
                rsqlite_storage::codec::Value::Integer(10),
                rsqlite_storage::codec::Value::Integer(30),
            ],
        ).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
    }

    #[test]
    fn insert_select_all_columns() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)").unwrap();
        db.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)").unwrap();
        db.execute("INSERT INTO src VALUES (1, 'a', 10)").unwrap();
        db.execute("INSERT INTO src VALUES (2, 'b', 20)").unwrap();
        db.execute("INSERT INTO src VALUES (3, 'c', 30)").unwrap();
        db.execute("INSERT INTO dst SELECT * FROM src WHERE val > 10").unwrap();
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
        db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)").unwrap();
        db.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, label TEXT)").unwrap();
        db.execute("INSERT INTO src VALUES (1, 'alpha', 10)").unwrap();
        db.execute("INSERT INTO src VALUES (2, 'beta', 20)").unwrap();
        db.execute("INSERT INTO dst (label) SELECT name FROM src ORDER BY name").unwrap();
        let r = db.query("SELECT label FROM dst ORDER BY label").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("alpha".into()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("beta".into()));
    }

    #[test]
    fn insert_select_with_expression() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        db.execute("CREATE TABLE dst (id INTEGER PRIMARY KEY, doubled INTEGER)").unwrap();
        db.execute("INSERT INTO src VALUES (1, 5)").unwrap();
        db.execute("INSERT INTO src VALUES (2, 10)").unwrap();
        db.execute("INSERT INTO dst (doubled) SELECT val * 2 FROM src").unwrap();
        let r = db.query("SELECT doubled FROM dst ORDER BY doubled").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(10));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Integer(20));
    }

    #[test]
    fn group_concat_basic() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, grp TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice', 'a')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'bob', 'a')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'carol', 'b')").unwrap();
        let r = db.query("SELECT grp, GROUP_CONCAT(name) FROM t GROUP BY grp ORDER BY grp").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("alice,bob".into()));
        assert_eq!(r.rows[1].values[1], crate::types::Value::Text("carol".into()));
    }

    #[test]
    fn group_concat_custom_separator() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
        let r = db.query("SELECT GROUP_CONCAT(name, ' | ') FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("a | b | c".into()));
    }

    #[test]
    fn group_concat_distinct() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'x')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'y')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'x')").unwrap();
        let r = db.query("SELECT GROUP_CONCAT(DISTINCT val) FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("x,y".into()));
    }

    #[test]
    fn total_aggregate() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        let r = db.query("SELECT TOTAL(val) FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Real(30.0));

        let r2 = db.query("SELECT TOTAL(val) FROM t WHERE id > 100").unwrap();
        assert_eq!(r2.rows[0].values[0], crate::types::Value::Real(0.0));
    }

    #[test]
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (42, 'test')").unwrap();
        let r = db.query("SELECT LAST_INSERT_ROWID()").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(42));
    }

    #[test]
    fn changes_function() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();
        db.execute("INSERT OR REPLACE INTO t VALUES (1, 'replaced')").unwrap();
        let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("replaced".into()));
    }

    #[test]
    fn replace_into() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
        db.execute("REPLACE INTO t VALUES (1, 'second')").unwrap();
        db.execute("REPLACE INTO t VALUES (2, 'new')").unwrap();
        let r = db.query("SELECT id, name FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("second".into()));
        assert_eq!(r.rows[1].values[1], crate::types::Value::Text("new".into()));
    }

    #[test]
    fn printf_function() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        let r = db.query("SELECT PRINTF('hello %s, you are %d', 'world', 42)").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("hello world, you are 42".into()));
    }

    #[test]
    fn insert_or_ignore() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
        db.execute("INSERT OR IGNORE INTO t VALUES (1, 'ignored')").unwrap();
        db.execute("INSERT OR IGNORE INTO t VALUES (2, 'second')").unwrap();
        let r = db.query("SELECT id, name FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("first".into()));
        assert_eq!(r.rows[1].values[1], crate::types::Value::Text("second".into()));
    }

    #[test]
    fn order_by_column_number() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)").unwrap();
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
        db.execute("CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)").unwrap();
        db.execute("INSERT INTO src VALUES (1, 'alice', 100)").unwrap();
        db.execute("INSERT INTO src VALUES (2, 'bob', 200)").unwrap();
        db.execute("CREATE TABLE dst AS SELECT name, val FROM src WHERE val > 100").unwrap();
        let r = db.query("SELECT name, val FROM dst").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("bob".into()));
        assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(200));
    }

    #[test]
    fn create_table_as_select_if_not_exists() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'test')").unwrap();
        db.execute("CREATE TABLE IF NOT EXISTS t AS SELECT * FROM t").unwrap();
        let r = db.query("SELECT * FROM t").unwrap();
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn date_function_literal() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        let r = db.query("SELECT DATE('2024-06-15')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("2024-06-15".into()));
    }

    #[test]
    fn date_function_with_modifier() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        let r = db.query("SELECT DATE('2024-01-31', '+1 month')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("2024-02-29".into()));
    }

    #[test]
    fn datetime_function() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        let r = db.query("SELECT DATETIME('2024-06-15 10:30:00', '+2 hours')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("2024-06-15 12:30:00".into()));
    }

    #[test]
    fn time_function() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        let r = db.query("SELECT TIME('2024-06-15 10:30:45')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("10:30:45".into()));
    }

    #[test]
    fn strftime_function() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        let r = db.query("SELECT STRFTIME('%Y/%m/%d', '2024-06-15')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("2024/06/15".into()));
    }

    #[test]
    fn date_start_of_year() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        let r = db.query("SELECT DATE('2024-06-15', 'start of year')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("2024-01-01".into()));
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
        db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, event_date TEXT)").unwrap();
        db.execute("INSERT INTO events VALUES (1, 'start', '2024-01-15')").unwrap();
        db.execute("INSERT INTO events VALUES (2, 'middle', '2024-06-15')").unwrap();
        db.execute("INSERT INTO events VALUES (3, 'end', '2024-12-15')").unwrap();
        let r = db.query("SELECT name FROM events WHERE event_date > DATE('2024-06-01') ORDER BY event_date").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("middle".into()));
        assert_eq!(r.rows[1].values[0], crate::types::Value::Text("end".into()));
    }

    #[test]
    fn group_by_column_number() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b', 20)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'a', 30)").unwrap();
        let r = db.query("SELECT grp, SUM(val) FROM t GROUP BY 1 ORDER BY 1").unwrap();
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
        db.execute("CREATE TABLE v (id INTEGER PRIMARY KEY, vec BLOB)").unwrap();
        let v1 = make_f32_blob(&[1.0, 0.0, 0.0]);
        let v2 = make_f32_blob(&[0.0, 1.0, 0.0]);
        let v3 = make_f32_blob(&[1.0, 0.0, 0.0]);
        db.execute_with_params("INSERT INTO v VALUES (1, ?)", vec![crate::types::Value::Blob(v1)]).unwrap();
        db.execute_with_params("INSERT INTO v VALUES (2, ?)", vec![crate::types::Value::Blob(v2)]).unwrap();
        db.execute_with_params("INSERT INTO v VALUES (3, ?)", vec![crate::types::Value::Blob(v3)]).unwrap();

        let query_vec = crate::types::Value::Blob(make_f32_blob(&[1.0, 0.0, 0.0]));
        let r = db.query_with_params(
            "SELECT id, vec_distance_cosine(vec, ?) AS dist FROM v ORDER BY dist",
            vec![query_vec],
        ).unwrap();
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
        let r = db.query_with_params("SELECT vec_distance_l2(?, ?)", vec![v1, v2]).unwrap();
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
        let r = db.query_with_params("SELECT vec_distance_dot(?, ?)", vec![v1, v2]).unwrap();
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
        let r = db.query_with_params("SELECT vec_length(?)", vec![v]).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(4));
    }

    #[test]
    fn vec_normalize_basic() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        let v = crate::types::Value::Blob(make_f32_blob(&[3.0, 4.0]));
        let r = db.query_with_params("SELECT vec_normalize(?)", vec![v]).unwrap();
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
        let r = db.query("SELECT vec_to_json(vec_from_json('[1.5, 2.5, 3.5]'))").unwrap();
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
        db.execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, text TEXT, vector BLOB)").unwrap();
        let cat_vec = crate::types::Value::Blob(make_f32_blob(&[1.0, 0.0, 0.0]));
        let dog_vec = crate::types::Value::Blob(make_f32_blob(&[0.9, 0.1, 0.0]));
        let car_vec = crate::types::Value::Blob(make_f32_blob(&[0.0, 0.0, 1.0]));
        db.execute_with_params("INSERT INTO embeddings VALUES (1, 'cat', ?)", vec![cat_vec]).unwrap();
        db.execute_with_params("INSERT INTO embeddings VALUES (2, 'dog', ?)", vec![dog_vec]).unwrap();
        db.execute_with_params("INSERT INTO embeddings VALUES (3, 'car', ?)", vec![car_vec]).unwrap();

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
        db.execute("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)").unwrap();
        db.execute("INSERT INTO emp VALUES (1, 'Alice', 'eng', 100)").unwrap();
        db.execute("INSERT INTO emp VALUES (2, 'Bob', 'eng', 120)").unwrap();
        db.execute("INSERT INTO emp VALUES (3, 'Carol', 'sales', 90)").unwrap();
        db.execute("INSERT INTO emp VALUES (4, 'Dave', 'sales', 110)").unwrap();
        db.execute("INSERT INTO emp VALUES (5, 'Eve', 'eng', 100)").unwrap();
        db
    }

    #[test]
    fn window_row_number() {
        let mut db = setup_employees();
        let r = db.query(
            "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM emp ORDER BY rn"
        ).unwrap();
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
        let r = db.query(
            "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn \
             FROM emp ORDER BY dept, rn"
        ).unwrap();
        assert_eq!(r.rows.len(), 5);
        // eng partition first (alphabetical): Bob(120)=1, then Alice/Eve
        let eng_rows: Vec<_> = r.rows.iter()
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
        let r = db.query(
            "SELECT name, salary, LAG(salary, 1, 0) OVER (ORDER BY salary) AS prev_sal, \
             LEAD(salary, 1, 0) OVER (ORDER BY salary) AS next_sal FROM emp ORDER BY salary"
        ).unwrap();
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
        let r = db.query("SELECT name, COUNT(*) OVER () AS total FROM emp").unwrap();
        assert_eq!(r.rows.len(), 5);
        for row in &r.rows {
            assert_eq!(row.values[1], crate::types::Value::Integer(5));
        }
    }

    #[test]
    fn window_ntile() {
        let mut db = setup_employees();
        let r = db.query("SELECT name, NTILE(2) OVER (ORDER BY salary) AS bucket FROM emp").unwrap();
        assert_eq!(r.rows.len(), 5);
        // 5 rows in 2 buckets: 3 in bucket 1, 2 in bucket 2
        let bucket1_count = r.rows.iter().filter(|r| r.values[1] == crate::types::Value::Integer(1)).count();
        let bucket2_count = r.rows.iter().filter(|r| r.values[1] == crate::types::Value::Integer(2)).count();
        assert!(bucket1_count > 0 && bucket2_count > 0, "both buckets should have rows");
    }

    #[test]
    fn window_first_last_value() {
        let mut db = setup_employees();
        let r = db.query(
            "SELECT name, FIRST_VALUE(name) OVER (ORDER BY salary DESC) AS first_n, \
             LAST_VALUE(name) OVER (ORDER BY salary DESC) AS last_n FROM emp"
        ).unwrap();
        assert_eq!(r.rows.len(), 5);
        // First value should be Bob (highest salary)
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("Bob".into()));
    }

    #[test]
    fn window_multiple_functions() {
        let mut db = setup_employees();
        let r = db.query(
            "SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn, \
             COUNT(*) OVER () AS total FROM emp"
        ).unwrap();
        assert_eq!(r.rows.len(), 5);
        for row in &r.rows {
            assert_eq!(row.values[2], crate::types::Value::Integer(5));
        }
        assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(1));
    }

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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        let r = db.query("SELECT nonexistent FROM t");
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("unknown column"));
    }

    #[test]
    fn error_insert_named_nonexistent_column() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();

        db.execute("BEGIN").unwrap();
        db.execute("UPDATE t SET val = 'modified' WHERE id = 1").unwrap();
        let r = db.query("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("modified".into()));
        db.execute("ROLLBACK").unwrap();

        let r = db.query("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("original".into()));
    }

    // --- View edge cases ---

    #[test]
    fn view_with_aggregation_complex() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount INTEGER)").unwrap();
        db.execute("INSERT INTO sales VALUES (1, 'A', 100)").unwrap();
        db.execute("INSERT INTO sales VALUES (2, 'B', 200)").unwrap();
        db.execute("INSERT INTO sales VALUES (3, 'A', 150)").unwrap();
        db.execute("CREATE VIEW product_totals AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product").unwrap();

        let r = db.query("SELECT * FROM product_totals ORDER BY product").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("A".into()));
        assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(250));
    }

    // --- Large data test ---

    #[test]
    fn insert_100_rows_and_query() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();

        for i in 0..100 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 2)).unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();

        // Test multiple CTEs in sequence
        let r = db.query(
            "WITH doubled AS (SELECT id, val * 2 AS dval FROM t), \
             tripled AS (SELECT id, val * 3 AS tval FROM t) \
             SELECT d.dval FROM doubled AS d WHERE d.id = 1"
        ).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(20));
    }

    // --- Index edge cases ---

    #[test]
    fn index_with_null_values() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("CREATE INDEX idx_name ON t(name)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'Bob')").unwrap();

        let r = db.query("SELECT id FROM t WHERE name = 'Alice'").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));

        let r = db.query("SELECT COUNT(*) FROM t WHERE name IS NULL").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
    }

    #[test]
    fn index_after_delete() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
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
        let r = db.query(r#"SELECT json_extract('{"name":"Alice","age":30}', '$.name')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Alice".into()));

        let r = db.query(r#"SELECT json_extract('{"name":"Alice","age":30}', '$.age')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(30));

        let r = db.query(r#"SELECT json_extract('{"a":{"b":[10,20,30]}}', '$.a.b[1]')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(20));
    }

    #[test]
    fn json_extract_missing_path() {
        let mut db = setup_json_db();
        let r = db.query(r#"SELECT json_extract('{"a":1}', '$.b')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
    }

    #[test]
    fn json_type_function() {
        let mut db = setup_json_db();
        let r = db.query(r#"SELECT json_type('{"a":1}')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("object".into()));

        let r = db.query(r#"SELECT json_type('[1,2]')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("array".into()));

        let r = db.query(r#"SELECT json_type('{"a":1}', '$.a')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("real".into()));

        let r = db.query(r#"SELECT json_type('{"a":"hello"}', '$.a')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("text".into()));
    }

    #[test]
    fn json_minify() {
        let mut db = setup_json_db();
        let r = db.query(r#"SELECT json('  { "a" : 1 , "b" : [2, 3] }  ')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"a":1,"b":[2,3]}"#.into()));
    }

    #[test]
    fn json_array_function() {
        let mut db = setup_json_db();
        let r = db.query("SELECT json_array(1, 'hello', NULL)").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"[1,"hello",null]"#.into()));

        let r = db.query("SELECT json_array()").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("[]".into()));
    }

    #[test]
    fn json_object_function() {
        let mut db = setup_json_db();
        let r = db.query("SELECT json_object('name', 'Alice', 'age', 30)").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"name":"Alice","age":30}"#.into()));
    }

    #[test]
    fn json_array_length_function() {
        let mut db = setup_json_db();
        let r = db.query("SELECT json_array_length('[1,2,3,4]')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(4));

        let r = db.query("SELECT json_array_length('[]')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(0));

        let r = db.query(r#"SELECT json_array_length('{"a":[10,20]}', '$.a')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
    }

    #[test]
    fn json_quote_function() {
        let mut db = setup_json_db();
        let r = db.query("SELECT json_quote('hello')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#""hello""#.into()));

        let r = db.query("SELECT json_quote(42)").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("42".into()));

        let r = db.query("SELECT json_quote(NULL)").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("null".into()));
    }

    #[test]
    fn json_with_table_data() {
        let mut db = setup_json_db();
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, data TEXT)").unwrap();
        db.execute(r#"INSERT INTO docs VALUES (1, '{"name":"Alice","scores":[95,87,92]}')"#).unwrap();
        db.execute(r#"INSERT INTO docs VALUES (2, '{"name":"Bob","scores":[78,85,90]}')"#).unwrap();

        let r = db.query("SELECT id, json_extract(data, '$.name') AS name FROM docs ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].values[1], crate::types::Value::Text("Alice".into()));
        assert_eq!(r.rows[1].values[1], crate::types::Value::Text("Bob".into()));

        let r = db.query("SELECT id, json_extract(data, '$.scores[0]') AS top FROM docs ORDER BY id").unwrap();
        assert_eq!(r.rows[0].values[1], crate::types::Value::Integer(95));
        assert_eq!(r.rows[1].values[1], crate::types::Value::Integer(78));
    }

    #[test]
    fn json_extract_nested_object() {
        let mut db = setup_json_db();
        let r = db.query(r#"SELECT json_extract('{"user":{"addr":{"city":"NYC"}}}', '$.user.addr.city')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("NYC".into()));
    }

    #[test]
    fn json_extract_returns_subobject() {
        let mut db = setup_json_db();
        let r = db.query(r#"SELECT json_extract('{"a":{"b":1}}', '$.a')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"b":1}"#.into()));
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
        let r = db.query(r#"SELECT json_insert('{"a":1}', '$.b', 2)"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"a":1,"b":2}"#.into()));

        // json_insert does NOT replace existing keys
        let r = db.query(r#"SELECT json_insert('{"a":1}', '$.a', 99)"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"a":1}"#.into()));
    }

    #[test]
    fn json_replace_function() {
        let mut db = setup_json_db();
        let r = db.query(r#"SELECT json_replace('{"a":1}', '$.a', 99)"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"a":99}"#.into()));

        // json_replace does NOT insert new keys
        let r = db.query(r#"SELECT json_replace('{"a":1}', '$.b', 2)"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"a":1}"#.into()));
    }

    #[test]
    fn json_set_function() {
        let mut db = setup_json_db();
        // json_set inserts AND replaces
        let r = db.query(r#"SELECT json_set('{"a":1}', '$.a', 99)"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"a":99}"#.into()));

        let r = db.query(r#"SELECT json_set('{"a":1}', '$.b', 2)"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"a":1,"b":2}"#.into()));
    }

    #[test]
    fn json_remove_function() {
        let mut db = setup_json_db();
        let r = db.query(r#"SELECT json_remove('{"a":1,"b":2}', '$.a')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"b":2}"#.into()));

        let r = db.query("SELECT json_remove('[1,2,3]', '$[1]')").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("[1,3]".into()));
    }

    #[test]
    fn json_patch_function() {
        let mut db = setup_json_db();
        let r = db.query(r#"SELECT json_patch('{"a":1,"b":2}', '{"b":3,"c":4}')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"a":1,"b":3,"c":4}"#.into()));

        // null in patch removes key
        let r = db.query(r#"SELECT json_patch('{"a":1,"b":2}', '{"a":null}')"#).unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text(r#"{"b":2}"#.into()));
    }

    // ───────────────────── NOT NULL constraint tests ─────────────────────

    #[test]
    fn not_null_insert_violation() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        let r = db.execute("INSERT INTO t (id) VALUES (1)");
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(err.contains("NOT NULL"), "Expected NOT NULL error, got: {err}");
    }

    #[test]
    fn not_null_insert_explicit_null() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        let r = db.execute("INSERT INTO t VALUES (1, NULL)");
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(err.contains("NOT NULL"), "Expected NOT NULL error, got: {err}");
    }

    #[test]
    fn not_null_insert_valid() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        let r = db.query("SELECT name FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("Alice".into()));
    }

    #[test]
    fn not_null_update_violation() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        let r = db.execute("UPDATE t SET name = NULL WHERE id = 1");
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(err.contains("NOT NULL"), "Expected NOT NULL error, got: {err}");
    }

    #[test]
    fn not_null_nullable_column_allows_null() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, email TEXT)").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice@test.com')").unwrap();
        let r = db.execute("INSERT INTO t VALUES (2, 'alice@test.com')");
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(err.contains("UNIQUE"), "Expected UNIQUE error, got: {err}");
    }

    #[test]
    fn unique_insert_different_values_ok() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice@test.com')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'bob@test.com')").unwrap();
        let r = db.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
    }

    #[test]
    fn unique_null_allowed_multiple() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").unwrap();
        db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
        let r = db.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
    }

    #[test]
    fn unique_update_violation() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice@test.com')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'bob@test.com')").unwrap();
        let r = db.execute("UPDATE t SET email = 'alice@test.com' WHERE id = 2");
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(err.contains("UNIQUE"), "Expected UNIQUE error, got: {err}");
    }

    #[test]
    fn unique_update_same_row_ok() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice@test.com')").unwrap();
        db.execute("UPDATE t SET email = 'alice@test.com' WHERE id = 1").unwrap();
        let r = db.query("SELECT email FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Text("alice@test.com".into()));
    }

    // ───────────────────── CHECK constraint tests ─────────────────────

    #[test]
    fn check_constraint_insert_violation() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))").unwrap();
        db.execute("INSERT INTO t VALUES (1, 0)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 100)").unwrap();
        let r = db.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(2));
    }

    #[test]
    fn check_constraint_update_violation() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0))").unwrap();
        db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
        let r = db.query("SELECT age FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Null);
    }

    #[test]
    fn check_constraint_table_level() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, lo INTEGER, hi INTEGER, CHECK(lo <= hi))").unwrap();
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
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        let r = db.query("EXPLAIN QUERY PLAN SELECT * FROM users").unwrap();
        assert_eq!(r.columns, vec!["id", "parent", "notused", "detail"]);
        assert!(!r.rows.is_empty());
        let detail = &r.rows[0].values[3];
        if let crate::types::Value::Text(s) = detail {
            assert!(s.contains("SCAN TABLE users"), "Expected SCAN TABLE users, got: {s}");
        } else {
            panic!("Expected text detail");
        }
    }

    #[test]
    fn explain_query_plan_index() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("CREATE INDEX idx_name ON users(name)").unwrap();
        let r = db.query("EXPLAIN QUERY PLAN SELECT * FROM users WHERE name = 'Alice'").unwrap();
        assert!(!r.rows.is_empty());
        let detail = &r.rows[0].values[3];
        if let crate::types::Value::Text(s) = detail {
            assert!(s.contains("SEARCH TABLE") || s.contains("SCAN TABLE"), "got: {s}");
        } else {
            panic!("Expected text detail");
        }
    }

    #[test]
    fn explain_query_plan_join() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)").unwrap();
        db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)").unwrap();
        let r = db.query("EXPLAIN QUERY PLAN SELECT * FROM a INNER JOIN b ON a.id = b.a_id").unwrap();
        let details: Vec<String> = r.rows.iter().filter_map(|row| {
            if let crate::types::Value::Text(s) = &row.values[3] { Some(s.clone()) } else { None }
        }).collect();
        assert!(details.iter().any(|d| d.contains("JOIN")), "Expected JOIN in plan: {details:?}");
    }

    // ───────────────────── SAVEPOINT tests ─────────────────────

    #[test]
    fn savepoint_rollback_to() {
        let vfs = rsqlite_vfs::memory::MemoryVfs::new();
        let mut db = Database::create(&vfs, "test.db").unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
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
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        db.execute("SAVEPOINT sp1").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        db.execute("ROLLBACK TO sp1").unwrap();

        let r = db.query("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(r.rows[0].values[0], crate::types::Value::Integer(1));
    }

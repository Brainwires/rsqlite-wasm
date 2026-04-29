use rsqlite_storage::pager::Pager;
use rsqlite_vfs::Vfs;
use sqlparser::ast::Statement;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::executor::{self, ExecResult};
use crate::planner;
use crate::types::QueryResult;

pub struct Database {
    pager: Pager,
    catalog: Catalog,
}

impl Database {
    pub fn open(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let mut pager = Pager::open(vfs, path)?;
        let catalog = Catalog::load(&mut pager)?;
        Ok(Self { pager, catalog })
    }

    pub fn create(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let mut pager = Pager::create(vfs, path)?;
        let catalog = Catalog::load(&mut pager)?;
        Ok(Self { pager, catalog })
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            });
        }

        let plan = planner::plan_statement(&stmts[0], &self.catalog)?;
        if let planner::Plan::Pragma { ref name, ref argument } = plan {
            return executor::execute_pragma(name, argument.as_deref(), &self.pager, &self.catalog);
        }
        executor::execute(&plan, &mut self.pager, &self.catalog)
    }

    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(ExecResult { rows_affected: 0 });
        }

        let plan = planner::plan_statement(&stmts[0], &self.catalog)?;
        executor::execute_mut(&plan, &mut self.pager, &mut self.catalog)
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<SqlResult> {
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(SqlResult::Execute(ExecResult { rows_affected: 0 }));
        }

        let stmt = &stmts[0];
        let plan = planner::plan_statement(stmt, &self.catalog)?;

        if let planner::Plan::Pragma { ref name, ref argument } = plan {
            return Ok(SqlResult::Query(executor::execute_pragma(
                name,
                argument.as_deref(),
                &self.pager,
                &self.catalog,
            )?));
        }

        if is_query_statement(stmt) {
            Ok(SqlResult::Query(executor::execute(
                &plan,
                &mut self.pager,
                &self.catalog,
            )?))
        } else {
            Ok(SqlResult::Execute(executor::execute_mut(
                &plan,
                &mut self.pager,
                &mut self.catalog,
            )?))
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
}

pub enum SqlResult {
    Query(QueryResult),
    Execute(ExecResult),
}

fn is_query_statement(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Query(_))
}

#[cfg(test)]
mod tests {
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
}

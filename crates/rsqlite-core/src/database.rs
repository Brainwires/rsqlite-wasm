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
        executor::execute(&plan, &mut self.pager)
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

        if is_query_statement(stmt) {
            Ok(SqlResult::Query(executor::execute(
                &plan,
                &mut self.pager,
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
}

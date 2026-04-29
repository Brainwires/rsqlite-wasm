use rsqlite_storage::pager::Pager;
use rsqlite_vfs::Vfs;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::executor;
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

    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            });
        }

        let plan = planner::plan_query(&stmts[0], &self.catalog)?;
        executor::execute(&plan, &mut self.pager)
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
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
}

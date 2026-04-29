use std::collections::HashMap;

use rsqlite_parser::parse::parse_sql;
use rsqlite_storage::btree::{read_schema, SchemaEntry};
use rsqlite_storage::pager::Pager;
use sqlparser::ast::{self, ColumnOption, Statement};

use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeAffinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

impl TypeAffinity {
    pub fn from_type_name(name: &str) -> Self {
        let upper = name.to_uppercase();
        if upper.contains("INT") {
            Self::Integer
        } else if upper.contains("CHAR")
            || upper.contains("CLOB")
            || upper.contains("TEXT")
            || upper.contains("STRING")
        {
            Self::Text
        } else if upper.contains("BLOB") || upper.is_empty() {
            Self::Blob
        } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            Self::Real
        } else {
            Self::Numeric
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: String,
    pub affinity: TypeAffinity,
    pub is_primary_key: bool,
    pub is_rowid_alias: bool,
    pub nullable: bool,
    pub column_index: usize,
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub root_page: u32,
    pub sql: Option<String>,
}

impl TableDef {
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnDef> {
        let lower = name.to_lowercase();
        self.columns.iter().find(|c| c.name.to_lowercase() == lower)
    }
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub table_name: String,
    pub root_page: u32,
    pub columns: Vec<String>,
    pub sql: Option<String>,
}

#[derive(Debug)]
pub struct Catalog {
    pub tables: HashMap<String, TableDef>,
    pub indexes: HashMap<String, IndexDef>,
}

impl Catalog {
    pub fn load(pager: &mut Pager) -> Result<Self> {
        let schema_entries = read_schema(pager)?;
        let mut tables = HashMap::new();
        let mut indexes = HashMap::new();

        for entry in &schema_entries {
            match entry.entry_type.as_str() {
                "table" => {
                    if let Some(table_def) = parse_table_def(entry)? {
                        tables.insert(table_def.name.to_lowercase(), table_def);
                    }
                }
                "index" => {
                    if let Some(index_def) = parse_index_def(entry)? {
                        indexes.insert(index_def.name.to_lowercase(), index_def);
                    }
                }
                _ => {}
            }
        }

        Ok(Catalog { tables, indexes })
    }

    pub fn get_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(&name.to_lowercase())
    }

    pub fn reload(&mut self, pager: &mut Pager) -> Result<()> {
        let fresh = Self::load(pager)?;
        self.tables = fresh.tables;
        self.indexes = fresh.indexes;
        Ok(())
    }
}

fn parse_table_def(entry: &SchemaEntry) -> Result<Option<TableDef>> {
    let sql = match &entry.sql {
        Some(s) => s,
        None => return Ok(None),
    };

    let stmts = match parse_sql(sql) {
        Ok(s) => s,
        Err(_) => {
            // If we can't parse the SQL, create a minimal table def from the schema entry
            return Ok(Some(TableDef {
                name: entry.name.clone(),
                columns: vec![],
                root_page: entry.rootpage,
                sql: Some(sql.clone()),
            }));
        }
    };

    let stmt = match stmts.into_iter().next() {
        Some(s) => s,
        None => return Ok(None),
    };

    if let Statement::CreateTable(ct) = stmt {
        let mut columns = Vec::new();
        let mut has_pk_in_columns = false;

        // Check for table-level PRIMARY KEY constraint
        let mut table_pk_cols: Vec<String> = Vec::new();
        for constraint in &ct.constraints {
            if let ast::TableConstraint::PrimaryKey { columns: pk_cols, .. } = constraint {
                for col in pk_cols {
                    table_pk_cols.push(col.value.to_lowercase());
                }
            }
        }

        for (i, col) in ct.columns.iter().enumerate() {
            let type_name = col
                .data_type
                .to_string();

            let affinity = TypeAffinity::from_type_name(&type_name);

            let is_pk_inline = col.options.iter().any(|opt| {
                matches!(
                    opt.option,
                    ColumnOption::Unique { is_primary: true, .. }
                )
            });
            let is_pk_from_table = table_pk_cols.contains(&col.name.value.to_lowercase());
            let is_primary_key = is_pk_inline || is_pk_from_table;

            if is_primary_key {
                has_pk_in_columns = true;
            }

            let is_rowid_alias =
                is_primary_key && affinity == TypeAffinity::Integer;

            let nullable = !col.options.iter().any(|opt| {
                matches!(opt.option, ColumnOption::NotNull)
            }) && !is_primary_key;

            columns.push(ColumnDef {
                name: col.name.value.clone(),
                type_name: type_name.clone(),
                affinity,
                is_primary_key,
                is_rowid_alias,
                nullable,
                column_index: i,
            });
        }

        // If no explicit PK, SQLite has an implicit rowid
        let _ = has_pk_in_columns;

        Ok(Some(TableDef {
            name: entry.name.clone(),
            columns,
            root_page: entry.rootpage,
            sql: Some(sql.clone()),
        }))
    } else {
        Ok(None)
    }
}

fn parse_index_def(entry: &SchemaEntry) -> Result<Option<IndexDef>> {
    let sql = match &entry.sql {
        Some(s) => s,
        None => {
            // Autoindex — no SQL
            return Ok(Some(IndexDef {
                name: entry.name.clone(),
                table_name: entry.tbl_name.clone(),
                root_page: entry.rootpage,
                columns: vec![],
                sql: None,
            }));
        }
    };

    let stmts = match parse_sql(sql) {
        Ok(s) => s,
        Err(_) => {
            return Ok(Some(IndexDef {
                name: entry.name.clone(),
                table_name: entry.tbl_name.clone(),
                root_page: entry.rootpage,
                columns: vec![],
                sql: Some(sql.clone()),
            }));
        }
    };

    let stmt = match stmts.into_iter().next() {
        Some(s) => s,
        None => return Ok(None),
    };

    if let Statement::CreateIndex(ci) = stmt {
        let columns: Vec<String> = ci
            .columns
            .iter()
            .map(|c| c.expr.to_string())
            .collect();

        Ok(Some(IndexDef {
            name: entry.name.clone(),
            table_name: entry.tbl_name.clone(),
            root_page: entry.rootpage,
            columns,
            sql: Some(sql.clone()),
        }))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_affinity_rules() {
        assert_eq!(TypeAffinity::from_type_name("INTEGER"), TypeAffinity::Integer);
        assert_eq!(TypeAffinity::from_type_name("INT"), TypeAffinity::Integer);
        assert_eq!(TypeAffinity::from_type_name("TINYINT"), TypeAffinity::Integer);
        assert_eq!(TypeAffinity::from_type_name("BIGINT"), TypeAffinity::Integer);
        assert_eq!(TypeAffinity::from_type_name("TEXT"), TypeAffinity::Text);
        assert_eq!(TypeAffinity::from_type_name("VARCHAR(255)"), TypeAffinity::Text);
        assert_eq!(TypeAffinity::from_type_name("CLOB"), TypeAffinity::Text);
        assert_eq!(TypeAffinity::from_type_name("BLOB"), TypeAffinity::Blob);
        assert_eq!(TypeAffinity::from_type_name(""), TypeAffinity::Blob);
        assert_eq!(TypeAffinity::from_type_name("REAL"), TypeAffinity::Real);
        assert_eq!(TypeAffinity::from_type_name("DOUBLE"), TypeAffinity::Real);
        assert_eq!(TypeAffinity::from_type_name("FLOAT"), TypeAffinity::Real);
        assert_eq!(TypeAffinity::from_type_name("NUMERIC"), TypeAffinity::Numeric);
        assert_eq!(TypeAffinity::from_type_name("BOOLEAN"), TypeAffinity::Numeric);
        assert_eq!(TypeAffinity::from_type_name("DATE"), TypeAffinity::Numeric);
    }

    #[test]
    fn load_catalog_from_real_db() {
        let test_db = "/tmp/rsqlite_catalog_test.db";
        let _ = std::fs::remove_file(test_db);
        let status = std::process::Command::new("sqlite3")
            .arg(test_db)
            .arg(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);\
                 CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT);\
                 CREATE INDEX idx_posts_user ON posts(user_id);",
            )
            .status();

        match status {
            Ok(s) if s.success() => {
                let vfs = rsqlite_vfs::native::NativeVfs::new();
                let mut pager = rsqlite_storage::pager::Pager::open(&vfs, test_db).unwrap();
                let catalog = Catalog::load(&mut pager).unwrap();

                let users = catalog.get_table("users").unwrap();
                assert_eq!(users.columns.len(), 3);
                assert_eq!(users.columns[0].name, "id");
                assert!(users.columns[0].is_primary_key);
                assert!(users.columns[0].is_rowid_alias);
                assert_eq!(users.columns[0].affinity, TypeAffinity::Integer);
                assert_eq!(users.columns[1].name, "name");
                assert!(!users.columns[1].nullable);
                assert_eq!(users.columns[2].name, "age");
                assert!(users.columns[2].nullable);

                let posts = catalog.get_table("posts").unwrap();
                assert_eq!(posts.columns.len(), 3);

                assert!(catalog.indexes.contains_key("idx_posts_user"));
                let idx = &catalog.indexes["idx_posts_user"];
                assert_eq!(idx.table_name, "posts");

                let _ = std::fs::remove_file(test_db);
            }
            _ => {
                eprintln!("sqlite3 not available, skipping catalog test");
            }
        }
    }
}

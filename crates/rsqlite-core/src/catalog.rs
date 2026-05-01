use std::collections::HashMap;

use rsqlite_parser::parse::parse_sql;
use rsqlite_storage::btree::{SchemaEntry, read_schema};
use rsqlite_storage::pager::Pager;
use sqlparser::ast::{self, ColumnOption, Statement};
use sqlparser::tokenizer::Token;

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
    pub is_unique: bool,
    pub autoincrement: bool,
    pub column_index: usize,
    pub default_expr: Option<String>,
    /// Set when the column is `GENERATED ALWAYS AS (...)`. Stored expression
    /// in source form; STORED columns persist their computed values into the
    /// row, VIRTUAL columns are left for read-time computation (not yet
    /// implemented — VIRTUAL columns currently behave like STORED).
    pub generated: Option<GeneratedColumn>,
}

#[derive(Debug, Clone)]
pub struct GeneratedColumn {
    pub expr: String,
    pub stored: bool,
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub root_page: u32,
    pub sql: Option<String>,
    pub check_constraints: Vec<String>,
    pub has_autoincrement: bool,
    pub foreign_keys: Vec<ForeignKeyDef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone)]
pub struct ForeignKeyDef {
    pub from_columns: Vec<String>,
    pub to_table: String,
    pub to_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}

impl TableDef {
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnDef> {
        let lower = name.to_lowercase();
        self.columns.iter().find(|c| c.name.to_lowercase() == lower)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct TriggerDef {
    pub name: String,
    pub table_name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub when_condition: Option<String>,
    pub body_sql: String,
    pub for_each_row: bool,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub table_name: String,
    pub root_page: u32,
    pub columns: Vec<String>,
    pub sql: Option<String>,
    /// WHERE clause for a partial index. Re-parsed into an expression at use
    /// time; stored as source text on the catalog.
    pub predicate: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ViewDef {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct Catalog {
    pub tables: HashMap<String, TableDef>,
    pub indexes: HashMap<String, IndexDef>,
    pub views: HashMap<String, ViewDef>,
    pub triggers: HashMap<String, TriggerDef>,
}

impl Catalog {
    pub fn load(pager: &mut Pager) -> Result<Self> {
        let schema_entries = read_schema(pager)?;
        let mut tables = HashMap::new();
        let mut indexes = HashMap::new();
        let mut views = HashMap::new();
        let mut triggers = HashMap::new();

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
                "view" => {
                    if let Some(sql) = &entry.sql {
                        views.insert(
                            entry.name.to_lowercase(),
                            ViewDef {
                                name: entry.name.clone(),
                                sql: sql.clone(),
                            },
                        );
                    }
                }
                "trigger" => {
                    if let Some(sql) = &entry.sql {
                        if let Some(tdef) = parse_trigger_def(&entry.name, &entry.tbl_name, sql) {
                            triggers.insert(tdef.name.to_lowercase(), tdef);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(Catalog {
            tables,
            indexes,
            views,
            triggers,
        })
    }

    pub fn get_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(&name.to_lowercase())
    }

    pub fn all_tables(&self) -> impl Iterator<Item = &TableDef> {
        self.tables.values()
    }

    pub fn get_view(&self, name: &str) -> Option<&ViewDef> {
        self.views.get(&name.to_lowercase())
    }

    pub fn triggers_for_table(
        &self,
        table: &str,
        timing: &TriggerTiming,
        event: &TriggerEvent,
    ) -> Vec<&TriggerDef> {
        let lower = table.to_lowercase();
        self.triggers
            .values()
            .filter(|t| {
                t.table_name.to_lowercase() == lower && t.timing == *timing && t.event == *event
            })
            .collect()
    }

    pub fn reload(&mut self, pager: &mut Pager) -> Result<()> {
        let fresh = Self::load(pager)?;
        self.tables = fresh.tables;
        self.indexes = fresh.indexes;
        self.views = fresh.views;
        self.triggers = fresh.triggers;
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
                check_constraints: vec![],
                has_autoincrement: false,
                foreign_keys: vec![],
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
            if let ast::TableConstraint::PrimaryKey {
                columns: pk_cols, ..
            } = constraint
            {
                for col in pk_cols {
                    table_pk_cols.push(col.value.to_lowercase());
                }
            }
        }

        for (i, col) in ct.columns.iter().enumerate() {
            let type_name = col.data_type.to_string();

            let affinity = TypeAffinity::from_type_name(&type_name);

            let is_pk_inline = col.options.iter().any(|opt| {
                matches!(
                    opt.option,
                    ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
                )
            });
            let is_pk_from_table = table_pk_cols.contains(&col.name.value.to_lowercase());
            let is_primary_key = is_pk_inline || is_pk_from_table;

            if is_primary_key {
                has_pk_in_columns = true;
            }

            let is_rowid_alias = is_primary_key && affinity == TypeAffinity::Integer;

            let nullable = !col
                .options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull))
                && !is_primary_key;

            let is_unique = col.options.iter().any(|opt| {
                matches!(
                    opt.option,
                    ColumnOption::Unique {
                        is_primary: false,
                        ..
                    }
                )
            }) || is_primary_key;

            let autoincrement = col.options.iter().any(|opt| {
                if let ColumnOption::DialectSpecific(tokens) = &opt.option {
                    tokens.iter().any(|t| matches!(t, Token::Word(w) if w.value.eq_ignore_ascii_case("AUTOINCREMENT")))
                } else {
                    false
                }
            });

            let default_expr = col.options.iter().find_map(|opt| {
                if let ColumnOption::Default(expr) = &opt.option {
                    Some(expr.to_string())
                } else {
                    None
                }
            });

            let generated = col.options.iter().find_map(|opt| {
                if let ColumnOption::Generated {
                    generation_expr: Some(expr),
                    generation_expr_mode,
                    ..
                } = &opt.option
                {
                    let stored = matches!(
                        generation_expr_mode,
                        Some(ast::GeneratedExpressionMode::Stored)
                    );
                    Some(GeneratedColumn {
                        expr: expr.to_string(),
                        stored,
                    })
                } else {
                    None
                }
            });

            columns.push(ColumnDef {
                name: col.name.value.clone(),
                type_name: type_name.clone(),
                affinity,
                is_primary_key,
                is_rowid_alias,
                nullable,
                is_unique,
                autoincrement,
                column_index: i,
                default_expr,
                generated,
            });
        }

        // If no explicit PK, SQLite has an implicit rowid
        let _ = has_pk_in_columns;

        let mut check_constraints = Vec::new();
        for col in &ct.columns {
            for opt in &col.options {
                if let ColumnOption::Check(expr) = &opt.option {
                    check_constraints.push(expr.to_string());
                }
            }
        }
        for constraint in &ct.constraints {
            if let ast::TableConstraint::Check { expr, .. } = constraint {
                check_constraints.push(expr.to_string());
            }
        }

        let has_autoincrement = columns.iter().any(|c| c.autoincrement);

        let mut foreign_keys = Vec::new();
        for (i, col) in ct.columns.iter().enumerate() {
            for opt in &col.options {
                if let ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ..
                } = &opt.option
                {
                    foreign_keys.push(ForeignKeyDef {
                        from_columns: vec![columns[i].name.clone()],
                        to_table: foreign_table.to_string(),
                        to_columns: referred_columns.iter().map(|c| c.value.clone()).collect(),
                        on_delete: map_referential_action(*on_delete),
                        on_update: map_referential_action(*on_update),
                    });
                }
            }
        }
        for constraint in &ct.constraints {
            if let ast::TableConstraint::ForeignKey {
                columns: fk_cols,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } = constraint
            {
                foreign_keys.push(ForeignKeyDef {
                    from_columns: fk_cols.iter().map(|c| c.value.clone()).collect(),
                    to_table: foreign_table.to_string(),
                    to_columns: referred_columns.iter().map(|c| c.value.clone()).collect(),
                    on_delete: map_referential_action(*on_delete),
                    on_update: map_referential_action(*on_update),
                });
            }
        }

        Ok(Some(TableDef {
            name: entry.name.clone(),
            columns,
            root_page: entry.rootpage,
            sql: Some(sql.clone()),
            check_constraints,
            has_autoincrement,
            foreign_keys,
        }))
    } else {
        Ok(None)
    }
}

fn map_referential_action(action: Option<ast::ReferentialAction>) -> ReferentialAction {
    match action {
        Some(ast::ReferentialAction::Cascade) => ReferentialAction::Cascade,
        Some(ast::ReferentialAction::SetNull) => ReferentialAction::SetNull,
        Some(ast::ReferentialAction::SetDefault) => ReferentialAction::SetDefault,
        Some(ast::ReferentialAction::Restrict) => ReferentialAction::Restrict,
        Some(ast::ReferentialAction::NoAction) | None => ReferentialAction::NoAction,
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
                predicate: None,
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
                predicate: None,
            }));
        }
    };

    let stmt = match stmts.into_iter().next() {
        Some(s) => s,
        None => return Ok(None),
    };

    if let Statement::CreateIndex(ci) = stmt {
        let columns: Vec<String> = ci.columns.iter().map(|c| c.expr.to_string()).collect();
        let predicate = ci.predicate.as_ref().map(|p| p.to_string());

        Ok(Some(IndexDef {
            name: entry.name.clone(),
            table_name: entry.tbl_name.clone(),
            root_page: entry.rootpage,
            columns,
            sql: Some(sql.clone()),
            predicate,
        }))
    } else {
        Ok(None)
    }
}

fn parse_trigger_def(name: &str, tbl_name: &str, sql: &str) -> Option<TriggerDef> {
    let upper = sql.to_uppercase();
    let tokens: Vec<&str> = upper.split_whitespace().collect();

    let mut pos = 2; // skip CREATE TRIGGER
    if tokens.get(pos) == Some(&"IF")
        && tokens.get(pos + 1) == Some(&"NOT")
        && tokens.get(pos + 2) == Some(&"EXISTS")
    {
        pos += 3;
    }
    pos += 1; // skip trigger name

    let timing = match tokens.get(pos).copied()? {
        "BEFORE" => {
            pos += 1;
            TriggerTiming::Before
        }
        "AFTER" => {
            pos += 1;
            TriggerTiming::After
        }
        "INSTEAD" => {
            if tokens.get(pos + 1) == Some(&"OF") {
                pos += 2;
                TriggerTiming::InsteadOf
            } else {
                return None;
            }
        }
        _ => return None,
    };

    let event = match tokens.get(pos).copied()? {
        "INSERT" => {
            pos += 1;
            TriggerEvent::Insert
        }
        "UPDATE" => {
            pos += 1;
            TriggerEvent::Update
        }
        "DELETE" => {
            pos += 1;
            TriggerEvent::Delete
        }
        _ => return None,
    };

    if tokens.get(pos) != Some(&"ON") {
        return None;
    }
    pos += 1;
    pos += 1; // skip table name

    if tokens.get(pos) == Some(&"FOR") {
        if tokens.get(pos + 1) == Some(&"EACH") && tokens.get(pos + 2) == Some(&"ROW") {
            pos += 3;
        }
    }

    let begin_idx = upper.find("BEGIN")?;
    let end_idx = upper.rfind("END")?;

    let when_condition = if tokens.get(pos) == Some(&"WHEN") {
        let when_start = upper.find("WHEN")? + 4;
        let cond = sql[when_start..begin_idx].trim().to_string();
        if cond.is_empty() { None } else { Some(cond) }
    } else {
        None
    };

    let body_sql = sql[begin_idx + 5..end_idx].trim().to_string();

    Some(TriggerDef {
        name: name.to_string(),
        table_name: tbl_name.to_string(),
        timing,
        event,
        when_condition,
        body_sql,
        for_each_row: true,
        sql: sql.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_affinity_rules() {
        assert_eq!(
            TypeAffinity::from_type_name("INTEGER"),
            TypeAffinity::Integer
        );
        assert_eq!(TypeAffinity::from_type_name("INT"), TypeAffinity::Integer);
        assert_eq!(
            TypeAffinity::from_type_name("TINYINT"),
            TypeAffinity::Integer
        );
        assert_eq!(
            TypeAffinity::from_type_name("BIGINT"),
            TypeAffinity::Integer
        );
        assert_eq!(TypeAffinity::from_type_name("TEXT"), TypeAffinity::Text);
        assert_eq!(
            TypeAffinity::from_type_name("VARCHAR(255)"),
            TypeAffinity::Text
        );
        assert_eq!(TypeAffinity::from_type_name("CLOB"), TypeAffinity::Text);
        assert_eq!(TypeAffinity::from_type_name("BLOB"), TypeAffinity::Blob);
        assert_eq!(TypeAffinity::from_type_name(""), TypeAffinity::Blob);
        assert_eq!(TypeAffinity::from_type_name("REAL"), TypeAffinity::Real);
        assert_eq!(TypeAffinity::from_type_name("DOUBLE"), TypeAffinity::Real);
        assert_eq!(TypeAffinity::from_type_name("FLOAT"), TypeAffinity::Real);
        assert_eq!(
            TypeAffinity::from_type_name("NUMERIC"),
            TypeAffinity::Numeric
        );
        assert_eq!(
            TypeAffinity::from_type_name("BOOLEAN"),
            TypeAffinity::Numeric
        );
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

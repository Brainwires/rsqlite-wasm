//! Virtual table foundation.
//!
//! A *virtual table* is a table whose rows aren't stored in the engine's
//! btrees — instead they're produced on demand by a [`VirtualTable`]
//! implementation supplied by a *module*. SQLite uses the same pattern for
//! FTS5, R-Tree, and many extensions.
//!
//! For v0.1 the surface is deliberately minimal:
//!
//! - **No `xBestIndex`.** Every scan is a full enumeration via
//!   [`VirtualTable::scan`]; the executor wraps the result in a Filter
//!   for any `WHERE` clause.
//! - **Read-only.** No `xUpdate` — virtual tables can't be the target of
//!   INSERT / UPDATE / DELETE yet.
//! - **In-memory column metadata.** A module declares its column names
//!   up front via [`Module::columns`]; types are inferred at scan time.
//!
//! That's enough surface to host built-in modules (the bundled `series`
//! generator) and to give external add-ons (FTS5, R-Tree, HNSW vector
//! index) somewhere to land.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rsqlite_storage::codec::Value;

pub(crate) mod fts5;
mod rtree;
mod vec_index;

use crate::error::Result;
use crate::types::Row;

/// Definition of a virtual-table module: how it parses its CREATE
/// VIRTUAL TABLE arguments and what columns its tables expose.
pub trait Module: 'static {
    /// Module name, matched case-insensitively against `USING <name>(...)`.
    fn name(&self) -> &str;

    /// Build a virtual-table instance for one `CREATE VIRTUAL TABLE`
    /// statement. `args` are the comma-separated tokens between the
    /// parentheses, with whitespace trimmed. The returned instance is
    /// kept alive by the catalog and shared across queries — modules
    /// that hold mutable state (anything supporting INSERT) must use
    /// interior mutability (`RefCell`, `Mutex`).
    fn create(&self, table_name: &str, args: &[String]) -> Result<Rc<dyn VirtualTable>>;
}

/// A live virtual table. The catalog stores one shared `Rc<dyn
/// VirtualTable>` per CREATE VIRTUAL TABLE, so all `&self` methods may
/// be called concurrently across queries. Implementations needing
/// mutable state should wrap that state in interior-mutability cells.
pub trait VirtualTable {
    /// Names of the columns this table exposes, in declaration order.
    /// Matches the column list `xColumn` would project for SQLite.
    fn columns(&self) -> Vec<String>;

    /// Produce all rows. The executor may wrap the result in a Filter
    /// if there's a WHERE clause; a future `xBestIndex` hook would let
    /// the module push filters down itself.
    fn scan(&self) -> Result<Vec<Row>>;

    /// Optional INSERT hook — `xUpdate` in SQLite vtab terms. Default
    /// is read-only; modules that override this can be the target of
    /// `INSERT INTO vt VALUES (...)`. Returns the rowid of the new
    /// row. The values slice is in column order.
    fn insert(&self, _values: &[Value]) -> Result<i64> {
        Err(crate::error::Error::Other(
            "this virtual table is read-only".into(),
        ))
    }
}

thread_local! {
    static MODULE_REGISTRY: RefCell<HashMap<String, Rc<dyn Module>>> =
        RefCell::new(default_modules());
}

fn default_modules() -> HashMap<String, Rc<dyn Module>> {
    let mut map: HashMap<String, Rc<dyn Module>> = HashMap::new();
    let series = Rc::new(SeriesModule) as Rc<dyn Module>;
    map.insert("series".to_string(), series.clone());
    map.insert("generate_series".to_string(), series);
    map.insert("kvstore".to_string(), Rc::new(KvStoreModule));
    map.insert("vec_index".to_string(), Rc::new(vec_index::VecIndexModule));
    map.insert("rtree".to_string(), Rc::new(rtree::RtreeModule));
    map.insert("fts5".to_string(), Rc::new(fts5::Fts5Module));
    map
}

/// Register a virtual-table module by name. Names are stored
/// lowercased and matched case-insensitively. Re-registering replaces
/// the previous definition.
pub fn register_module(module: Rc<dyn Module>) {
    let key = module.name().to_ascii_lowercase();
    MODULE_REGISTRY.with(|r| {
        r.borrow_mut().insert(key, module);
    });
}

/// Look up a module by name (case-insensitive). Returns `None` if no
/// module with that name is registered.
pub fn lookup_module(name: &str) -> Option<Rc<dyn Module>> {
    let key = name.to_ascii_lowercase();
    MODULE_REGISTRY.with(|r| r.borrow().get(&key).cloned())
}

/// Drop every registered module and reinstate the built-in defaults.
/// Useful when tests register stub modules and want a clean slate.
pub fn reset_to_defaults() {
    MODULE_REGISTRY.with(|r| *r.borrow_mut() = default_modules());
}

// ── Built-in: `series` ────────────────────────────────────────────────

/// Generate-series style enumerator. Mirrors SQLite's `generate_series`
/// extension: `SELECT * FROM series(start, stop[, step])` yields one
/// row per integer in the half-open range.
struct SeriesModule;

impl Module for SeriesModule {
    fn name(&self) -> &str {
        "series"
    }

    fn create(&self, _table_name: &str, args: &[String]) -> Result<Rc<dyn VirtualTable>> {
        let parse_int = |s: &str| -> Result<i64> {
            s.parse::<i64>().map_err(|_| {
                crate::error::Error::Other(format!(
                    "series module: argument {s:?} is not an integer"
                ))
            })
        };
        let (start, stop, step) = match args {
            [a, b] => (parse_int(a)?, parse_int(b)?, 1i64),
            [a, b, c] => (parse_int(a)?, parse_int(b)?, parse_int(c)?),
            _ => {
                return Err(crate::error::Error::Other(
                    "series module: expected 2 or 3 integer arguments".into(),
                ));
            }
        };
        Ok(Rc::new(SeriesTable { start, stop, step }))
    }
}

struct SeriesTable {
    start: i64,
    stop: i64,
    step: i64,
}

impl VirtualTable for SeriesTable {
    fn columns(&self) -> Vec<String> {
        // Every series instance exposes one column, `value`. Matches
        // SQLite's generate_series schema (without the optional
        // `start`/`stop`/`step` reflection columns).
        vec!["value".to_string()]
    }

    fn scan(&self) -> Result<Vec<Row>> {
        if self.step == 0 {
            return Err(crate::error::Error::Other(
                "series module: step must be non-zero".into(),
            ));
        }
        let mut rows = Vec::new();
        let mut v = self.start;
        if self.step > 0 {
            while v <= self.stop {
                rows.push(Row::with_rowid(vec![Value::Integer(v)], v));
                v += self.step;
            }
        } else {
            while v >= self.stop {
                rows.push(Row::with_rowid(vec![Value::Integer(v)], v));
                v += self.step;
            }
        }
        Ok(rows)
    }
}

// ── Built-in: `kvstore` ───────────────────────────────────────────────

/// Simple in-memory key/value store. Demonstrates the writeable
/// virtual-table path: `CREATE VIRTUAL TABLE kv USING kvstore;`
/// declares a two-column table `(key TEXT, value)`. Rows are
/// appended in insertion order; rowid = position + 1.
struct KvStoreModule;

impl Module for KvStoreModule {
    fn name(&self) -> &str {
        "kvstore"
    }

    fn create(&self, _table_name: &str, _args: &[String]) -> Result<Rc<dyn VirtualTable>> {
        Ok(Rc::new(KvStoreTable {
            rows: RefCell::new(Vec::new()),
        }))
    }
}

struct KvStoreTable {
    rows: RefCell<Vec<(Value, Value)>>,
}

impl VirtualTable for KvStoreTable {
    fn columns(&self) -> Vec<String> {
        vec!["key".to_string(), "value".to_string()]
    }

    fn scan(&self) -> Result<Vec<Row>> {
        Ok(self
            .rows
            .borrow()
            .iter()
            .enumerate()
            .map(|(i, (k, v))| Row::with_rowid(vec![k.clone(), v.clone()], (i as i64) + 1))
            .collect())
    }

    fn insert(&self, values: &[Value]) -> Result<i64> {
        if values.len() != 2 {
            return Err(crate::error::Error::Other(
                "kvstore.insert: expected 2 values (key, value)".into(),
            ));
        }
        let mut rows = self.rows.borrow_mut();
        rows.push((values[0].clone(), values[1].clone()));
        Ok(rows.len() as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_two_args_default_step() {
        reset_to_defaults();
        let m = lookup_module("series").unwrap();
        let t = m.create("s", &["1".into(), "5".into()]).unwrap();
        let rows = t.scan().unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].values[0], Value::Integer(1));
        assert_eq!(rows[4].values[0], Value::Integer(5));
    }

    #[test]
    fn series_three_args_negative_step() {
        let m = lookup_module("generate_series").unwrap();
        let t = m
            .create("s", &["10".into(), "0".into(), "-2".into()])
            .unwrap();
        let rows = t.scan().unwrap();
        let vs: Vec<i64> = rows
            .iter()
            .filter_map(|r| {
                if let Value::Integer(n) = r.values[0] {
                    Some(n)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(vs, vec![10, 8, 6, 4, 2, 0]);
    }

    #[test]
    fn series_zero_step_errors() {
        let m = lookup_module("series").unwrap();
        let t = m
            .create("s", &["1".into(), "5".into(), "0".into()])
            .unwrap();
        assert!(t.scan().is_err());
    }

    #[test]
    fn unknown_module_returns_none() {
        reset_to_defaults();
        assert!(lookup_module("not_a_real_module").is_none());
    }

    #[test]
    fn registered_custom_module_is_found() {
        struct Stub;
        impl Module for Stub {
            fn name(&self) -> &str {
                "stub_module"
            }
            fn create(&self, _t: &str, _a: &[String]) -> Result<Rc<dyn VirtualTable>> {
                Err(crate::error::Error::Other("not implemented".into()))
            }
        }
        register_module(Rc::new(Stub));
        assert!(lookup_module("stub_module").is_some());
        assert!(lookup_module("STUB_MODULE").is_some());
        reset_to_defaults();
        assert!(lookup_module("stub_module").is_none());
    }

    #[test]
    fn writeable_module_supports_insert() {
        use std::cell::RefCell;
        struct MemTable {
            cols: Vec<String>,
            rows: RefCell<Vec<Row>>,
        }
        impl VirtualTable for MemTable {
            fn columns(&self) -> Vec<String> {
                self.cols.clone()
            }
            fn scan(&self) -> Result<Vec<Row>> {
                Ok(self.rows.borrow().clone())
            }
            fn insert(&self, values: &[Value]) -> Result<i64> {
                let mut r = self.rows.borrow_mut();
                let id = r.len() as i64 + 1;
                r.push(Row::with_rowid(values.to_vec(), id));
                Ok(id)
            }
        }
        struct MemModule;
        impl Module for MemModule {
            fn name(&self) -> &str {
                "memtab"
            }
            fn create(&self, _t: &str, _a: &[String]) -> Result<Rc<dyn VirtualTable>> {
                Ok(Rc::new(MemTable {
                    cols: vec!["k".into(), "v".into()],
                    rows: RefCell::new(Vec::new()),
                }))
            }
        }
        register_module(Rc::new(MemModule));
        let table = lookup_module("memtab").unwrap().create("t", &[]).unwrap();
        let id = table
            .insert(&[Value::Integer(7), Value::Text("hello".into())])
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(table.scan().unwrap().len(), 1);
        reset_to_defaults();
    }
}

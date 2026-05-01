//! # rsqlite
//!
//! A pure-Rust, SQLite-compatible database engine. The [`core`] module holds
//! the catalog, planner, and executor; [`storage`] holds the pager and B-tree;
//! [`parser`] wraps `sqlparser-rs`; and [`vfs`] holds the VFS trait plus the
//! native-file and in-memory backends.
//!
//! Databases written by this crate are file-format compatible with
//! SQLite 3 — they round-trip through the `sqlite3` CLI.
//!
//! For the deferred-feature inventory, see `LIMITATIONS.md` in the repo root.
//!
//! # Quick start
//!
//! ```no_run
//! use rsqlite::vfs::memory::MemoryVfs;
//! use rsqlite::core::database::Database;
//!
//! let vfs = MemoryVfs::new();
//! let mut db = Database::create(&vfs, "test.db").unwrap();
//!
//! db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
//! db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
//!
//! let result = db.query("SELECT * FROM t").unwrap();
//! assert_eq!(result.rows.len(), 1);
//! ```

pub use rsqlite_core as core;
pub use rsqlite_parser as parser;
pub use rsqlite_storage as storage;
pub use rsqlite_vfs as vfs;

// Test module for the Database type. Tests are split by theme so no single
// file grows unbounded. Each submodule does `use super::*` to inherit the
// parent module's symbol re-exports.

pub(super) use super::*;

mod basic;
mod constraints_integrity;
mod modern;
mod scalar_extras;
mod schema;
mod views_ctes_advanced;

/// Shared helper: write a database file at `path` and seed it with `sql`
/// using the local sqlite3 binary. Returns false (and logs) when sqlite3 is
/// not installed so tests can early-return as a no-op rather than fail.
pub(super) fn setup_test_db(path: &str, sql: &str) -> bool {
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

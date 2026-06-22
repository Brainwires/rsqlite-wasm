//! On-disk persistence for FTS5 virtual tables.
//!
//! Each FTS5 vtab is backed by:
//!
//! - A row in `sqlite_schema` with `type='table'` and `sql='CREATE
//!   VIRTUAL TABLE …'` so the catalog reload sees it on next open.
//! - A shadow table `__fts5_<name>_data(blob BLOB)` with a single
//!   row (rowid=1) holding the latest serialized snapshot
//!   ([`super::Fts5Table::snapshot`]).
//!
//! Helpers here read / write those structures via the same `Pager`
//! the rest of the engine uses.

use rsqlite_storage::btree::{
    BTreeCursor, btree_create_table, btree_delete, btree_insert, insert_schema_entry,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};

use super::Fts5Table;

/// Build the conventional shadow-table name for a given FTS5 vtab.
pub fn shadow_table_name(vtab_name: &str) -> String {
    format!("__fts5_{}_data", vtab_name.to_lowercase())
}

/// Ensure both the schema-row marker and the shadow data table exist
/// for a freshly-created FTS5 vtab. Returns the shadow root page.
pub fn ensure_persistence(
    vtab_name: &str,
    create_sql: &str,
    pager: &mut Pager,
    catalog: &mut Catalog,
) -> Result<u32> {
    // 1. Persist the `CREATE VIRTUAL TABLE` declaration in
    //    sqlite_schema so the catalog re-instantiates the vtab on
    //    next open.
    insert_schema_entry(pager, "table", vtab_name, vtab_name, 0, create_sql)
        .map_err(|e| Error::Other(e.to_string()))?;

    // 2. Create the shadow data table if it doesn't already exist.
    let shadow = shadow_table_name(vtab_name);
    let root = if catalog.get_table(&shadow).is_some() {
        catalog.get_table(&shadow).unwrap().root_page
    } else {
        let r = btree_create_table(pager).map_err(|e| Error::Other(e.to_string()))?;
        let shadow_sql = format!("CREATE TABLE {shadow}(blob BLOB)");
        insert_schema_entry(pager, "table", &shadow, &shadow, r, &shadow_sql)
            .map_err(|e| Error::Other(e.to_string()))?;
        r
    };

    if !pager.in_transaction() {
        pager.flush().map_err(|e| Error::Other(e.to_string()))?;
    }

    catalog.reload(pager)?;
    Ok(root)
}

/// Write the FTS5 snapshot to the shadow table. Replaces any
/// existing row at rowid 1.
pub fn write_snapshot(
    table: &Fts5Table,
    vtab_name: &str,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    use crate::vtab::VirtualTable;
    let Some(blob) = table.snapshot() else {
        return Ok(());
    };
    let shadow_name = shadow_table_name(vtab_name);
    let shadow = catalog
        .get_table(&shadow_name)
        .ok_or_else(|| Error::Other(format!("fts5: shadow table missing: {shadow_name}")))?;

    let _ = btree_delete(pager, shadow.root_page, 1);
    let record = Record {
        values: vec![Value::Blob(blob)],
    };
    btree_insert(pager, shadow.root_page, 1, &record).map_err(|e| Error::Other(e.to_string()))?;
    if !pager.in_transaction() {
        pager.flush().map_err(|e| Error::Other(e.to_string()))?;
    }
    Ok(())
}

/// Read the latest snapshot, if any, and restore it into the live
/// FTS5 instance. No-op when the shadow table doesn't exist or is
/// empty.
pub fn restore_if_present(
    table: &Fts5Table,
    vtab_name: &str,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<()> {
    use crate::vtab::VirtualTable;
    let shadow_name = shadow_table_name(vtab_name);
    let Some(shadow) = catalog.get_table(&shadow_name) else {
        return Ok(());
    };
    let mut cursor = BTreeCursor::new(pager, shadow.root_page);
    let rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(());
    };
    let Value::Blob(blob) = row.record.values.first().cloned().unwrap_or(Value::Null) else {
        return Ok(());
    };
    table.restore(&blob)?;
    Ok(())
}

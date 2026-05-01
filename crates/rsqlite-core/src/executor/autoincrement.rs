use rsqlite_storage::btree::{
    BTreeCursor, btree_create_table, btree_delete, btree_insert, btree_max_rowid,
    insert_schema_entry,
};
use rsqlite_storage::codec::{Record, Value};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};

pub(super) fn ensure_sqlite_sequence(pager: &mut Pager, catalog: &mut Catalog) -> Result<()> {
    if catalog.get_table("sqlite_sequence").is_some() {
        return Ok(());
    }
    let root_page = btree_create_table(pager)?;
    insert_schema_entry(
        pager,
        "table",
        "sqlite_sequence",
        "sqlite_sequence",
        root_page,
        "CREATE TABLE sqlite_sequence(name,seq)",
    )?;
    catalog.reload(pager)?;
    Ok(())
}

pub(super) fn read_autoincrement_seq(
    pager: &mut Pager,
    catalog: &Catalog,
    table_name: &str,
) -> Result<i64> {
    let seq_table = match catalog.get_table("sqlite_sequence") {
        Some(t) => t,
        None => return Ok(0),
    };
    let mut cursor = BTreeCursor::new(pager, seq_table.root_page);
    let rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;
    for row in &rows {
        if let Some(Value::Text(name)) = row.record.values.first() {
            if name.eq_ignore_ascii_case(table_name) {
                if let Some(Value::Integer(seq)) = row.record.values.get(1) {
                    return Ok(*seq);
                }
            }
        }
    }
    Ok(0)
}

pub(super) fn update_autoincrement_seq(
    pager: &mut Pager,
    catalog: &Catalog,
    table_name: &str,
    new_seq: i64,
) -> Result<()> {
    let seq_table = match catalog.get_table("sqlite_sequence") {
        Some(t) => t,
        None => return Ok(()),
    };
    let mut cursor = BTreeCursor::new(pager, seq_table.root_page);
    let rows = cursor
        .collect_all()
        .map_err(|e| Error::Other(e.to_string()))?;
    let mut existing_rowid = None;
    for row in &rows {
        if let Some(Value::Text(name)) = row.record.values.first() {
            if name.eq_ignore_ascii_case(table_name) {
                existing_rowid = Some(row.rowid);
                break;
            }
        }
    }

    let record = Record {
        values: vec![Value::Text(table_name.to_string()), Value::Integer(new_seq)],
    };
    let root = seq_table.root_page;
    match existing_rowid {
        Some(rowid) => {
            btree_delete(pager, root, rowid).map_err(|e| Error::Other(e.to_string()))?;
            btree_insert(pager, root, rowid, &record)?;
        }
        None => {
            let rowid = btree_max_rowid(pager, root)? + 1;
            btree_insert(pager, root, rowid, &record)?;
        }
    }
    Ok(())
}

pub(super) fn compute_autoincrement_rowid(
    pager: &mut Pager,
    catalog: &Catalog,
    table_name: &str,
    current_root: u32,
) -> Result<i64> {
    let seq = read_autoincrement_seq(pager, catalog, table_name)?;
    let max_rowid = btree_max_rowid(pager, current_root)?;
    Ok(std::cmp::max(seq, max_rowid) + 1)
}

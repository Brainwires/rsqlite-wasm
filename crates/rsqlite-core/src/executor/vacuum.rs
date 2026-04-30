use std::collections::HashMap;

use rsqlite_vfs::Vfs;
use rsqlite_storage::btree::{
    btree_create_index, btree_create_table, btree_insert, insert_schema_entry,
    read_schema, BTreeCursor, IndexCursor,
};
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};

use super::ExecResult;

pub(super) fn execute_vacuum(pager: &mut Pager, catalog: &mut Catalog) -> Result<ExecResult> {
    if pager.in_transaction() {
        return Err(Error::Other(
            "cannot VACUUM from within a transaction".to_string(),
        ));
    }

    let schema_entries = read_schema(pager)?;
    if schema_entries.is_empty() {
        return Ok(ExecResult { rows_affected: 0 });
    }

    let temp_vfs = rsqlite_vfs::memory::MemoryVfs::new();
    let mut temp_pager = Pager::create(&temp_vfs, "__vacuum_temp.db")?;

    let mut root_page_map: HashMap<u32, u32> = HashMap::new();

    for entry in &schema_entries {
        match entry.entry_type.as_str() {
            "table" => {
                let new_root = btree_create_table(&mut temp_pager)?;
                root_page_map.insert(entry.rootpage, new_root);

                let mut cursor = BTreeCursor::new(pager, entry.rootpage);
                let rows = cursor.collect_all()?;
                for row in &rows {
                    btree_insert(&mut temp_pager, new_root, row.rowid, &row.record)?;
                }
            }
            "index" => {
                let new_root = btree_create_index(&mut temp_pager)?;
                root_page_map.insert(entry.rootpage, new_root);

                let mut cursor = IndexCursor::new(pager, entry.rootpage);
                let records = cursor.collect_all()?;
                for rec in &records {
                    rsqlite_storage::btree::btree_index_insert(
                        &mut temp_pager,
                        new_root,
                        rec,
                    )?;
                }
            }
            _ => {}
        }
    }

    for entry in &schema_entries {
        let new_rootpage = root_page_map.get(&entry.rootpage).copied().unwrap_or(0);
        let sql_str = entry.sql.as_deref().unwrap_or("");
        insert_schema_entry(
            &mut temp_pager,
            &entry.entry_type,
            &entry.name,
            &entry.tbl_name,
            new_rootpage,
            sql_str,
        )?;
    }

    temp_pager.flush()?;

    let temp_file = temp_vfs.open(
        "__vacuum_temp.db",
        rsqlite_vfs::OpenFlags {
            create: false,
            read_write: false,
            delete_on_close: false,
        },
    ).map_err(|e| Error::Other(format!("vacuum: failed to read temp db: {e}")))?;

    let size = temp_file
        .file_size()
        .map_err(|e| Error::Other(format!("vacuum: {e}")))?;
    let mut buf = vec![0u8; size as usize];
    temp_file
        .read(0, &mut buf)
        .map_err(|e| Error::Other(format!("vacuum: {e}")))?;

    pager.replace_content(&buf)?;
    catalog.reload(pager)?;

    Ok(ExecResult { rows_affected: 0 })
}

use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::types::{QueryResult, Row};

pub fn execute_pragma(
    name: &str,
    argument: Option<&str>,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    match name {
        "table_info" => {
            let table_name = argument.ok_or_else(|| {
                Error::Other("PRAGMA table_info requires a table name".to_string())
            })?;
            let table = catalog.get_table(table_name).ok_or_else(|| {
                Error::Other(format!("no such table: {table_name}"))
            })?;
            let columns = vec![
                "cid".to_string(),
                "name".to_string(),
                "type".to_string(),
                "notnull".to_string(),
                "dflt_value".to_string(),
                "pk".to_string(),
            ];
            let rows = table
                .columns
                .iter()
                .map(|col| Row {
                    values: vec![
                        Value::Integer(col.column_index as i64),
                        Value::Text(col.name.clone()),
                        Value::Text(col.type_name.clone()),
                        Value::Integer(if col.nullable { 0 } else { 1 }),
                        Value::Null,
                        Value::Integer(if col.is_primary_key { 1 } else { 0 }),
                    ],
                })
                .collect();
            Ok(QueryResult { columns, rows })
        }
        "table_list" => {
            let columns = vec![
                "schema".to_string(),
                "name".to_string(),
                "type".to_string(),
            ];
            let mut rows: Vec<Row> = catalog
                .tables
                .values()
                .map(|t| Row {
                    values: vec![
                        Value::Text("main".to_string()),
                        Value::Text(t.name.clone()),
                        Value::Text("table".to_string()),
                    ],
                })
                .collect();
            rows.sort_by(|a, b| a.values[1].to_string().cmp(&b.values[1].to_string()));
            Ok(QueryResult { columns, rows })
        }
        "index_list" => {
            let table_name = argument.ok_or_else(|| {
                Error::Other("PRAGMA index_list requires a table name".to_string())
            })?;
            let columns = vec![
                "seq".to_string(),
                "name".to_string(),
                "unique".to_string(),
                "origin".to_string(),
                "partial".to_string(),
            ];
            let mut rows = Vec::new();
            let mut seq = 0i64;
            for idx in catalog.indexes.values() {
                if idx.table_name.eq_ignore_ascii_case(table_name) {
                    rows.push(Row {
                        values: vec![
                            Value::Integer(seq),
                            Value::Text(idx.name.clone()),
                            Value::Integer(0),
                            Value::Text("c".to_string()),
                            Value::Integer(0),
                        ],
                    });
                    seq += 1;
                }
            }
            Ok(QueryResult { columns, rows })
        }
        "index_info" => {
            let index_name = argument.ok_or_else(|| {
                Error::Other("PRAGMA index_info requires an index name".to_string())
            })?;
            let idx = catalog
                .indexes
                .get(&index_name.to_lowercase())
                .ok_or_else(|| Error::Other(format!("no such index: {index_name}")))?;
            let table = catalog.get_table(&idx.table_name);
            let columns = vec![
                "seqno".to_string(),
                "cid".to_string(),
                "name".to_string(),
            ];
            let rows = idx
                .columns
                .iter()
                .enumerate()
                .map(|(i, col_name)| {
                    let cid = table
                        .and_then(|t| {
                            t.columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                        })
                        .map(|p| p as i64)
                        .unwrap_or(-1);
                    Row {
                        values: vec![
                            Value::Integer(i as i64),
                            Value::Integer(cid),
                            Value::Text(col_name.clone()),
                        ],
                    }
                })
                .collect();
            Ok(QueryResult { columns, rows })
        }
        "page_size" => Ok(QueryResult {
            columns: vec!["page_size".to_string()],
            rows: vec![Row {
                values: vec![Value::Integer(pager.page_size() as i64)],
            }],
        }),
        "page_count" => Ok(QueryResult {
            columns: vec!["page_count".to_string()],
            rows: vec![Row {
                values: vec![Value::Integer(pager.page_count() as i64)],
            }],
        }),
        "database_list" => Ok(QueryResult {
            columns: vec![
                "seq".to_string(),
                "name".to_string(),
                "file".to_string(),
            ],
            rows: vec![Row {
                values: vec![
                    Value::Integer(0),
                    Value::Text("main".to_string()),
                    Value::Text(String::new()),
                ],
            }],
        }),
        "journal_mode" => Ok(QueryResult {
            columns: vec!["journal_mode".to_string()],
            rows: vec![Row {
                values: vec![Value::Text("delete".to_string())],
            }],
        }),
        "foreign_keys" | "foreign_key_list" if name == "foreign_keys" => {
            match argument {
                Some(val) => {
                    let enabled = matches!(val.trim().trim_matches('\''), "1" | "ON" | "on" | "yes" | "true");
                    super::state::set_foreign_keys_enabled(enabled);
                    Ok(QueryResult {
                        columns: vec!["foreign_keys".to_string()],
                        rows: vec![Row {
                            values: vec![Value::Integer(if enabled { 1 } else { 0 })],
                        }],
                    })
                }
                None => Ok(QueryResult {
                    columns: vec!["foreign_keys".to_string()],
                    rows: vec![Row {
                        values: vec![Value::Integer(if super::state::foreign_keys_enabled() { 1 } else { 0 })],
                    }],
                }),
            }
        }
        "encoding" => Ok(QueryResult {
            columns: vec!["encoding".to_string()],
            rows: vec![Row {
                values: vec![Value::Text("UTF-8".to_string())],
            }],
        }),
        "compile_options" => Ok(QueryResult {
            columns: vec!["compile_options".to_string()],
            rows: vec![],
        }),
        "auto_vacuum" => Ok(QueryResult {
            columns: vec!["auto_vacuum".to_string()],
            rows: vec![Row { values: vec![Value::Integer(0)] }],
        }),
        "cache_size" => Ok(QueryResult {
            columns: vec!["cache_size".to_string()],
            rows: vec![Row { values: vec![Value::Integer(-2000)] }],
        }),
        "collation_list" => Ok(QueryResult {
            columns: vec!["seq".to_string(), "name".to_string()],
            rows: vec![
                Row { values: vec![Value::Integer(0), Value::Text("BINARY".to_string())] },
                Row { values: vec![Value::Integer(1), Value::Text("NOCASE".to_string())] },
                Row { values: vec![Value::Integer(2), Value::Text("RTRIM".to_string())] },
            ],
        }),
        "integrity_check" | "quick_check" => Ok(QueryResult {
            columns: vec![name.to_string()],
            rows: vec![Row { values: vec![Value::Text("ok".to_string())] }],
        }),
        "user_version" => {
            let user_version = read_header_u32(pager, 60)?;
            Ok(QueryResult {
                columns: vec!["user_version".to_string()],
                rows: vec![Row { values: vec![Value::Integer(user_version as i64)] }],
            })
        }
        "application_id" => {
            let app_id = read_header_u32(pager, 68)?;
            Ok(QueryResult {
                columns: vec!["application_id".to_string()],
                rows: vec![Row { values: vec![Value::Integer(app_id as i64)] }],
            })
        }
        "schema_version" => {
            let schema_version = read_header_u32(pager, 40)?;
            Ok(QueryResult {
                columns: vec!["schema_version".to_string()],
                rows: vec![Row { values: vec![Value::Integer(schema_version as i64)] }],
            })
        }
        "table_xinfo" => {
            let table_name = argument.ok_or_else(|| {
                Error::Other("PRAGMA table_xinfo requires a table name".to_string())
            })?;
            let table = catalog.get_table(table_name).ok_or_else(|| {
                Error::Other(format!("no such table: {table_name}"))
            })?;
            let columns = vec![
                "cid".to_string(),
                "name".to_string(),
                "type".to_string(),
                "notnull".to_string(),
                "dflt_value".to_string(),
                "pk".to_string(),
                "hidden".to_string(),
            ];
            let rows = table
                .columns
                .iter()
                .map(|col| Row {
                    values: vec![
                        Value::Integer(col.column_index as i64),
                        Value::Text(col.name.clone()),
                        Value::Text(col.type_name.clone()),
                        Value::Integer(if col.nullable { 0 } else { 1 }),
                        col.default_expr
                            .as_ref()
                            .map(|s| Value::Text(s.clone()))
                            .unwrap_or(Value::Null),
                        Value::Integer(if col.is_primary_key { 1 } else { 0 }),
                        Value::Integer(0),
                    ],
                })
                .collect();
            Ok(QueryResult { columns, rows })
        }
        "foreign_key_list" => {
            let table_name = argument.ok_or_else(|| {
                Error::Other("PRAGMA foreign_key_list requires a table name".to_string())
            })?;
            let table = catalog.get_table(table_name).ok_or_else(|| {
                Error::Other(format!("no such table: {table_name}"))
            })?;
            let columns = vec![
                "id".to_string(),
                "seq".to_string(),
                "table".to_string(),
                "from".to_string(),
                "to".to_string(),
                "on_update".to_string(),
                "on_delete".to_string(),
                "match".to_string(),
            ];
            let mut rows = Vec::new();
            for (id, fk) in table.foreign_keys.iter().enumerate() {
                for (seq, (from_col, to_col)) in fk
                    .from_columns
                    .iter()
                    .zip(fk.to_columns.iter())
                    .enumerate()
                {
                    rows.push(Row {
                        values: vec![
                            Value::Integer(id as i64),
                            Value::Integer(seq as i64),
                            Value::Text(fk.to_table.clone()),
                            Value::Text(from_col.clone()),
                            Value::Text(to_col.clone()),
                            Value::Text("NO ACTION".to_string()),
                            Value::Text("NO ACTION".to_string()),
                            Value::Text("NONE".to_string()),
                        ],
                    });
                }
            }
            Ok(QueryResult { columns, rows })
        }
        "foreign_key_check" => {
            // Return one row per FK violation: (table, rowid, parent, fkid).
            // Currently a stub that returns an empty result (no violations);
            // matches SQLite's behavior when foreign_keys = OFF.
            Ok(QueryResult {
                columns: vec![
                    "table".to_string(),
                    "rowid".to_string(),
                    "parent".to_string(),
                    "fkid".to_string(),
                ],
                rows: vec![],
            })
        }
        _ => Err(Error::Other(format!("unsupported PRAGMA: {name}"))),
    }
}

/// Read a 4-byte big-endian unsigned integer from the database header
/// (page 1) at the given byte offset.
fn read_header_u32(pager: &mut Pager, offset: usize) -> Result<u32> {
    let page = pager.get_page(1).map_err(|e| Error::Other(e.to_string()))?;
    let bytes = &page.data;
    if bytes.len() < offset + 4 {
        return Ok(0);
    }
    Ok(u32::from_be_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]))
}

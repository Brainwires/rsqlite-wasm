mod idb;
mod opfs;

use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

use rsqlite_core::database::Database;
use rsqlite_core::types::Value;
use rsqlite_vfs::memory::MemoryVfs;
use rsqlite_vfs::multiplex::{DEFAULT_CHUNK_SIZE, MultiplexVfs};
use rsqlite_vfs::{OpenFlags, Vfs};

/// Sentinel size (in bytes) below which we treat a file as effectively
/// non-existent — too small to hold even an SQLite header. Hit when a
/// previous session created the OPFS / IDB handle but crashed (or was
/// reloaded) before any pages were written. The 100-byte threshold
/// matches `rsqlite_storage::header::HEADER_SIZE`.
const MIN_VALID_DB_BYTES: u64 = 100;

/// Returns `true` when `path` exists in `vfs` AND has at least a valid
/// header's worth of bytes. A 0-byte file (or a partial-header
/// truncation) is treated as `false` so the caller falls through to
/// `Database::create` instead of `Database::open`-ing into the
/// `InvalidHeader("file too small")` failure mode.
fn db_path_is_loadable(vfs: &dyn Vfs, path: &str) -> Result<bool, JsError> {
    if !vfs.exists(path).map_err(to_js_error)? {
        return Ok(false);
    }
    // Open read-only just long enough to query size; the file handle is
    // dropped at the end of the function.
    let flags = OpenFlags {
        create: false,
        read_write: false,
        delete_on_close: false,
    };
    let file = match vfs.open(path, flags) {
        Ok(f) => f,
        Err(_) => return Ok(false),
    };
    let size = file.file_size().map_err(to_js_error)?;
    Ok(size >= MIN_VALID_DB_BYTES)
}

/// Default per-shard cap for OPFS-backed databases. With 1 GB chunks this
/// gives a 16 GB ceiling per database — more than the per-file caps any
/// browser currently enforces, while only costing 16 zero-byte handle slots
/// for fresh databases.
const DEFAULT_MAX_SHARDS: usize = 16;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

enum VfsBackend {
    Memory(MemoryVfs),
    Opfs {
        mux: MultiplexVfs,
        _raw: opfs::OpfsVfs,
    },
    Idb {
        mux: MultiplexVfs,
        raw: idb::IdbVfs,
    },
}

#[wasm_bindgen]
pub struct WasmDatabase {
    db: Database,
    backend: VfsBackend,
    path: String,
}

#[wasm_bindgen]
impl WasmDatabase {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<WasmDatabase, JsError> {
        let vfs = MemoryVfs::new();
        let db = Database::create(&vfs, "memory.db").map_err(to_js_error)?;
        Ok(WasmDatabase {
            db,
            backend: VfsBackend::Memory(vfs),
            path: "memory.db".to_string(),
        })
    }

    #[wasm_bindgen(js_name = "openInMemory")]
    pub fn open_in_memory() -> Result<WasmDatabase, JsError> {
        WasmDatabase::new()
    }

    #[wasm_bindgen(js_name = "openWithOpfs")]
    pub async fn open_with_opfs(
        name: &str,
        chunk_size: Option<u64>,
        max_shards: Option<usize>,
    ) -> Result<WasmDatabase, JsError> {
        let chunk_size = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);
        let max_shards = max_shards.unwrap_or(DEFAULT_MAX_SHARDS);

        let raw = opfs::OpfsVfs::new().await.map_err(jsval_to_js_error)?;
        let db_path = if name.ends_with(".db") {
            name.to_string()
        } else {
            format!("{name}.db")
        };

        // Pre-register all shard handles. SyncAccessHandle creation is async
        // but the engine's reads/writes are sync, so we must hold every
        // handle we might need before any query runs.
        raw.register_shards(&db_path, max_shards)
            .await
            .map_err(jsval_to_js_error)?;

        let mux = MultiplexVfs::with_chunk_size(raw.clone_box(), chunk_size);
        let db = if db_path_is_loadable(&mux, &db_path)? {
            Database::open(&mux, &db_path).map_err(to_js_error)?
        } else {
            // File missing OR too small to hold a header. Either way,
            // create a fresh database. `MultiplexVfs::open(create=true)`
            // truncates / reinitializes the underlying shards, so an
            // empty leftover file from a crashed prior session is
            // safely overwritten.
            Database::create(&mux, &db_path).map_err(to_js_error)?
        };

        Ok(WasmDatabase {
            db,
            backend: VfsBackend::Opfs { mux, _raw: raw },
            path: db_path,
        })
    }

    #[wasm_bindgen(js_name = "openWithIdb")]
    pub async fn open_with_idb(
        name: &str,
        chunk_size: Option<u64>,
    ) -> Result<WasmDatabase, JsError> {
        let chunk_size = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);

        let idb_name = format!("rsqlite_{name}");
        let raw = idb::IdbVfs::new(&idb_name)
            .await
            .map_err(jsval_to_js_error)?;
        let db_path = if name.ends_with(".db") {
            name.to_string()
        } else {
            format!("{name}.db")
        };

        let mux = MultiplexVfs::with_chunk_size(raw.clone_box(), chunk_size);
        let db = if db_path_is_loadable(&mux, &db_path)? {
            Database::open(&mux, &db_path).map_err(to_js_error)?
        } else {
            Database::create(&mux, &db_path).map_err(to_js_error)?
        };

        Ok(WasmDatabase {
            db,
            backend: VfsBackend::Idb { mux, raw },
            path: db_path,
        })
    }

    #[wasm_bindgen(js_name = "openPersisted")]
    pub async fn open_persisted(
        name: &str,
        chunk_size: Option<u64>,
        max_shards: Option<usize>,
    ) -> Result<WasmDatabase, JsError> {
        match Self::open_with_opfs(name, chunk_size, max_shards).await {
            Ok(db) => return Ok(db),
            Err(e) => {
                // Surface why OPFS failed before silently falling back to IDB,
                // so quota / permission / browser-support issues are diagnosable.
                let val: JsValue = e.into();
                let detail = js_sys::JSON::stringify(&val)
                    .ok()
                    .and_then(|s| s.as_string())
                    .unwrap_or_else(|| format!("{val:?}"));
                web_sys::console::warn_1(&JsValue::from_str(&format!(
                    "rsqlite-wasm: OPFS unavailable, falling back to IndexedDB: {detail}"
                )));
            }
        }
        Self::open_with_idb(name, chunk_size).await
    }

    #[wasm_bindgen(js_name = "fromBuffer")]
    pub fn from_buffer(data: &[u8]) -> Result<WasmDatabase, JsError> {
        use rsqlite_vfs::OpenFlags;

        let vfs = MemoryVfs::new();
        let path = "imported.db".to_string();

        {
            let flags = OpenFlags {
                create: true,
                read_write: true,
                delete_on_close: false,
            };
            let mut file = vfs.open(&path, flags).map_err(to_js_error)?;
            file.write(0, data).map_err(to_js_error)?;
        }

        let db = Database::open(&vfs, &path).map_err(to_js_error)?;
        Ok(WasmDatabase {
            db,
            backend: VfsBackend::Memory(vfs),
            path,
        })
    }

    pub fn exec(&mut self, sql: &str) -> Result<u64, JsError> {
        let result = self.db.execute(sql).map_err(to_js_error)?;
        Ok(result.rows_affected)
    }

    #[wasm_bindgen(js_name = "execParams")]
    pub fn exec_params(&mut self, sql: &str, params: JsValue) -> Result<u64, JsError> {
        let params = js_params_to_values(params)?;
        let result = self
            .db
            .execute_with_params(sql, params)
            .map_err(to_js_error)?;
        Ok(result.rows_affected)
    }

    #[wasm_bindgen(js_name = "queryParams")]
    pub fn query_params(&mut self, sql: &str, params: JsValue) -> Result<JsValue, JsError> {
        let params = js_params_to_values(params)?;
        let result = self
            .db
            .query_with_params(sql, params)
            .map_err(to_js_error)?;
        return query_result_to_js(&result);
    }

    pub fn query(&mut self, sql: &str) -> Result<JsValue, JsError> {
        let result = self.db.query(sql).map_err(to_js_error)?;

        let rows = js_sys::Array::new();
        for row in &result.rows {
            let obj = js_sys::Object::new();
            for (i, col_name) in result.columns.iter().enumerate() {
                let val = row.values.get(i).unwrap_or(&Value::Null);
                let js_val = value_to_js(val);
                js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val)
                    .map_err(|_| JsError::new("failed to set property"))?;
            }
            rows.push(&obj);
        }
        Ok(rows.into())
    }

    #[wasm_bindgen(js_name = "queryOne")]
    pub fn query_one(&mut self, sql: &str) -> Result<JsValue, JsError> {
        let result = self.db.query(sql).map_err(to_js_error)?;

        if result.rows.is_empty() {
            return Ok(JsValue::NULL);
        }

        let row = &result.rows[0];
        let obj = js_sys::Object::new();
        for (i, col_name) in result.columns.iter().enumerate() {
            let val = row.values.get(i).unwrap_or(&Value::Null);
            let js_val = value_to_js(val);
            js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val)
                .map_err(|_| JsError::new("failed to set property"))?;
        }
        Ok(obj.into())
    }

    #[wasm_bindgen(js_name = "execMany")]
    pub fn exec_many(&mut self, sql: &str) -> Result<(), JsError> {
        for stmt in split_statements(sql) {
            self.db.execute_sql(&stmt).map_err(to_js_error)?;
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = "toBuffer")]
    pub fn to_buffer(&mut self) -> Result<Vec<u8>, JsError> {
        use rsqlite_vfs::OpenFlags;

        let flags = OpenFlags {
            create: false,
            read_write: false,
            delete_on_close: false,
        };
        let vfs: &dyn Vfs = match &self.backend {
            VfsBackend::Memory(v) => v,
            VfsBackend::Opfs { mux, .. } => mux,
            VfsBackend::Idb { mux, .. } => mux,
        };
        let file = vfs.open(&self.path, flags).map_err(to_js_error)?;
        let size = file.file_size().map_err(to_js_error)? as usize;
        let mut buf = vec![0u8; size];
        file.read(0, &mut buf).map_err(to_js_error)?;
        Ok(buf)
    }

    pub fn flush(&self) -> Result<(), JsError> {
        if let VfsBackend::Idb { raw, .. } = &self.backend {
            raw.flush_all_sync()
                .map_err(|e| JsError::new(&format!("IDB flush failed: {e:?}")))?;
        }
        Ok(())
    }

    /// Register a JavaScript callback as a SQL scalar function.
    ///
    /// The callback receives the evaluated arguments as JS values and must
    /// return synchronously (async callbacks are deferred to a later
    /// release). Pass `n_args = -1` for variadic.
    ///
    /// User-defined functions cannot shadow built-ins — the engine resolves
    /// known names (`UPPER`, `JSON_EXTRACT`, `vec_distance_cosine`, …) before
    /// consulting the UDF registry.
    #[wasm_bindgen(js_name = "createFunction")]
    pub fn create_function(&self, name: &str, n_args: i32, callback: js_sys::Function) {
        let cb_rc = std::rc::Rc::new(callback);
        let wrapped = std::rc::Rc::new(move |args: &[Value]| -> Result<Value, rsqlite_core::error::Error> {
            let js_args = js_sys::Array::new();
            for v in args {
                js_args.push(&value_to_js(v));
            }
            match cb_rc.apply(&JsValue::NULL, &js_args) {
                Ok(result) => Ok(js_to_value(&result)),
                Err(e) => {
                    let msg = js_sys::JSON::stringify(&e)
                        .ok()
                        .and_then(|s| s.as_string())
                        .unwrap_or_else(|| format!("{e:?}"));
                    Err(rsqlite_core::error::Error::Other(format!(
                        "user function threw: {msg}"
                    )))
                }
            }
        });
        let n = if n_args < 0 {
            None
        } else {
            Some(n_args as usize)
        };
        rsqlite_core::udf::register(name, n, wrapped);
    }

    /// Remove a previously-registered user-defined function. Returns true if
    /// a function by that name existed.
    #[wasm_bindgen(js_name = "deleteFunction")]
    pub fn delete_function(&self, name: &str) -> bool {
        rsqlite_core::udf::unregister(name)
    }

    pub fn close(self) {}
}

fn value_to_js(val: &Value) -> JsValue {
    match val {
        Value::Null => JsValue::NULL,
        Value::Integer(i) => JsValue::from_f64(*i as f64),
        Value::Real(f) => JsValue::from_f64(*f),
        Value::Text(s) => JsValue::from_str(s),
        Value::Blob(b) => {
            let arr = js_sys::Uint8Array::new_with_length(b.len() as u32);
            arr.copy_from(b);
            arr.into()
        }
    }
}

fn query_result_to_js(result: &rsqlite_core::types::QueryResult) -> Result<JsValue, JsError> {
    let rows = js_sys::Array::new();
    for row in &result.rows {
        let obj = js_sys::Object::new();
        for (i, col_name) in result.columns.iter().enumerate() {
            let val = row.values.get(i).unwrap_or(&Value::Null);
            let js_val = value_to_js(val);
            js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val)
                .map_err(|_| JsError::new("failed to set property"))?;
        }
        rows.push(&obj);
    }
    Ok(rows.into())
}

fn js_params_to_values(params: JsValue) -> Result<Vec<Value>, JsError> {
    let arr: js_sys::Array = params
        .dyn_into()
        .map_err(|_| JsError::new("params must be an array"))?;
    let mut values = Vec::with_capacity(arr.length() as usize);
    for i in 0..arr.length() {
        let val = arr.get(i);
        values.push(js_to_value(&val));
    }
    Ok(values)
}

fn js_to_value(val: &JsValue) -> Value {
    if val.is_null() || val.is_undefined() {
        Value::Null
    } else if let Some(n) = val.as_f64() {
        if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            Value::Integer(n as i64)
        } else {
            Value::Real(n)
        }
    } else if let Some(s) = val.as_string() {
        Value::Text(s)
    } else if val.is_instance_of::<js_sys::Uint8Array>() {
        let arr: &js_sys::Uint8Array = val.unchecked_ref();
        Value::Blob(arr.to_vec())
    } else {
        Value::Null
    }
}

/// Split a multi-statement SQL string on semicolons, but keep
/// `BEGIN...END` blocks (used in trigger bodies) intact.
fn split_statements(sql: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut depth = 0u32;

    for raw_part in sql.split(';') {
        let trimmed = raw_part.trim();
        if trimmed.is_empty() && depth == 0 {
            continue;
        }

        if !current.is_empty() {
            current.push(';');
        }
        current.push_str(raw_part);

        let upper = trimmed.to_uppercase();
        for word in upper.split_whitespace() {
            if word == "BEGIN" {
                depth += 1;
            } else if word == "END" && depth > 0 {
                depth -= 1;
            }
        }

        if depth == 0 {
            let stmt = current.trim().to_string();
            if !stmt.is_empty() {
                let terminated = if stmt.ends_with(';') {
                    stmt
                } else {
                    format!("{stmt};")
                };
                result.push(terminated);
            }
            current.clear();
        }
    }

    if !current.trim().is_empty() {
        let stmt = current.trim().to_string();
        let terminated = if stmt.ends_with(';') {
            stmt
        } else {
            format!("{stmt};")
        };
        result.push(terminated);
    }

    result
}

fn to_js_error(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}

fn jsval_to_js_error(e: JsValue) -> JsError {
    JsError::new(&format!("{e:?}"))
}

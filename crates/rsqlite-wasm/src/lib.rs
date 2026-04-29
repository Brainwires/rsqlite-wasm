mod idb;
mod opfs;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use rsqlite_core::database::Database;
use rsqlite_core::types::Value;
use rsqlite_vfs::Vfs;
use rsqlite_vfs::memory::MemoryVfs;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

enum VfsBackend {
    Memory(MemoryVfs),
    Opfs(opfs::OpfsVfs),
    Idb(idb::IdbVfs),
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
    pub async fn open_with_opfs(name: &str) -> Result<WasmDatabase, JsError> {
        let vfs = opfs::OpfsVfs::new().await.map_err(jsval_to_js_error)?;
        let db_path = if name.ends_with(".db") {
            name.to_string()
        } else {
            format!("{name}.db")
        };

        let exists = {
            let result = vfs.open_file(&db_path, false).await;
            result.is_ok()
        };

        let db = if exists {
            Database::open(&vfs, &db_path).map_err(to_js_error)?
        } else {
            vfs.open_file(&db_path, true).await.map_err(jsval_to_js_error)?;
            Database::create(&vfs, &db_path).map_err(to_js_error)?
        };

        Ok(WasmDatabase {
            db,
            backend: VfsBackend::Opfs(vfs),
            path: db_path,
        })
    }

    #[wasm_bindgen(js_name = "openWithIdb")]
    pub async fn open_with_idb(name: &str) -> Result<WasmDatabase, JsError> {
        let idb_name = format!("rsqlite_{name}");
        let vfs = idb::IdbVfs::new(&idb_name).await.map_err(jsval_to_js_error)?;
        let db_path = if name.ends_with(".db") {
            name.to_string()
        } else {
            format!("{name}.db")
        };

        let db = if vfs.exists(&db_path).unwrap_or(false) {
            Database::open(&vfs, &db_path).map_err(to_js_error)?
        } else {
            Database::create(&vfs, &db_path).map_err(to_js_error)?
        };

        Ok(WasmDatabase {
            db,
            backend: VfsBackend::Idb(vfs),
            path: db_path,
        })
    }

    #[wasm_bindgen(js_name = "openPersisted")]
    pub async fn open_persisted(name: &str) -> Result<WasmDatabase, JsError> {
        match Self::open_with_opfs(name).await {
            Ok(db) => return Ok(db),
            Err(_) => {}
        }
        Self::open_with_idb(name).await
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
        let result = self.db.execute_with_params(sql, params).map_err(to_js_error)?;
        Ok(result.rows_affected)
    }

    #[wasm_bindgen(js_name = "queryParams")]
    pub fn query_params(&mut self, sql: &str, params: JsValue) -> Result<JsValue, JsError> {
        let params = js_params_to_values(params)?;
        let result = self.db.query_with_params(sql, params).map_err(to_js_error)?;
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
        let statements: Vec<&str> = sql.split(';').collect();
        for stmt in statements {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            let full = format!("{trimmed};");
            self.db.execute_sql(&full).map_err(to_js_error)?;
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
            VfsBackend::Opfs(v) => v,
            VfsBackend::Idb(v) => v,
        };
        let file = vfs.open(&self.path, flags).map_err(to_js_error)?;
        let size = file.file_size().map_err(to_js_error)? as usize;
        let mut buf = vec![0u8; size];
        file.read(0, &mut buf).map_err(to_js_error)?;
        Ok(buf)
    }

    pub fn flush(&self) {
        if let VfsBackend::Idb(vfs) = &self.backend {
            vfs.flush_all_sync();
        }
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

fn to_js_error(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}

fn jsval_to_js_error(e: JsValue) -> JsError {
    JsError::new(&format!("{e:?}"))
}

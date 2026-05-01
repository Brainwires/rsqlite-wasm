use std::cell::RefCell;
use std::collections::HashMap;

use rsqlite_vfs::{LockType, OpenFlags, SyncFlags, Vfs, VfsError, VfsFile};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

const DB_VERSION: u32 = 1;
const STORE_NAME: &str = "files";

pub struct IdbVfs {
    idb: web_sys::IdbDatabase,
    buffers: RefCell<HashMap<String, Vec<u8>>>,
}

unsafe impl Send for IdbVfs {}

#[wasm_bindgen(inline_js = "
export function idb_request_to_promise(req) {
    return new Promise((resolve, reject) => {
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}
export function idb_transaction_to_promise(tx) {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
}
")]
extern "C" {
    fn idb_request_to_promise(req: &JsValue) -> js_sys::Promise;
    fn idb_transaction_to_promise(tx: &web_sys::IdbTransaction) -> js_sys::Promise;
}

async fn idb_request_await(req: &JsValue) -> Result<JsValue, JsValue> {
    JsFuture::from(idb_request_to_promise(req)).await
}

#[allow(dead_code)]
async fn idb_transaction_await(tx: &web_sys::IdbTransaction) -> Result<(), JsValue> {
    JsFuture::from(idb_transaction_to_promise(tx)).await?;
    Ok(())
}

impl IdbVfs {
    pub async fn new(db_name: &str) -> Result<Self, JsValue> {
        let factory: web_sys::IdbFactory =
            js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("indexedDB"))?
                .unchecked_into();

        let open_req = factory.open_with_u32(db_name, DB_VERSION)?;

        let on_upgrade = Closure::once(move |event: web_sys::Event| {
            let target = event.target().unwrap();
            let req: web_sys::IdbOpenDbRequest = target.unchecked_into();
            let db: web_sys::IdbDatabase = req.result().unwrap().unchecked_into();
            if !db.object_store_names().contains(STORE_NAME) {
                db.create_object_store(STORE_NAME).unwrap();
            }
        });
        open_req.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));
        on_upgrade.forget();

        let idb: web_sys::IdbDatabase =
            idb_request_await(open_req.as_ref()).await?.unchecked_into();

        let mut buffers = HashMap::new();

        let tx = idb.transaction_with_str(STORE_NAME)?;
        let store = tx.object_store(STORE_NAME)?;
        let keys_req = store.get_all_keys()?;
        let keys: js_sys::Array = idb_request_await(keys_req.as_ref()).await?.unchecked_into();

        for i in 0..keys.length() {
            let key = keys.get(i);
            let key_str = key.as_string().unwrap_or_default();
            let get_req = store.get(&key)?;
            let val = idb_request_await(get_req.as_ref()).await?;
            if !val.is_undefined() && !val.is_null() {
                let arr: js_sys::Uint8Array = val.unchecked_into();
                buffers.insert(key_str, arr.to_vec());
            }
        }

        Ok(Self {
            idb,
            buffers: RefCell::new(buffers),
        })
    }

    /// Flush all in-memory buffers to IndexedDB. Errors (quota exceeded,
    /// closed connection, etc.) are propagated rather than silently dropped
    /// so callers can surface them to the application.
    pub fn flush_all_sync(&self) -> Result<(), JsValue> {
        let buffers = self.buffers.borrow();
        let tx = self
            .idb
            .transaction_with_str_and_mode(STORE_NAME, web_sys::IdbTransactionMode::Readwrite)?;
        let store = tx.object_store(STORE_NAME)?;
        for (path, data) in buffers.iter() {
            let arr = js_sys::Uint8Array::from(data.as_slice());
            store.put_with_key(&arr, &JsValue::from_str(path))?;
        }
        Ok(())
    }
}

impl Vfs for IdbVfs {
    fn open(&self, path: &str, flags: OpenFlags) -> rsqlite_vfs::Result<Box<dyn VfsFile>> {
        let mut buffers = self.buffers.borrow_mut();
        if !buffers.contains_key(path) {
            if flags.create {
                buffers.insert(path.to_string(), Vec::new());
            } else {
                return Err(VfsError::NotFound(path.to_string()));
            }
        }
        Ok(Box::new(IdbFile {
            path: path.to_string(),
            buffers: self.buffers.clone(),
            delete_on_close: flags.delete_on_close,
            lock: LockType::None,
        }))
    }

    fn delete(&self, path: &str) -> rsqlite_vfs::Result<()> {
        self.buffers.borrow_mut().remove(path);
        Ok(())
    }

    fn exists(&self, path: &str) -> rsqlite_vfs::Result<bool> {
        Ok(self.buffers.borrow().contains_key(path))
    }

    fn clone_box(&self) -> Box<dyn Vfs> {
        Box::new(IdbVfs {
            idb: self.idb.clone(),
            buffers: self.buffers.clone(),
        })
    }
}

impl Drop for IdbVfs {
    fn drop(&mut self) {
        // Drop can't propagate; surface any flush failure via console.error
        // so it shows up in the browser devtools instead of vanishing.
        if let Err(e) = self.flush_all_sync() {
            web_sys::console::error_1(&JsValue::from_str(&format!(
                "rsqlite-wasm: IDB flush failed during drop: {e:?}"
            )));
        }
        self.idb.close();
    }
}

pub struct IdbFile {
    path: String,
    buffers: RefCell<HashMap<String, Vec<u8>>>,
    delete_on_close: bool,
    lock: LockType,
}

unsafe impl Send for IdbFile {}

impl VfsFile for IdbFile {
    fn read(&self, offset: u64, buf: &mut [u8]) -> rsqlite_vfs::Result<usize> {
        let buffers = self.buffers.borrow();
        let data = buffers
            .get(&self.path)
            .ok_or_else(|| VfsError::NotFound(self.path.clone()))?;

        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(0);
        }
        let available = data.len() - offset;
        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&data[offset..offset + to_read]);
        Ok(to_read)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> rsqlite_vfs::Result<()> {
        let mut buffers = self.buffers.borrow_mut();
        let file_data = buffers
            .get_mut(&self.path)
            .ok_or_else(|| VfsError::NotFound(self.path.clone()))?;

        let offset = offset as usize;
        let needed = offset + data.len();
        if needed > file_data.len() {
            file_data.resize(needed, 0);
        }
        file_data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn file_size(&self) -> rsqlite_vfs::Result<u64> {
        let buffers = self.buffers.borrow();
        let data = buffers
            .get(&self.path)
            .ok_or_else(|| VfsError::NotFound(self.path.clone()))?;
        Ok(data.len() as u64)
    }

    fn truncate(&mut self, size: u64) -> rsqlite_vfs::Result<()> {
        let mut buffers = self.buffers.borrow_mut();
        let data = buffers
            .get_mut(&self.path)
            .ok_or_else(|| VfsError::NotFound(self.path.clone()))?;
        data.resize(size as usize, 0);
        Ok(())
    }

    fn sync(&mut self, _flags: SyncFlags) -> rsqlite_vfs::Result<()> {
        Ok(())
    }

    fn lock(&mut self, lock_type: LockType) -> rsqlite_vfs::Result<()> {
        self.lock = lock_type;
        Ok(())
    }

    fn unlock(&mut self, lock_type: LockType) -> rsqlite_vfs::Result<()> {
        self.lock = lock_type;
        Ok(())
    }
}

impl Drop for IdbFile {
    fn drop(&mut self) {
        if self.delete_on_close {
            self.buffers.borrow_mut().remove(&self.path);
        }
    }
}

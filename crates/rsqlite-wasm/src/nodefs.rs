//! A real-file VFS backed by Node's synchronous `fs` API, usable from any
//! runtime that implements the `node:fs` builtin — Node.js **and** Deno. This
//! is what gives `rsqlite-wasm` a first-class server/persistence story outside
//! the browser: unlike the OPFS/IDB backends it needs no `web-sys`, no Web
//! Worker, and no async handle pre-registration, because `node:fs` exposes
//! synchronous, offset-addressed I/O that maps 1:1 onto [`VfsFile`].
//!
//! Compiled only into the Node/Deno wasm build (the `nodefs` cargo feature).
//! The browser build must NOT include this module: the static
//! `import ... from "node:fs"` it generates would fail to resolve in a browser.
//!
//! `NativeVfs` (`rsqlite-vfs`) covers the same ground for *native* (non-wasm)
//! builds via `std::fs`, but `std::fs` cannot compile to `wasm32`, so this
//! backend re-implements it over the JS `fs` binding for the wasm target.

use js_sys::Uint8Array;
use rsqlite_vfs::{LockType, OpenFlags, SyncFlags, Vfs, VfsError, VfsFile};
use wasm_bindgen::prelude::*;

// Synchronous `node:fs` primitives. `catch` turns JS exceptions (ENOENT, EBADF,
// …) into `Err(JsValue)` instead of unwinding across the FFI boundary.
#[wasm_bindgen(module = "node:fs")]
extern "C" {
    #[wasm_bindgen(js_name = openSync, catch)]
    fn open_sync(path: &str, flags: &str) -> Result<i32, JsValue>;

    #[wasm_bindgen(js_name = closeSync, catch)]
    fn close_sync(fd: i32) -> Result<(), JsValue>;

    // fs.readSync(fd, buffer, offsetInBuffer, length, positionInFile) -> bytesRead
    #[wasm_bindgen(js_name = readSync, catch)]
    fn read_sync(
        fd: i32,
        buffer: &Uint8Array,
        offset: u32,
        length: u32,
        position: f64,
    ) -> Result<u32, JsValue>;

    // fs.writeSync(fd, buffer, offsetInBuffer, length, positionInFile) -> bytesWritten
    #[wasm_bindgen(js_name = writeSync, catch)]
    fn write_sync(
        fd: i32,
        buffer: &Uint8Array,
        offset: u32,
        length: u32,
        position: f64,
    ) -> Result<u32, JsValue>;

    #[wasm_bindgen(js_name = fstatSync, catch)]
    fn fstat_sync(fd: i32) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(js_name = ftruncateSync, catch)]
    fn ftruncate_sync(fd: i32, len: f64) -> Result<(), JsValue>;

    #[wasm_bindgen(js_name = fsyncSync, catch)]
    fn fsync_sync(fd: i32) -> Result<(), JsValue>;

    #[wasm_bindgen(js_name = fdatasyncSync, catch)]
    fn fdatasync_sync(fd: i32) -> Result<(), JsValue>;

    #[wasm_bindgen(js_name = existsSync)]
    fn exists_sync(path: &str) -> bool;

    #[wasm_bindgen(js_name = unlinkSync, catch)]
    fn unlink_sync(path: &str) -> Result<(), JsValue>;
}

fn vfs_err(ctx: &str, e: JsValue) -> VfsError {
    VfsError::Other(format!("node:fs {ctx}: {e:?}"))
}

/// A [`Vfs`] that stores each logical file as a real file on the host
/// filesystem via `node:fs`. Stateless (a ZST) — every open resolves a fresh
/// file descriptor on demand, so `clone_box` is a no-op copy, exactly like
/// `NativeVfs`.
pub struct NodeFsVfs;

impl NodeFsVfs {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NodeFsVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vfs for NodeFsVfs {
    fn open(&self, path: &str, flags: OpenFlags) -> rsqlite_vfs::Result<Box<dyn VfsFile>> {
        // Map OpenFlags onto node's fopen(3) mode strings. There is no string
        // mode for "read/write, create if missing, do NOT truncate", so we pick
        // "r+" vs "w+" by existence — matching NativeVfs, which likewise never
        // truncates an existing file on create.
        let mode = if !flags.read_write {
            "r"
        } else if flags.create {
            if exists_sync(path) { "r+" } else { "w+" }
        } else {
            "r+"
        };
        let fd = open_sync(path, mode).map_err(|e| vfs_err(&format!("open {path}"), e))?;
        Ok(Box::new(NodeFsFile {
            fd,
            path: path.to_string(),
            delete_on_close: flags.delete_on_close,
            lock: LockType::None,
        }))
    }

    fn delete(&self, path: &str) -> rsqlite_vfs::Result<()> {
        if exists_sync(path) {
            unlink_sync(path).map_err(|e| vfs_err(&format!("unlink {path}"), e))?;
        }
        Ok(())
    }

    fn exists(&self, path: &str) -> rsqlite_vfs::Result<bool> {
        Ok(exists_sync(path))
    }

    fn clone_box(&self) -> Box<dyn Vfs> {
        Box::new(NodeFsVfs)
    }
}

/// An open file descriptor. Holds only `Copy`/owned data (no JS handle), so it
/// is `Send` without any `unsafe impl`.
pub struct NodeFsFile {
    fd: i32,
    path: String,
    delete_on_close: bool,
    lock: LockType,
}

impl VfsFile for NodeFsFile {
    fn read(&self, offset: u64, buf: &mut [u8]) -> rsqlite_vfs::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let len = buf.len() as u32;
        let js_buf = Uint8Array::new_with_length(len);
        let n =
            read_sync(self.fd, &js_buf, 0, len, offset as f64).map_err(|e| vfs_err("read", e))?;
        if n > 0 {
            // Copy back only the bytes actually read.
            js_buf.subarray(0, n).copy_to(&mut buf[..n as usize]);
        }
        Ok(n as usize)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> rsqlite_vfs::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let js_buf = Uint8Array::from(data);
        let total = data.len() as u32;
        let mut written = 0u32;
        // Loop to tolerate short writes (rare, but node makes no full-write
        // guarantee for writeSync); mirrors NativeVfs's write_all.
        while written < total {
            let n = write_sync(
                self.fd,
                &js_buf,
                written,
                total - written,
                offset as f64 + written as f64,
            )
            .map_err(|e| vfs_err("write", e))?;
            if n == 0 {
                return Err(VfsError::Other("node:fs write: zero-length write".into()));
            }
            written += n;
        }
        Ok(())
    }

    fn file_size(&self) -> rsqlite_vfs::Result<u64> {
        let stats = fstat_sync(self.fd).map_err(|e| vfs_err("fstat", e))?;
        let size = js_sys::Reflect::get(&stats, &JsValue::from_str("size"))
            .ok()
            .and_then(|v| v.as_f64())
            .ok_or_else(|| VfsError::Other("node:fs fstat: missing size".into()))?;
        Ok(size as u64)
    }

    fn truncate(&mut self, size: u64) -> rsqlite_vfs::Result<()> {
        ftruncate_sync(self.fd, size as f64).map_err(|e| vfs_err("ftruncate", e))?;
        Ok(())
    }

    fn sync(&mut self, flags: SyncFlags) -> rsqlite_vfs::Result<()> {
        if flags.full {
            fsync_sync(self.fd).map_err(|e| vfs_err("fsync", e))?;
        } else {
            fdatasync_sync(self.fd).map_err(|e| vfs_err("fdatasync", e))?;
        }
        Ok(())
    }

    fn lock(&mut self, lock_type: LockType) -> rsqlite_vfs::Result<()> {
        // Advisory only. Cross-process file locking (flock/fcntl) is not yet
        // exposed by node:fs sync; single-writer use is the supported mode.
        self.lock = lock_type;
        Ok(())
    }

    fn unlock(&mut self, lock_type: LockType) -> rsqlite_vfs::Result<()> {
        self.lock = lock_type;
        Ok(())
    }
}

impl Drop for NodeFsFile {
    fn drop(&mut self) {
        // Release the descriptor; best-effort (nothing actionable on error).
        let _ = close_sync(self.fd);
        if self.delete_on_close {
            let _ = unlink_sync(&self.path);
        }
    }
}

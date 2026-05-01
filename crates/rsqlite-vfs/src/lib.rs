//! # rsqlite-vfs
//!
//! VFS abstraction over storage backends. Implementors expose paged
//! random-access I/O so the engine doesn't care whether bytes live in OPFS,
//! IndexedDB, the native filesystem, or RAM.
//!
//! Provided backends:
//! - [`memory::MemoryVfs`] — in-process in-memory store, used for tests and
//!   the `:memory:` open mode.
//! - [`native::NativeVfs`] — std::fs-backed (only built with the `native`
//!   feature, which is on by default for non-WASM targets). The browser
//!   targets use OPFS / IDB backends in the `rsqlite-wasm` crate.
//! - [`multiplex::MultiplexVfs`] — wraps any inner VFS and shards a logical
//!   file across N capped-size physical files. Used to escape browser
//!   per-file size caps.

pub mod error;
pub mod memory;
pub mod multiplex;
#[cfg(feature = "native")]
pub mod native;

pub use error::VfsError;
pub use multiplex::{DEFAULT_CHUNK_SIZE, MultiplexVfs};

pub type Result<T> = std::result::Result<T, VfsError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    None,
    Shared,
    Reserved,
    Pending,
    Exclusive,
}

#[derive(Debug, Clone, Copy)]
pub struct SyncFlags {
    pub full: bool,
}

#[derive(Debug, Clone)]
pub struct OpenFlags {
    pub create: bool,
    pub read_write: bool,
    pub delete_on_close: bool,
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self {
            create: true,
            read_write: true,
            delete_on_close: false,
        }
    }
}

/// A VFS — virtual file system. Implementors provide named-file open/delete
/// semantics over some storage substrate.
pub trait Vfs: Send {
    /// Open `path` with the given flags. Returns a file handle for paged I/O.
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Box<dyn VfsFile>>;
    /// Remove `path` if it exists.
    fn delete(&self, path: &str) -> Result<()>;
    /// Whether `path` exists in this VFS.
    fn exists(&self, path: &str) -> Result<bool>;
    /// Clone this VFS into a new boxed handle. Implementations should share
    /// any underlying state (e.g. an in-memory file map) so the clone sees
    /// the same files as the original.
    fn clone_box(&self) -> Box<dyn Vfs>;
}

/// A handle to a file inside a [`Vfs`]. All I/O is offset-addressed; the
/// engine never seeks.
pub trait VfsFile: Send {
    /// Read up to `buf.len()` bytes starting at `offset`. Returns the number
    /// of bytes actually read; 0 indicates EOF.
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
    /// Write `data` starting at `offset`, extending the file if necessary.
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()>;
    /// Current file size in bytes.
    fn file_size(&self) -> Result<u64>;
    /// Truncate or extend the file to exactly `size` bytes (zero-fill on grow).
    fn truncate(&mut self, size: u64) -> Result<()>;
    /// Flush buffered writes to the storage substrate.
    fn sync(&mut self, flags: SyncFlags) -> Result<()>;
    /// Acquire a logical lock at the given level. Backends without
    /// cross-process semantics may treat this as advisory state-tracking only.
    fn lock(&mut self, lock_type: LockType) -> Result<()>;
    /// Downgrade or release the current lock.
    fn unlock(&mut self, lock_type: LockType) -> Result<()>;
}

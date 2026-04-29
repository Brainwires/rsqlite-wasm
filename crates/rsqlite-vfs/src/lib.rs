pub mod error;
pub mod memory;
#[cfg(feature = "native")]
pub mod native;

pub use error::VfsError;

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

pub trait Vfs: Send {
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Box<dyn VfsFile>>;
    fn delete(&self, path: &str) -> Result<()>;
    fn exists(&self, path: &str) -> Result<bool>;
}

pub trait VfsFile: Send {
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()>;
    fn file_size(&self) -> Result<u64>;
    fn truncate(&mut self, size: u64) -> Result<()>;
    fn sync(&mut self, flags: SyncFlags) -> Result<()>;
    fn lock(&mut self, lock_type: LockType) -> Result<()>;
    fn unlock(&mut self, lock_type: LockType) -> Result<()>;
}

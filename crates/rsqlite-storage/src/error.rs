use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("VFS error: {0}")]
    Vfs(#[from] rsqlite_vfs::VfsError),

    #[error("invalid database header: {0}")]
    InvalidHeader(String),

    #[error("corrupt database: {0}")]
    Corrupt(String),

    #[error("page {0} out of range (database has {1} pages)")]
    PageOutOfRange(u32, u32),

    #[error("overflow: {0}")]
    Overflow(String),

    #[error("storage error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("file not found: {0}")]
    NotFound(String),

    #[error("lock conflict: cannot acquire {requested:?} lock while in {current:?}")]
    LockConflict {
        current: crate::LockType,
        requested: crate::LockType,
    },

    #[error("read past end of file at offset {offset} (file size: {file_size})")]
    ReadPastEnd { offset: u64, file_size: u64 },

    #[error("vfs error: {0}")]
    Other(String),
}

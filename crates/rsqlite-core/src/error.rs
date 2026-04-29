use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(#[from] rsqlite_parser::error::ParseError),

    #[error("storage error: {0}")]
    Storage(#[from] rsqlite_storage::error::StorageError),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

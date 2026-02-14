use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ZolaError {
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid file {path}: {reason}")]
    InvalidFile { path: PathBuf, reason: String },
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
}

impl ZolaError {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        ZolaError::Io {
            path: path.into(),
            source,
        }
    }

    pub fn invalid(path: impl Into<PathBuf>, reason: impl Into<String>) -> Self {
        ZolaError::InvalidFile {
            path: path.into(),
            reason: reason.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, ZolaError>;

use std::{error::Error, fmt::Display, sync::PoisonError};

use thiserror::Error as ErrorMacro;

use super::{StringError, DatabaseError};

#[derive(ErrorMacro, Debug)]
pub enum StorageError {
    #[error("Cannot parse the file")] Parse(#[from] serde_json::Error),
    #[error("Cannot access the caches")] CacheAccess(#[from] CacheAccessError),
    #[error("Cannot load the file")] Io(#[from] std::io::Error),
    #[error("Cannot compress/decompress data")] Compression(#[from] CompressionError),
    #[error("Data does not keep the schema structure")] Schema(#[from] SchemaError),
    #[error("File inconsistency")] Inconsistency()
}

#[derive(ErrorMacro, Debug)]
pub struct CompressionError(pub Box<dyn Error>);

impl CompressionError {
    pub fn from_str(text: &'static str) -> Self {
        CompressionError::wrap(StringError::Static(text))
    }

    pub fn from_string(text: String) -> Self {
        CompressionError::wrap(StringError::Owning(text))
    }

    pub fn wrap(error: impl Error + 'static) -> Self {
        CompressionError(Box::new(error))
    }
}

impl Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(ErrorMacro, Debug)]
pub struct CacheAccessError(pub Box<dyn Error>);

impl Display for CacheAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(ErrorMacro, Debug)]
pub struct SchemaError(pub Box<dyn Error>);

impl SchemaError {
    fn wrap(error: impl Error + 'static) -> Self {
        SchemaError(Box::new(error))
    }

    pub fn from_str(text: &'static str) -> Self {
        SchemaError::wrap(StringError::Static(text))
    }

    pub fn from_string(text: String) -> Self {
        SchemaError::wrap(StringError::Owning(text))
    }
}

impl Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<serde_json::Error> for DatabaseError {
    fn from(value: serde_json::Error) -> Self {
        StorageError::from(value).into()
    }
}

impl<T> From<PoisonError<T>> for DatabaseError {
    fn from(value: PoisonError<T>) -> Self {
        StorageError::from(CacheAccessError(Box::new(StringError::Owning(value.to_string())))).into()
    }
}

impl From<std::io::Error> for DatabaseError {
    fn from(value: std::io::Error) -> Self {
        StorageError::from(value).into()
    }
}

impl From<CompressionError> for DatabaseError {
    fn from(value: CompressionError) -> Self {
        StorageError::from(value).into()
    }
}

impl From<SchemaError> for DatabaseError {
    fn from(value: SchemaError) -> Self {
        StorageError::from(value).into()
    }
}

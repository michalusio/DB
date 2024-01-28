use std::fmt::Display;

use thiserror::Error as ErrorMacro;

use self::{storage_error::StorageError, client_side_error::ClientSideError, compaction_error::CompactionError, query_error::QueryError};

pub mod storage_error;
pub mod client_side_error;
pub mod compaction_error;
pub mod query_error;

#[derive(ErrorMacro, Debug)]
pub enum DatabaseError {
    #[error("Error while querying the database")] Query(#[from] QueryError),
    #[error("Error while storing the data")] Storage(#[from] StorageError),
    #[error("Error while compacting log files")] Compaction(#[from] CompactionError),
    #[error("The operation requested resulted in a client-side error")] ClientSide(#[from] ClientSideError),
}

#[derive(ErrorMacro, Debug)]
pub(crate) enum StringError {
    Owning(String),
    Static(&'static str)
}

impl Display for StringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StringError::Owning(s) => s.fmt(f),
            StringError::Static(s) => s.fmt(f)
        }
    }
}
use std::{error::Error, fmt::Display};

use thiserror::Error as ErrorMacro;

use super::StringError;

#[derive(ErrorMacro, Debug)]
pub struct CompactionError(pub Box<dyn Error>);

impl CompactionError {
    pub fn from_str(text: &'static str) -> Self {
        CompactionError::wrap(StringError::Static(text))
    }

    pub fn wrap(error: impl Error + 'static) -> Self {
        CompactionError(Box::new(error))
    }
}

impl Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
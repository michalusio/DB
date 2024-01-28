use std::fmt::Display;

use thiserror::Error as ErrorMacro;

use super::StringError;

#[derive(ErrorMacro, Debug)]
pub enum QueryError {
    #[error("Error while deserializing a record")] DeserializerError(#[from] DeserializerError)
}

#[derive(ErrorMacro, Debug)]
pub struct DeserializerError(StringError);

impl Display for DeserializerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl serde::de::Error for DeserializerError {
    fn custom<T>(msg:T) -> Self where T:Display {
        DeserializerError(StringError::Owning(msg.to_string()))
    }
}

impl DeserializerError {
    pub fn from_str(s: &'static str) -> Self {
        DeserializerError(StringError::Static(s))
    }
}
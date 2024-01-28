use std::fmt::Display;

use thiserror::Error as ErrorMacro;

#[derive(ErrorMacro, Debug)]
pub enum ClientSideError {
    #[error("Collection already exists")] CollectionAlreadyExistsError(#[from] CollectionAlreadyExistsError),
    #[error("Collection does not exist")] CollectionDoesNotExistError(#[from] CollectionDoesNotExistError)
}

#[derive(ErrorMacro, Debug)]
pub struct CollectionAlreadyExistsError {
    pub name: String
}

impl Display for CollectionAlreadyExistsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Collection \"{}\" already exists.", self.name))
    }
}

#[derive(ErrorMacro, Debug)]
pub struct CollectionDoesNotExistError {
    pub name: String
}

impl Display for CollectionDoesNotExistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Collection \"{}\" already exists.", self.name))
    }
}
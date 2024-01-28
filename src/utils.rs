mod split_by_length_encoding;
pub use split_by_length_encoding::SplittableByLengthEncoding;

mod set_macro;

use crate::errors::DatabaseError;

pub type DBResult<T> = Result<T, DatabaseError>;
mod split_by_length_encoding;
use std::sync::RwLockReadGuard;

pub use split_by_length_encoding::SplittableByLengthEncoding;

mod set_macro;

use crate::errors::DatabaseError;

pub type DBResult<T> = Result<T, DatabaseError>;

pub unsafe fn make_static<'a, T>(t: RwLockReadGuard<'a, T>) -> RwLockReadGuard<'static, T> {
    core::mem::transmute(t)
}
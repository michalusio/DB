mod split_by_length_encoding;

use std::sync::{LockResult, MutexGuard, RwLockReadGuard};

use log_err::LogErrResult;
pub use split_by_length_encoding::SplittableByLengthEncoding;

mod set_macro;
mod yokeable;
pub use yokeable::*;

use crate::errors::DatabaseError;

pub type DBResult<T> = Result<T, DatabaseError>;

#[allow(dead_code)]
pub(crate) trait GuardExtensions<T>: Sized {
    fn not_poisoned(self) -> T;
}

impl<'a, T: 'a + Sized> GuardExtensions<RwLockReadGuard<'a, T>> for LockResult<RwLockReadGuard<'a, T>> {
    fn not_poisoned(self) -> RwLockReadGuard<'a, T> {
        self.log_expect("The RwLock is poisoned")
    }
}

impl<'a, T: 'a + Sized> GuardExtensions<MutexGuard<'a, T>> for LockResult<MutexGuard<'a, T>> {
    fn not_poisoned(self) -> MutexGuard<'a, T> {
        self.log_expect("The Mutex is poisoned")
    }
}

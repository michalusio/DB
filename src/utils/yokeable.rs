use std::{ops::{Deref}, sync::{RwLockReadGuard}};
use yoke::{Yokeable};

#[derive(Yokeable)]
pub struct RwLockReadGuardian<'a, T>(pub RwLockReadGuard<'a, T>);

impl<'a, T> Deref for RwLockReadGuardian<'a, T> {
    type Target = RwLockReadGuard<'a, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

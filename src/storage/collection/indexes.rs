use crate::storage::log_file::log_entry::LogEntry;

use self::{sorted::SortedIndex, reverse::ReverseIndex, bitmap::BitmapIndex, hash::HashIndex};

mod sorted;
mod reverse;
mod bitmap;
mod hash;

#[derive(Clone, Debug)]
pub enum WrappedIndex {
    Sorted(SortedIndex),
    Reverse(ReverseIndex),
    Bitmap(BitmapIndex),
    Hash(HashIndex)
}

pub trait Index {
    fn update(&mut self, entry: &LogEntry);
}
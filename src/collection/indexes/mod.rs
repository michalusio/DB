use uuid::Uuid;

use crate::{ObjectField};

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
    fn update(&mut self, entry_id: Uuid, entry: &Box<[ObjectField]>);
}
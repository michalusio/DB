use std::hash::Hash;

mod nested_loop; pub use nested_loop::NestedLoop;
mod hash_match; pub use hash_match::HashMatch;

use crate::ObjectField;

pub trait IntoHashable {
    type Item: Hash + Clone + Eq;
    fn to_hashable(&self) -> Self::Item;
}

impl<T: Hash + Clone + Eq> IntoHashable for T {
    type Item = T;
    
    fn to_hashable(&self) -> Self::Item {
        self.clone()
    }
}

impl IntoHashable for ObjectField<'_> {
    type Item = Box<[u8]>;

    fn to_hashable(&self) -> Self::Item {
        let bytes: &[u8] = match self {
            ObjectField::Bool(b) => &[*b as u8],
            ObjectField::I32(i) => &i.to_le_bytes(),
            ObjectField::I64(i) => &i.to_le_bytes(),
            ObjectField::Decimal(d) => &d.to_le_bytes(),
            ObjectField::Id(uuid) => &uuid.to_bytes_le(),
            ObjectField::Bytes(cow) => cow.as_ref(),
            ObjectField::String(cow) => cow.as_bytes(),
        };
        (*bytes).into()
    }
}

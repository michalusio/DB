use uuid::Uuid;

use crate::{objects::{ObjectField}};

use super::Index;

#[derive(Clone, Debug)]
pub struct ReverseIndex {
    pub column: usize,
    pub descending: bool,
    pub data: Vec<(ObjectField<'static>, Vec<Uuid>)>
}

impl Index for ReverseIndex {
    fn update(&mut self, entry_id: Uuid, entry: &Box<[ObjectField]>) {
        let column_data = entry[self.column].clone();
    }
}

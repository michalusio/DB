use uuid::Uuid;

use crate::{objects::{ObjectField}};

use super::Index;

#[derive(Clone, Debug)]
pub struct SortedIndex {
    pub column: usize,
    pub descending: bool,
    pub data: Vec<(ObjectField, Uuid)>
}

impl Index for SortedIndex {
    fn update(&mut self, entry_id: Uuid, entry: &Box<[ObjectField]>) {
        let column_data = entry[self.column].clone();
    }
}

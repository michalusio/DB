use uuid::Uuid;

use crate::ObjectField;

use super::Index;

#[derive(Clone, Debug)]
pub struct BitmapIndex {
    pub column: usize,
    pub data: Vec<u64>
}

impl Index for BitmapIndex {
    fn update(&mut self, entry_id: Uuid, entry: &Box<[ObjectField]>) {
        let column_data = entry[self.column].clone();
    }
}
use std::collections::HashMap;

use uuid::Uuid;

use crate::{objects::{ObjectField}};

use super::Index;

#[derive(Clone, Debug)]
pub struct HashIndex {
    pub column: usize,
    pub data: HashMap<ObjectField, Vec<Uuid>>
}

impl Index for HashIndex {
    fn update(&mut self, entry_id: Uuid, entry: &Box<[ObjectField]>) {
        let column_data = entry[self.column].clone();
    }
}
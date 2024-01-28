use std::collections::HashMap;

use uuid::Uuid;

use crate::{objects::{ObjectField, ObjectState}, storage::log_file::log_entry::LogEntry};

use super::Index;

#[derive(Clone, Debug)]
pub struct HashIndex {
    pub column: usize,
    pub data: HashMap<ObjectField, Vec<Uuid>>
}

impl Index for HashIndex {
    fn update(&mut self, entry: &LogEntry) {
        match entry.entry_data() {
            ObjectState::Tombstone => {

            },
            ObjectState::ObjectValues(values) => {
                let column_data = values[self.column].clone();
            }
        }
    }
}
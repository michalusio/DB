use uuid::Uuid;

use crate::{objects::{ObjectField, ObjectState}, storage::log_file::log_entry::LogEntry};

use super::Index;

#[derive(Clone, Debug)]
pub struct SortedIndex {
    pub column: usize,
    pub descending: bool,
    pub data: Vec<(ObjectField, Uuid)>
}

impl Index for SortedIndex {
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
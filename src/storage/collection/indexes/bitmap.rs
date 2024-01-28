use crate::{storage::log_file::log_entry::LogEntry, objects::ObjectState};

use super::Index;

#[derive(Clone, Debug)]
pub struct BitmapIndex {
    pub column: usize,
    pub data: Vec<u64>
}

impl Index for BitmapIndex {
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
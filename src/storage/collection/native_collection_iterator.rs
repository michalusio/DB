use std::collections::HashSet;
use std::iter::FusedIterator;

use bumpalo::{Bump, collections::Vec};
use uuid::Uuid;

use super::Collection;
use crate::ObjectField;
use crate::objects::ObjectState::{self, ObjectValues};
use crate::storage::log_file::LogFile;
use crate::utils::DBResult;

pub(crate) struct NativeCollectionIterator<'a> {
    collection: &'a Collection,
    current_file_index: usize,
    current_file: Option<LogFile<'a>>,
    current_file_entry: usize,
    visited_ids: HashSet<Uuid>,
    arena: Bump,
    index: usize
}

impl<'a> NativeCollectionIterator<'a> {

    pub fn new(collection: &'a Collection) -> Self {
        let approx_entries = collection.statistics.approximate_entries();
        Self {
            collection,
            current_file_index: collection.log_files.len() - 1,
            current_file: None,
            current_file_entry: 0,
            visited_ids: HashSet::with_capacity(approx_entries + 10),
            arena: Bump::new(),
            index: 0
        }
    }
}

impl<'a> Iterator for NativeCollectionIterator<'a> {
    type Item = DBResult<ObjectState<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            println!("Index: {}, Arena mem: {}", self.index, self.arena.allocated_bytes());
            if let Some(file) = self.current_file.take() {
                if let Some(entry) = file.get(self.current_file_entry) {
                    self.current_file_entry += 1;
                    if self.visited_ids.insert(entry.object_id()) {
                        if let ObjectValues(v) = entry.entry_data() {
                            let pointer = &v as *const Vec<'_, ObjectField>;
                            std::mem::forget(v);
                            unsafe {
                                let pointer: *const Vec<'a, ObjectField> = std::mem::transmute(pointer);
                                let v = pointer.read();
                                self.index += 1;
                                return Some(Ok(ObjectValues(v)));
                            }
                        }
                    }
                    continue;
                }
                if self.current_file_index == 0 {
                    return None;
                }
                self.current_file_entry = 0;
                self.current_file_index -= 1;
                self.current_file = None;
            } else {
                match self.collection.get_file(self.current_file_index, &self.arena) {
                    None => {
                        if self.current_file_index == 0 {
                            return None;
                        } else {
                            self.current_file_entry = 0;
                            self.current_file_index -= 1;
                        }
                    },
                    Some(Err(err)) => return Some(Err(err)),
                    Some(Ok(file)) => {
                        let pointer = &file as *const LogFile<'_>;
                        std::mem::forget(file);
                        unsafe {
                            let pointer: *const LogFile<'a> = std::mem::transmute(pointer);
                            let file = pointer.read();
                            self.current_file = Some(file);
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let approx_entries = self.collection.statistics.approximate_entries();

        let files_left = self.collection.log_files.len() - self.current_file_index;
        let min_item_count = files_left;
        if self.current_file_index == 0 {
            (min_item_count, Some(approx_entries))
        } else {
            let approx_entries_per_file = 1 + self.visited_ids.len() / self.current_file_index;
            (min_item_count, Some((approx_entries + approx_entries_per_file * files_left) >> 1))
        }
    }
}

impl<'a> FusedIterator for NativeCollectionIterator<'a> {}
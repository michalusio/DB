use std::{ops::ControlFlow::Break, path::PathBuf, sync::{Arc, Mutex}};

use itertools::Itertools;
use schnellru::{ByLength, LruMap};
use uuid::Uuid;

use crate::{errors::storage_error::{SchemaError, StorageError}, operators::TableScan, storage::log_file::{log_entry::{EntityEntry, LogEntry}, LogFile}, utils::{DBResult, GuardExtensions}, DBOperator, Row};

use self::{collection_config::CollectionConfig, indexes::WrappedIndex, collection_statistics::CollectionStatistics};

pub mod collection_config;
mod collection_statistics;
mod indexes;

pub struct Collection {
    pub(crate) last_file_index: usize,
    log_files: Mutex<LruMap<usize, Arc<LogFile>, ByLength>>,
    pub config: CollectionConfig,
    indexes: Vec<WrappedIndex>,
    pub(crate) statistics: CollectionStatistics
}

impl Collection {
    pub(crate) fn new(mut config: CollectionConfig) -> DBResult<Collection> {
        config.ensure_folder_exists()?;
        config.ensure_file_exists(0)?;

        let file_count = config.storage_config.cache.file_count;

        let collection = Collection {
            last_file_index: config.get_log_file_paths()?.len() - 1,
            config,
            log_files: Mutex::new(LruMap::new(ByLength::new(file_count as u32))),
            indexes: std::vec::Vec::new(),
            statistics: CollectionStatistics::default()
        };

        Ok(collection)
    }

    pub fn name(&self) -> &str {
        &self.config.collection_name
    }

    /// Sets the state of the objects with the given ids to the given states
    pub fn set_objects(&mut self, transaction_id: Uuid, objects: impl IntoIterator<Item = Row>) -> DBResult<usize> {
        let entries = objects
            .into_iter()
            .map(|row| if row.fields.is_empty() { EntityEntry::Deleted(row.id) } else { EntityEntry::Updated(row) })
            .collect_vec();

        if let Some(value_entry) = self.get_any_entry_data()? {
            let error_entry = entries
                .iter()
                .find(|e| e.is_same_shape(&value_entry) == Break(false));
            if let Some(e) = error_entry {
                return Err(SchemaError::from_string(e.object_id().to_string()).into());
            }
        }

        let mut newest_file = self.get_file(self.last_file_index)?.ok_or(StorageError::Inconsistency())?;
        let max_entries = self.config.storage_config.log_file.max_entries;

        let mut leftover = max_entries - newest_file.read()?.len();
        let mut appends = 0;

        let mut entries_left = entries.len();
        for chunk in entries.into_iter()
            .batching(|iter| {
                let vec = iter.take(leftover).collect::<Vec<_>>();
                if vec.is_empty() {
                    None
                } else {
                    leftover = max_entries;
                    Some(vec)
                }
            }) {
            let added_entries = LogFile::append_entries(&newest_file, &self.config, chunk.into_iter().map(|e| LogEntry::Entity(transaction_id, e)))?;

            appends += added_entries;
            entries_left -= added_entries;

            if entries_left > 0 {
                self.last_file_index += 1;
                self.config.ensure_file_exists(self.last_file_index)?;
                newest_file = self.get_file(self.last_file_index)?.ok_or(StorageError::Inconsistency())?;
            }
        }

        self.statistics.approximate_total_entries += appends;

        Ok(appends)
    }

    /// Retrieves the specified log file from the collection
    pub(crate) fn get_file(&self, index: usize) -> DBResult<Option<Arc<LogFile>>> {
        match self.log_files
            .lock()
            .not_poisoned()
            .get_or_insert_fallible(index, || LogFile::load_log_file(&self.config, index).map(Arc::new)) {
            Ok(Some(file)) => Ok(Some(file.clone())),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn get_any_entry_data(&self) -> DBResult<Option<EntityEntry>> {
        match TableScan::new(self, Uuid::max()).next() {
            Ok(Some(row)) => Ok(Some(EntityEntry::Updated(row))),
            Ok(None) => Ok(None),
            Err(err) => Err(err)
        }
    }

    pub fn table_scan(&self, transaction_id: Uuid) -> TableScan<'_> {
        TableScan::new(self, transaction_id)
    }

    pub fn print_debug_info(&self) -> CollectionDebugInfo {
        let lock = self.log_files.lock().not_poisoned();
        CollectionDebugInfo {
            file_cache_slots_used: lock.len(),
            file_cache_slots_max: lock.limiter().max_length() as usize,
            file_cache_memory_used: lock.iter().map(|e| e.1.byte_size()).sum(),
            log_file_directory: self.config.get_collection_files_destination(),
            log_file_last_index: self.last_file_index
        }
    }

    pub fn clear_cache(&self) {
        let mut lock = self.log_files.lock().not_poisoned();
        lock.clear();
    }
}

#[derive(Clone)]
pub struct CollectionDebugInfo {
    pub file_cache_slots_used: usize,
    pub file_cache_slots_max: usize,
    pub file_cache_memory_used: usize,
    pub log_file_directory: PathBuf,
    pub log_file_last_index: usize
}

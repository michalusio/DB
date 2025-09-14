use std::{ops::ControlFlow::Break, sync::{Arc, Mutex}};

use super::{log_file::{LogFile, log_entry::LogEntry}, query::Query};
use itertools::Itertools;
use schnellru::{ByMemoryUsage, LruMap};
use serde::Deserialize;
use uuid::Uuid;

use crate::{errors::storage_error::{SchemaError, StorageError}, storage::log_file::log_entry::EntityEntry, utils::DBResult, ObjectField};

use self::{collection_config::CollectionConfig, indexes::WrappedIndex, collection_statistics::CollectionStatistics, collection_iterator::CollectionIterator, native_collection_iterator::NativeCollectionIterator};

pub mod collection_config;
mod collection_statistics;
mod indexes;
mod native_collection_iterator;
mod collection_iterator;

pub struct Collection {
    last_file_index: usize,
    log_files: Mutex<LruMap<usize, Arc<LogFile>, ByMemoryUsage>>,
    config: CollectionConfig,
    indexes: Vec<WrappedIndex>,
    statistics: CollectionStatistics
}

impl Collection {
    pub(crate) fn new(mut config: CollectionConfig) -> DBResult<Collection> {
        config.ensure_folder_exists()?;
        config.ensure_file_exists(0)?;

        let file_budget = config.storage_config.cache.file_budget;

        let collection = Collection {
            last_file_index: config.get_log_file_paths()?.len() - 1,
            config,
            log_files: Mutex::new(LruMap::with_memory_budget(file_budget)),
            indexes: std::vec::Vec::new(),
            statistics: CollectionStatistics::default()
        };

        Ok(collection)
    }

    pub fn name(&self) -> &str {
        &self.config.collection_name
    }

    /// Sets the state of the object with the given id to the given state
    pub fn set_object(&mut self, transaction_id: Uuid, object_id: Uuid, fields: Arc<[ObjectField]>) -> DBResult<usize> {
        self.set_objects(transaction_id, [(object_id, fields)])
    }

    /// Sets the state of the objects with the given ids to the given states
    pub fn set_objects(&mut self, transaction_id: Uuid, objects: impl IntoIterator<Item = (Uuid, Arc<[ObjectField]>)>) -> DBResult<usize> {
        let entries = objects
            .into_iter()
            .map(|(id, state)| if state.is_empty() { EntityEntry::Deleted(id) } else { EntityEntry::Updated(id, state) })
            .collect_vec();

        if let Some(value_entry) = self.get_any_entry_data()? {
            let error_entry = entries
                .iter()
                .find(|e| matches!(
                    e.is_same_shape(&value_entry),
                    Break(false)
                ));
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

    pub fn get_file(&self, index: usize) -> DBResult<Option<Arc<LogFile>>> {
        match self.log_files
            .lock()?
            .get_or_insert_fallible(index, || LogFile::load_log_file(&self.config, index).map(Arc::new)) {
            Ok(Some(file)) => Ok(Some(file.clone())),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn get_any_entry_data(&self) -> DBResult<Option<EntityEntry>> {
        match NativeCollectionIterator::new(self).next() {
            None => Ok(None),
            Some(Ok((id, values))) => Ok(Some(EntityEntry::Updated(id, values))),
            Some(Err(err)) => Err(err)
        }
    }

    pub fn iterate<'a, Item: Deserialize<'a>>(&'a self) -> CollectionIterator<'a, Item> {
        CollectionIterator::new(NativeCollectionIterator::new(self))
    }

    pub fn iterate_native(&'_ self) -> NativeCollectionIterator<'_> {
        NativeCollectionIterator::new(self)
    }

    pub fn query<'a, Item: Deserialize<'a> + 'a>(&'a self) -> Query<'a, Item> {
        Query::<Item>::from_collection(self)
    }

    pub fn print_debug_info(&self) {
        let lock = self.log_files.lock().unwrap();
        println!(
            "File cache memory usage: {}B/{}B",
            lock.memory_usage(),
            lock.limiter().max_memory_usage()
        );
        println!("Log file directory: {:#?}", self.config.get_collection_files_destination());
        println!("Last file index: {:#?}", self.last_file_index);
    }

    pub fn clear_cache(&self) {
        let mut lock = self.log_files.lock().unwrap();
        lock.clear();
    }

}

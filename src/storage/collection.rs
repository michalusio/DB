use std::ops::ControlFlow::Break;

use super::{log_file::{ LogFile, log_entry::LogEntry, LogFileData}, query::Query};
use bumpalo::{Bump, collections::{CollectIn, Vec}};
use itertools::Itertools;
use serde::Deserialize;
use uuid::Uuid;

use crate::{objects::ObjectState, errors::storage_error::SchemaError, utils::DBResult};

use self::{collection_config::CollectionConfig, indexes::WrappedIndex, collection_statistics::CollectionStatistics, collection_iterator::CollectionIterator, native_collection_iterator::NativeCollectionIterator};

pub mod collection_config;
mod collection_statistics;
mod indexes;
mod native_collection_iterator;
mod collection_iterator;

pub struct Collection {
    last_file_entries: usize,
    log_files: std::vec::Vec<LogFileData>,
    config: CollectionConfig,
    indexes: std::vec::Vec<WrappedIndex>,
    statistics: CollectionStatistics
}

impl Collection {
    pub(crate) fn new(mut config: CollectionConfig) -> DBResult<Collection> {
        config.ensure_folder_exists()?;

        let mut log_paths = config.get_log_file_paths()?;

        if log_paths.is_empty() {
            log_paths.push(LogFile::create_next_log_file(&mut config)?);
        }

        let mut collection = Collection {
            config,
            last_file_entries: 0,
            log_files: log_paths,
            indexes: std::vec::Vec::new(),
            statistics: CollectionStatistics::default()
        };

        let last_logfile_index = collection.log_files.last().map_or(0, |(_, i, _)| *i);

        let len = {
            let arena = Bump::new();
            let items = collection.get_file(last_logfile_index, &arena).unwrap()?;
            items.len()
        };
        collection.last_file_entries = len;

        Ok(collection)
    }

    pub fn name(&self) -> &str {
        &self.config.collection_name
    }

    /// Sets the state of the object with the given id to the given state
    pub fn set_object(&mut self, object_id: Uuid, object_state: ObjectState) -> DBResult<usize> {
        self.set_objects([(object_id, object_state)])
    }

    /// Sets the state of the objects with the given ids to the given states
    pub fn set_objects<'a>(&mut self, objects: impl IntoIterator<Item = (Uuid, ObjectState<'a>)>) -> DBResult<usize> {
        let entries = objects
            .into_iter()
            .map(|(id, state)| LogEntry::new(id, state))
            .collect_vec();

        if let Some(value_entry) = self.get_any_entry_data()? {
            let error_entry = entries
                .iter()
                .find(|e| matches!(
                    e.entry_data().is_same_shape(&value_entry),
                    Break(false)
                ));
            if let Some(e) = error_entry {
                return Err(SchemaError::from_string(e.object_id().to_string()).into());
            }
        }

        let arena = Bump::new();

        let max_entries = self.config.storage_config.log_file.max_entries;
        let mut leftover = max_entries - self.last_file_entries;
        let mut appends = 0;
        for chunk in entries.into_iter()
            .batching(|iter| {
                let vec = iter.take(leftover).collect_in::<Vec<_>>(&arena);
                if vec.is_empty() {
                    None
                } else {
                    leftover = max_entries;
                    Some(vec)
                }
            }) {
            let last_id = self.last_file_id();
            let last_file_data = &self.log_files[last_id];

            last_file_data.2.borrow_mut().take();
            LogFile::append_entries(&self.config, last_id, chunk.iter())?;
            appends += chunk.len();
    
            self.last_file_entries += chunk.len();

            if self.last_file_entries >= self.config.storage_config.log_file.max_entries {
                *last_file_data.2.borrow_mut() = Some(LogFile::load_from_path(&last_file_data.0)?.into());
                self.log_files.push(LogFile::create_next_log_file(&mut self.config)?);
                self.last_file_entries = 0;
            }
        }

        self.statistics.approximate_total_entries += appends;

        Ok(appends)
    }

    pub fn get_file<'c, 'bump: 'c>(&'c self, index: usize, arena: &'bump Bump) -> Option<DBResult<LogFile<'bump>>> {
        let data = self.log_files.get(index)?;

        if data.2.borrow().is_none() {
            match LogFile::load_from_path(&data.0) {
                Ok(map) => {
                    data.2.replace(Some(map.into()));
                },
                Err(err) => return Some(Err(err)),
            };
        }

        Some(LogFile::deserialize(arena, data.2.borrow().as_ref().unwrap()))
    }

    fn get_any_entry_data(&self) -> DBResult<Option<ObjectState<'_>>> {
        match NativeCollectionIterator::new(self).next() {
            None => Ok(None),
            Some(Ok(e)) => Ok(Some(e)),
            Some(Err(err)) => Err(err)
        }
    }

    fn last_file_id(&self) -> usize {
        self.log_files.last().map_or(0, |(_, i, _)| *i)
    }

    pub fn iterate<'a, Item: Deserialize<'a>>(&'a self) -> CollectionIterator<'a, Item> {
        CollectionIterator::new(NativeCollectionIterator::new(self))
    }

    pub fn query<'a, Item: Deserialize<'a> + 'a>(&'a self) -> Query<'a, Item> {
        Query::<Item>::from_collection(self)
    }

    /// Flushes the in-memory cache
    pub fn flush_cache(&mut self) -> DBResult<()> {
        for mut f in self.log_files.iter_mut() {
            f.2 = None.into();
        }
        Ok(())
    }

    pub fn print_debug_info(&self) {
        println!("File cache memory usage: {}/{}", self.log_files.iter().filter(|f| f.2.borrow().is_some()).count(), self.log_files.len());
        println!("Log file directory: {:#?}", self.config.get_collection_files_destination());
        println!("Last log file ID: {}", self.last_file_id());
        println!("Entries in last file: {}", self.last_file_entries);
    }

}

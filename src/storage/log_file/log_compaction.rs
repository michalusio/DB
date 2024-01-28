use std::collections::HashMap;
use bumpalo::Bump;
use itertools::Itertools;
use uuid::Uuid;

use crate::{storage::collection::collection_config::CollectionConfig, utils::DBResult, errors::compaction_error::CompactionError};

use super::{LogFile, log_entry::LogEntry};

pub fn compact_files(older_index: usize, newer_index: usize, config: &CollectionConfig) -> DBResult<()> {
    let arena = Bump::new();
    let older_map = LogFile::load_from_path(&config.get_log_path(older_index))?;
    let newer_map = LogFile::load_from_path(&config.get_log_path(newer_index))?;

    let older = LogFile::deserialize(&arena, &older_map)?;
    let newer = LogFile::deserialize(&arena, &newer_map)?;
    
    let entries = compress_log_files(older, newer)?;
    save_compacted_entries(entries, older_index, newer_index, config)
}

fn save_compacted_entries(entries: HashMap<Uuid, LogEntry>, older_index: usize, newer_index: usize, config: &CollectionConfig) -> DBResult<()> {
    let max_entries = config.storage_config.log_file.max_entries;
    let chunks = entries.values().chunks(max_entries);
    let mut chunked = chunks.into_iter();

    let first = chunked.next().unwrap();
    LogFile::truncate_entries(config, older_index, first)?;
    if let Some(second) = chunked.next() {
        LogFile::truncate_entries(config, newer_index, second)?;
    }
    
    if chunked.next().is_some() {
        Err(CompactionError::from_str("More than two log files from compaction - this should not happen").into())
    } else {
        Ok(())
    }
}

fn compress_log_files<'a>(older_entries: LogFile<'a>, newer_entries: LogFile<'a>) -> DBResult<HashMap<Uuid, LogEntry<'a>>> {
    let mut entries: HashMap<Uuid, LogEntry> = HashMap::with_capacity((older_entries.len() + newer_entries.len()) / 2);
    for entry in newer_entries.0.into_iter().chain(older_entries.0.into_iter()) {
        let key = entry.object_id();
        if entries.contains_key(&key) {
            entries.insert(key, entry);
        }
    }
    Ok(entries)
}


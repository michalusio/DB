use std::{collections::{HashMap, HashSet}};
use itertools::Itertools;
use uuid::Uuid;

use crate::{collection::collection_config::CollectionConfig, errors::compaction_error::CompactionError, storage::log_file::log_entry::{EntityEntry, TransactionEntry}, utils::{DBResult}, ObjectField};

use super::{LogFile, log_entry::LogEntry};

pub fn compact_files(older_index: usize, newer_index: usize, config: &CollectionConfig) -> DBResult<()> {
    let older = LogFile::load_log_file(config, older_index)?;
    let newer = LogFile::load_log_file(config, newer_index)?;

    let entries = compress_log_files(&older, &newer)?;
    save_compacted_entries(entries, older, newer, config)
}

fn save_compacted_entries(entries: HashMap<Uuid, Option<Vec<ObjectField>>>, older: LogFile, newer: LogFile, config: &CollectionConfig) -> DBResult<()> {
    let max_entries = config.storage_config.log_file.max_entries;
    let chunks = entries.into_iter().chunks(max_entries);

    for (index, chunk) in chunks.into_iter().enumerate() {
        if index >= 2 {
            return Err(CompactionError::from_str("More than two log files from compaction - this should not happen").into())
        }
        let file = if index == 0 { &older } else { &newer };
        LogFile::truncate_entries(file, config, chunk.filter_map(|(id, values)| {
            values.map(|v| LogEntry::update(Uuid::nil(), id, v))
        }))?;
    }

    Ok(())
}

fn compress_log_files(older_entries: &LogFile, newer_entries: &LogFile) -> DBResult<HashMap<Uuid, Option<Vec<ObjectField>>>> {
    let older_entries = older_entries.read()?;
    let newer_entries = newer_entries.read()?;
    let mut entries: HashMap<Uuid, Option<Vec<ObjectField>>> = HashMap::with_capacity((older_entries.len() + newer_entries.len()) / 2);
    let mut transactions = HashSet::<Uuid>::new();
    for entry in newer_entries.iter().chain(older_entries.iter()) {
        match entry {
            LogEntry::Entity(transaction_id, EntityEntry::Updated(entry_id,  values)) => {
                if transactions.contains(transaction_id) {
                    entries.entry(*entry_id).or_insert_with(|| Some(values.clone()));
                }
            },
            LogEntry::Entity(transaction_id, EntityEntry::Deleted(entry_id)) => {
                if transactions.contains(transaction_id) {
                    entries.entry(*entry_id).or_insert_with(|| None);
                }
            },
            LogEntry::Transaction(transaction_id, TransactionEntry::Committed) => {
                transactions.insert(*transaction_id);
            },
            LogEntry::Transaction(_, TransactionEntry::Rollbacked) => { },
        }
        
    }
    Ok(entries)
}


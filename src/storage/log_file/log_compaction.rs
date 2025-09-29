use std::{collections::{HashMap, HashSet}};
use itertools::Itertools;
use uuid::Uuid;

use crate::{collection::collection_config::CollectionConfig, errors::compaction_error::CompactionError, storage::log_file::{entry_fields::EntryFields, log_entry::{EntityEntry, Row, TransactionEntry}}, utils::DBResult};

use super::{LogFile, log_entry::LogEntry};

pub fn compact_files(older_index: usize, newer_index: usize, config: &CollectionConfig) -> DBResult<()> {
    let older = LogFile::load_log_file(config, older_index)?;
    let newer = LogFile::load_log_file(config, newer_index)?;

    let (entries, transaction_id) = compress_log_files(&older, &newer)?;
    save_compacted_entries(entries, older, newer, config, transaction_id)
}

fn save_compacted_entries(entries: HashMap<Uuid, Option<EntryFields>>, older: LogFile, newer: LogFile, config: &CollectionConfig, transaction_id: Uuid) -> DBResult<()> {
    let max_entries = config.storage_config.log_file.max_entries;
    let chunks = entries.into_iter().chunks(max_entries);

    for (index, chunk) in chunks.into_iter().enumerate() {
        if index >= 2 {
            return Err(CompactionError::from_str("More than two log files from compaction - this should not happen").into())
        }
        let file = if index == 0 { &older } else { &newer };
        LogFile::truncate_entries(file, config, chunk.filter_map(|(id, values)| {
            values.map(|fields| LogEntry::update(transaction_id, id, fields))
        }))?;
    }

    Ok(())
}

fn compress_log_files(older_entries: &LogFile, newer_entries: &LogFile) -> DBResult<(HashMap<Uuid, Option<EntryFields>>, Uuid)> {
    let older_entries = older_entries.read()?;
    let newer_entries = newer_entries.read()?;
    let mut entries: HashMap<Uuid, Option<EntryFields>> = HashMap::with_capacity((older_entries.len() + newer_entries.len()) / 2);
    let mut transactions = HashSet::<Uuid>::new();
    let mut newest_transaction_id = Uuid::nil();
    for entry in newer_entries.iter().chain(older_entries.iter()) {
        match entry {
            LogEntry::Entity(transaction_id, EntityEntry::Updated(row)) => {
                if transactions.contains(transaction_id) {
                    entries.entry(row.id).or_insert_with(|| Some(row.fields.clone()));
                }
            },
            LogEntry::Entity(transaction_id, EntityEntry::Deleted(entry_id)) => {
                if transactions.contains(transaction_id) {
                    entries.entry(*entry_id).or_insert_with(|| None);
                }
            },
            LogEntry::Transaction(transaction_id, TransactionEntry::Committed) => {
                let id = *transaction_id;
                transactions.insert(id);
                if newest_transaction_id < id {
                    newest_transaction_id = id;
                }
            },
            LogEntry::Transaction(_, TransactionEntry::Rollbacked) => { },
        }
        
    }
    Ok((entries, newest_transaction_id))
}


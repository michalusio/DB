use std::sync::{Arc};

use gxhash::{HashSet, HashSetExt};
use log_err::LogErrResult;
use uuid::Uuid;
use yoke::{Yoke};

use crate::collection::Collection;
use crate::errors::storage_error::StorageError;
use crate::errors::DatabaseError;
use crate::storage::log_file::log_entry::{EntityEntry, LogEntry, TransactionEntry};
use crate::storage::log_file::LogFile;
use crate::utils::{RwLockReadGuardian};
use crate::{DBOperator, DBResult, Row};

pub struct TableScan<'a> {
    collection: &'a Collection,
    current_file_index: usize,
    current_file_ref: Option<Yoke<RwLockReadGuardian<'static, Vec<LogEntry>>, Arc<LogFile>>>,
    current_file_entry: usize,
    visited_ids: HashSet<Uuid>,
    committed_transactions: HashSet<Uuid>,
    current_transaction_id: Uuid
}

impl<'a> Clone for TableScan<'a> {
    fn clone(&self) -> Self {
        Self {
            collection: self.collection,
            current_file_index: self.current_file_index,
            current_file_ref: self.current_file_ref
                .as_ref()
                .map(|yoke| {
                    Yoke::attach_to_cart(yoke.backing_cart().clone(), |file| {
                        RwLockReadGuardian(file.read().log_unwrap())
                    })
                }),
            current_file_entry: self.current_file_entry,
            visited_ids: self.visited_ids.clone(),
            committed_transactions: self.committed_transactions.clone(),
            current_transaction_id: self.current_transaction_id
        }
    }
}

impl<'a> TableScan<'a> {

    pub fn new(collection: &'a Collection, transaction_id: Uuid) -> Self {
        let approx_entries = collection.statistics.approximate_entries();
        let mut transactions = HashSet::new();
        transactions.insert(Uuid::nil());
        Self {
            collection,
            current_file_index: collection.last_file_index,
            current_file_ref: None,
            current_file_entry: 0,
            visited_ids: HashSet::with_capacity(approx_entries),
            committed_transactions: transactions,
            current_transaction_id: transaction_id
        }
    }
}

impl<'a> DBOperator for TableScan<'a> {
    fn next(&mut self) -> DBResult<Option<Row>> {
        loop {
            if let Some(file) = self.current_file_ref.as_mut() {
                self.current_file_entry += 1;
                let yoke = file.get();
                if let Some(entry) = yoke.get(yoke.len().wrapping_sub(self.current_file_entry)) {
                    match entry {
                        LogEntry::Entity(transaction_id, EntityEntry::Updated(row)) => {
                            if transaction_id <= &self.current_transaction_id && self.committed_transactions.contains(transaction_id)
                                && self.visited_ids.insert(row.id) {
                                    let entry_id = row.id;
                                    let fields = row.fields.clone();
                                    return Ok(Some(Row { id: entry_id, fields }));
                                }
                        },
                        LogEntry::Entity(transaction_id, EntityEntry::Deleted(entry_id)) => {
                            if transaction_id <= &self.current_transaction_id && self.committed_transactions.contains(transaction_id) {
                                self.visited_ids.insert(*entry_id);
                            }
                        },
                        LogEntry::Transaction(transaction_id, TransactionEntry::Committed) => {
                            if transaction_id <= &self.current_transaction_id {
                                self.committed_transactions.insert(*transaction_id);
                            }
                        },
                        LogEntry::Transaction(_, TransactionEntry::Rollbacked) => {

                        }
                    }
                } else {
                    self.current_file_ref = None;
                    if self.current_file_index == 0 {
                        return Ok(None);
                    }
                    self.current_file_entry = 0;
                    self.current_file_index -= 1;
                }
            } else {
                match self.collection.get_file(self.current_file_index) {
                    Ok(Some(file)) => {
                        let yoke = Yoke::try_attach_to_cart(file, |file| {
                            match file.read() {
                                Ok(lock) => Ok(RwLockReadGuardian(lock)),
                                Err(_) => Err(DatabaseError::Storage(StorageError::Inconsistency()))
                            }
                        });
                        match yoke {
                            Ok(yoke) => {
                                self.current_file_ref = Some(yoke);
                            },
                            Err(e) => return Err(e)
                        }
                    },
                    Ok(None) => return Ok(None),
                    Err(e) => return Err(e)
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let files_left = self.current_file_index + 1;
        (
            files_left,
            Some((files_left * self.collection.config.storage_config.log_file.max_entries).saturating_sub(self.current_file_entry))
        )
    }
}

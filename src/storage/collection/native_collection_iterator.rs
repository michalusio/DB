use std::collections::HashSet;
use std::iter::FusedIterator;
use std::ops::Deref;
use std::sync::{Arc, RwLockReadGuard};

use uuid::Uuid;
use yoke::{Yoke, Yokeable};

use super::Collection;
use crate::errors::storage_error::StorageError;
use crate::errors::DatabaseError;
use crate::storage::log_file::log_entry::{EntityEntry, LogEntry, TransactionEntry};
use crate::storage::log_file::LogFile;
use crate::utils::DBResult;
use crate::ObjectField;

#[derive(Yokeable)]
struct RwLockReadGuardian<'a>(RwLockReadGuard<'a, Vec<LogEntry>>);
impl<'a> Deref for RwLockReadGuardian<'a> {
    type Target = RwLockReadGuard<'a, Vec<LogEntry>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct NativeCollectionIterator<'a> {
    collection: &'a Collection,
    current_file_index: usize,
    current_file_ref: Option<Yoke<RwLockReadGuardian<'static>, Arc<LogFile>>>,
    current_file_entry: usize,
    visited_ids: HashSet<Uuid>,
    committed_transactions: HashSet<Uuid>
}

impl<'a> NativeCollectionIterator<'a> {

    pub fn new(collection: &'a Collection) -> Self {
        let approx_entries = collection.statistics.approximate_entries();
        let mut transactions = HashSet::new();
        transactions.insert(Uuid::nil());
        Self {
            collection,
            current_file_index: collection.last_file_index,
            current_file_ref: None,
            current_file_entry: 0,
            visited_ids: HashSet::with_capacity(approx_entries),
            committed_transactions: transactions
        }
    }
}

impl<'a> Iterator for NativeCollectionIterator<'a> {
    type Item = DBResult<(Uuid, Arc<[ObjectField]>)>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(file) = self.current_file_ref.take() {
                if let Some(entry) = file.get().get(self.current_file_entry) {
                    self.current_file_entry += 1;
                    match entry {
                        LogEntry::Entity(transaction_id, EntityEntry::Updated(entry_id, values)) => {
                            if self.committed_transactions.contains(transaction_id) {
                                if self.visited_ids.insert(*entry_id) {
                                    let entry_id = *entry_id;
                                    let values = values.clone();
                                    self.current_file_ref = Some(file);
                                    return Some(Ok((entry_id, values)));
                                }
                            }
                        },
                        LogEntry::Entity(transaction_id, EntityEntry::Deleted(entry_id)) => {
                            if self.committed_transactions.contains(transaction_id) {
                                self.visited_ids.insert(*entry_id);
                            }
                        },
                        LogEntry::Transaction(transaction_id, TransactionEntry::Committed) => {
                            self.committed_transactions.insert(*transaction_id);
                        },
                        LogEntry::Transaction(_, TransactionEntry::Rollbacked) => {

                        }
                    }
                    self.current_file_ref = Some(file);
                    continue;
                }
                drop(file);
                self.current_file_ref = None;
                if self.current_file_index == 0 {
                    return None;
                }
                self.current_file_entry = 0;
                self.current_file_index -= 1;
            } else {
                match self.collection
                    .get_file(self.current_file_index) {
                    Ok(Some(file)) => {
                        let yoke = Yoke::try_attach_to_cart(file, |file| {
                            match file.read() {
                                Ok(lock) => Ok(RwLockReadGuardian(lock)),
                                Err(e) => Err(DatabaseError::Storage(StorageError::Inconsistency()))
                            }
                        });
                        match yoke {
                            Ok(yoke) => {
                                self.current_file_ref = Some(yoke);
                            },
                            Err(e) => {
                                return Some(Err(e));
                            }
                        }
                    },
                    Ok(None) => {
                        return None;
                    },
                    Err(e) => {
                        return Some(Err(e));
                    }
                }
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let approx_entries = self.collection.statistics.approximate_entries();

        let files_left = self.collection.last_file_index + 1 - self.current_file_index;
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
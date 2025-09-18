use std::{array::TryFromSliceError, ops::ControlFlow};
use uuid::Uuid;
use crate::{errors::storage_error::CompressionError, ObjectField};

#[derive(Clone)]
pub enum LogEntry {
    Entity(Uuid, EntityEntry),
    Transaction(Uuid, TransactionEntry)
}

#[derive(Clone)]
pub enum EntityEntry {
    Updated(Uuid, Vec<ObjectField>),
    Deleted(Uuid)
}

#[derive(Clone)]
pub enum TransactionEntry {
    Committed,
    Rollbacked
}

impl LogEntry {
    pub fn update(transaction_id: Uuid, entity_id: Uuid, fields: Vec<ObjectField>) -> Self {
        LogEntry::Entity(transaction_id, EntityEntry::Updated(entity_id, fields))
    }

    pub fn delete(transaction_id: Uuid, entity_id: Uuid) -> Self {
        LogEntry::Entity(transaction_id, EntityEntry::Deleted(entity_id))
    }

    pub fn commit(transaction_id: Uuid) -> Self {
        LogEntry::Transaction(transaction_id, TransactionEntry::Committed)
    }

    pub fn rollback(transaction_id: Uuid) -> Self {
        LogEntry::Transaction(transaction_id, TransactionEntry::Rollbacked)
    }

    pub fn transaction_id(&self) -> Uuid {
        match self {
            LogEntry::Entity(id, _) => *id,
            LogEntry::Transaction(id, _) => *id
        }
    }

    pub fn compress_to(&self, store: &mut Vec<u8>) {
        match self {
            LogEntry::Entity(transaction_id, entity) => {
                store.extend(transaction_id.as_bytes());
                match entity {
                    EntityEntry::Deleted(id) => {
                        store.push(0);
                        store.extend(id.as_bytes());
                    },
                    EntityEntry::Updated(id, values) => {
                        store.push(values.len() as u8);
                        store.extend_from_slice(id.as_bytes());
                        for value in values.iter() {
                            value.compress_to(store);
                        }
                    }
                }
            },
            LogEntry::Transaction(transaction_id, transaction) => {
                store.extend(transaction_id.as_bytes());
                match transaction {
                    TransactionEntry::Committed => {
                        store.push(254);
                    },
                    TransactionEntry::Rollbacked => {
                        store.push(255);
                    }
                }
            }
        }
    }

    pub fn decompress(data: &[u8]) -> Result<LogEntry, CompressionError> {
        let transaction_id = decompress_uuid(data)?;
        let mut data = &data[16..];
        let kind = data[0];
        data = &data[1..];
        match kind {
            0 => {
                let id = decompress_uuid(data)?;
                Ok(LogEntry::Entity(transaction_id, EntityEntry::Deleted(id)))
            },
            254 => {
                Ok(LogEntry::Transaction(transaction_id, TransactionEntry::Committed))
            },
            255 => {
                Ok(LogEntry::Transaction(transaction_id, TransactionEntry::Rollbacked))
            },
            n => {
                let n = n as usize;
                let id = decompress_uuid(data)?;
                data = &data[16..];

                let mut fields: Vec<ObjectField> = Vec::with_capacity(n);
                
                for _ in 0..n {
                    let (field, offset) = ObjectField::decompress(data)?;
                    fields.push(field);
                    data = &data[offset+1..];
                }

                
                Ok(LogEntry::Entity(transaction_id, EntityEntry::Updated(id, fields)))
            }
        }
        
    }

    pub fn byte_size(&self) -> u64 {
        (std::mem::size_of::<Self>() as u64) + match self {
            LogEntry::Entity(_, entry) => entry.byte_size(),
            LogEntry::Transaction(_, _) => 0
        }
    }
}

impl EntityEntry {

    pub fn object_id(&self) -> Uuid {
        match self {
            EntityEntry::Updated(id, _) => *id,
            EntityEntry::Deleted(id) => *id
        }
    }

    /// Checks if the object has the same shape as the other one.
    /// <ul>
    /// <li>If the checked object is a tombstone, it automatically has a correct shape to every object.</li>
    /// <li>If the other shape is a tombstone, the function requests checking on another object.</li>
    /// <li>If both objects have values, all the values between them have to have the same kind.</li>
    /// </ul>
    pub fn is_same_shape(&self, other: &Self) -> ControlFlow<bool> {
        match (self, other) {
            (EntityEntry::Deleted(_), _) => ControlFlow::Break(true),
            (_, EntityEntry::Deleted(_)) => ControlFlow::Continue(()),
            (
                EntityEntry::Updated(_, self_values),
                EntityEntry::Updated(_, other_values)
            ) => {
                let all_values_are_same_type = self_values
                    .iter()
                    .map(ObjectField::value_id)
                    .eq(other_values.iter().map(ObjectField::value_id));
                ControlFlow::Break(all_values_are_same_type)
            },
        }
    }

    pub fn byte_size(&self) -> u64 {
        match self {
            EntityEntry::Updated(_, fields) => fields.iter().map(|f| f.byte_size()).sum(),
            EntityEntry::Deleted(_) => 0
        }
    }
}

fn decompress_uuid(data: &[u8]) -> Result<Uuid, CompressionError> {
    let fixed_uuid_slice: Result<[u8; 16], Box<TryFromSliceError>> = data[0..16].try_into().map_err(Box::new);
    let fixed_uuid_slice = fixed_uuid_slice.map_err(|e| CompressionError(e));
    Ok(Uuid::from_bytes(fixed_uuid_slice?))
}
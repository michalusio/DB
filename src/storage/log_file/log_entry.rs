use std::{ops::{ControlFlow, Range}, rc::Rc};
use log_err::LogErrResult;
use uuid::Uuid;
use crate::{errors::storage_error::CompressionError, storage::log_file::entry_fields::EntryFields};

#[derive(Clone)]
pub enum LogEntry {
    Entity(Uuid, EntityEntry),
    Transaction(Uuid, TransactionEntry)
}

#[derive(Clone)]
pub enum EntityEntry {
    Updated(Row),
    Deleted(Uuid)
}

#[derive(Clone)]
pub struct Row {
    pub id: Uuid,
    pub fields: EntryFields
}

#[derive(Clone)]
pub enum TransactionEntry {
    Committed,
    Rollbacked
}

impl LogEntry {
    pub fn update(transaction_id: Uuid, entity_id: Uuid, fields: EntryFields) -> Self {
        LogEntry::Entity(transaction_id, EntityEntry::Updated(Row { id: entity_id, fields }))
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
                    EntityEntry::Updated(row) => {
                        store.push(1);
                        store.extend_from_slice(row.id.as_bytes());
                        let range = &row.fields.0;
                        store.extend_from_slice(&row.fields.1[range.start..range.end]);
                    }
                }
            },
            LogEntry::Transaction(transaction_id, transaction) => {
                store.extend(transaction_id.as_bytes());
                match transaction {
                    TransactionEntry::Committed => {
                        store.push(2);
                    },
                    TransactionEntry::Rollbacked => {
                        store.push(3);
                    }
                }
            }
        }
    }

    pub fn decompress(rc: Rc<[u8]>, range: Range<usize>) -> Result<LogEntry, CompressionError> {
        let data = &rc[range.start..];
        let transaction_id = Uuid::from_bytes(data[0..16].try_into().log_unwrap());
        let kind = data[16];
        match kind {
            0 => {
                let id = Uuid::from_bytes(data[17..][0..16].try_into().log_unwrap());
                Ok(LogEntry::Entity(transaction_id, EntityEntry::Deleted(id)))
            },
            2 => {
                Ok(LogEntry::Transaction(transaction_id, TransactionEntry::Committed))
            },
            3 => {
                Ok(LogEntry::Transaction(transaction_id, TransactionEntry::Rollbacked))
            },
            1 => {
                let id = Uuid::from_bytes(data[17..][0..16].try_into().log_unwrap());

                Ok(LogEntry::Entity(transaction_id, EntityEntry::Updated(Row { id, fields: EntryFields(Range { start: range.start + 33, end: range.end }, rc) })))
            },
            _ => unimplemented!()
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
            EntityEntry::Updated(row) => row.id,
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
                EntityEntry::Updated(self_row),
                EntityEntry::Updated(other_row)
            ) => {
                let self_values = &self_row.fields;
                let other_values = &other_row.fields;
                let self_types = self_values.field_types();
                let other_types = other_values.field_types();
                ControlFlow::Break(self_types.eq(other_types))
            },
        }
    }

    pub fn byte_size(&self) -> u64 {
        match self {
            EntityEntry::Updated(row) => row.fields.byte_size(),
            EntityEntry::Deleted(_) => 0
        }
    }
}

impl Row {
    /// Combines two rows into one, containing all the columns - first the first entries, then the second one's.
    pub(crate) fn combine(a: &Row, b: &Row) -> Row {
        Row {
            id: a.id,
            fields: EntryFields::combine(&a.fields, &b.fields)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, rc::Rc};

    use serial_test::parallel;
    use uuid::Uuid;

    use crate::{objects::FieldType, storage::log_file::log_entry::{EntityEntry, LogEntry}, ObjectField};

    #[test]
    #[parallel]
    fn test_deserialization_serialization() {
        let mut data: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // Transaction UUID
            1, // Kind - Update
            2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, // Entry UUID
            4, // Fields count
            FieldType::I64 as u8,
            FieldType::I32 as u8,
            FieldType::String as u8,
            FieldType::Bool as u8,
            12, 0, 0, 0, 0, 0, 0, 0, // I64 8 bytes,
            9, 0, 0, 0, // I32 4 bytes
            13, // String 13 bytes
        ];
        data.extend("Hello, World!".as_bytes());
        data.extend([1]); // Bool 1 byte

        let data: Rc<[u8]> = data.into_boxed_slice().into();

        // testing deserialization
        let entry = LogEntry::decompress(data.clone(), Range { start: 0, end: data.len()}).unwrap();

        assert_eq!(entry.byte_size(), 136);
        match entry {
            LogEntry::Entity(transaction_id, EntityEntry::Updated(ref row)) => {
                assert_eq!(transaction_id, Uuid::nil());
                assert_eq!(row.id, Uuid::parse_str("02030203020302030203020302030203").unwrap());
                assert_eq!(row.fields.len(), 4);
                
                let checks = vec![
                    row.fields.get_field(0),
                    row.fields.get_field(1),
                    row.fields.get_field(2),
                    row.fields.get_field(3)
                ];
                let wanted: Vec<ObjectField> = vec![
                    (12i64).into(),
                    9.into(),
                    "Hello, World!".into(),
                    true.into()
                ];
                assert_eq!(checks, wanted);
            },
            _ => panic!("entry should be an entity update!")
        }

        let mut output = vec![];
        entry.compress_to(&mut output);

        assert_eq!(*output, *data);
    }
}

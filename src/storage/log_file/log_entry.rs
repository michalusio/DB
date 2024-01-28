use std::array::TryFromSliceError;

use bumpalo::Bump;
use uuid::Uuid;

use crate::{objects::ObjectState, errors::storage_error::CompressionError};

#[derive(Clone)]
pub struct LogEntry<'a> {
    object_id: Uuid,
    entry_data: ObjectState<'a>
}

impl<'a> LogEntry<'a> {
    #[inline]
    pub fn new(object_id: Uuid, object_state: ObjectState<'a>) -> Self {
        LogEntry { object_id, entry_data: object_state }
    }

    #[inline]
    pub fn object_id(&self) -> Uuid {
        self.object_id
    }

    #[inline]
    pub fn entry_data(&self) -> ObjectState {
        self.entry_data.clone()
    }


    #[inline]
    pub fn compress(&self, arena: &Bump) -> Vec<u8> {
        self.object_id.as_bytes()
            .iter()
            .chain(self.entry_data.compress(arena).iter())
            .copied()
            .collect()
    }

    #[inline]
    pub fn decompress(data: & [u8], arena: &'a Bump) -> Result<LogEntry<'a>, CompressionError> {
        let uuid = Self::decompress_uuid(data)?;
        let object_state = ObjectState::decompress(&data[16..], arena)?;
        Ok(LogEntry {
            object_id: uuid,
            entry_data: object_state
        })
    }

    #[inline]
    fn decompress_uuid(data: &[u8]) -> Result<Uuid, CompressionError> {
        let fixed_uuid_slice: Result<[u8; 16], Box<TryFromSliceError>> = data[0..16].try_into().map_err(Box::new);
        let fixed_uuid_slice = fixed_uuid_slice.map_err(|e| CompressionError(e));
        Ok(Uuid::from_bytes(fixed_uuid_slice?))
    }
}
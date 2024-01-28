use std::ops::ControlFlow;
use bumpalo::{vec, collections::Vec, Bump};

use crate::errors::storage_error::CompressionError;

use super::object_field::ObjectField;

#[derive(Clone, Debug)]
pub enum ObjectState<'bump> {
    /// The object was deleted
    Tombstone,
    /// The object exists - the enum contains the state of the object
    ObjectValues(Vec<'bump, ObjectField>)
}

impl<'bump> ObjectState<'bump> {

    #[inline]
    pub fn as_values(&self) -> Option<&'bump Vec<ObjectField>> {
        match self {
            ObjectState::Tombstone => None,
            ObjectState::ObjectValues(v) => Some(v),
        }
    }

    #[inline]
    pub fn compress(&self, arena: &'bump Bump) -> Vec<u8> {
        match self {
            ObjectState::Tombstone => vec![in arena; 0],
            ObjectState::ObjectValues(values) => {
                let mut bytes = Vec::with_capacity_in(1 + values.len() * 5, arena);
                bytes.push(values.len() as u8);
                for value in values.iter() {
                    value.compress_to(&mut bytes);
                }
                bytes
            }
        }
    }

    #[inline]
    pub fn decompress(mut data: &[u8], arena: &'bump Bump) -> Result<ObjectState<'bump>, CompressionError> {
        let len = data[0];
        data = &data[1..];
        match len {
            0 => Ok(ObjectState::Tombstone),
            n => {
                let mut fields = Vec::with_capacity_in(n.into(), arena);
                for _ in 0..n {
                    if data.len() < 2 {
                        return Err(CompressionError::from_str("Couldn't decompress data - stream is empty"));
                    }
                    let (field, offset) = ObjectField::decompress(data)?;
                    fields.push(field);
                    data = &data[offset+1..];
                }
                Ok(ObjectState::ObjectValues(fields))
            }
        }
    }

    /// Checks if the object has the same shape as the other one.
    /// <ul>
    /// <li>If the checked object is a tombstone, it automatically has a correct shape to every object.</li>
    /// <li>If the other shape is a tombstone, the function requests checking on another object.</li>
    /// <li>If both objects have values, all the values between them have to have the same kind.</li>
    /// </ul>
    #[inline]
    pub fn is_same_shape(&self, other: &Self) -> ControlFlow<bool> {
        match (self, other) {
            (ObjectState::Tombstone, _) => ControlFlow::Break(true),
            (_, ObjectState::Tombstone) => ControlFlow::Continue(()),
            (
                ObjectState::ObjectValues(self_values),
                ObjectState::ObjectValues(other_values)
            ) => {
                let all_values_are_same_type = self_values
                    .iter()
                    .map(ObjectField::value_id)
                    .eq(other_values.iter().map(ObjectField::value_id));
                ControlFlow::Break(all_values_are_same_type)
            },
        }
    }
}
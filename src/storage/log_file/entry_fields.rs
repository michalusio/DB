use std::{borrow::Cow, fmt::Display, ops::{Deref, Range}, rc::Rc};

use log_err::LogErrResult;
use uuid::Uuid;
use yoke::{Yoke, Yokeable};

use crate::{objects::FieldType, ObjectField, SelectField};

#[derive(Yokeable)]
#[repr(transparent)]
pub(crate) struct DeserializedFields<'a>(Vec<ObjectField<'a>>);

impl<'a> Deref for DeserializedFields<'a> {
    type Target = Vec<ObjectField<'a>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct EntryFields(pub(crate) Range<usize>, pub(crate) Rc<[u8]>);

impl From<Vec<ObjectField<'_>>> for EntryFields {
    fn from(fields: Vec<ObjectField<'_>>) -> Self {
        fields.as_slice().into()
    }
}

impl From<Vec<SelectField<'_>>> for EntryFields {
    fn from(fields: Vec<SelectField<'_>>) -> Self {
        let mut data = Vec::with_capacity(1 + fields.iter().map(|f| 1 + match &**f {
            ObjectField::Bool(_) => 1,
            ObjectField::I32(_) => 4,
            ObjectField::I64(_) => 8,
            ObjectField::Decimal(_) => 8,
            ObjectField::Id(_) => 16,
            ObjectField::Bytes(b) => 1 + b.len(),
            ObjectField::String(b) => 1 + b.len()
        }).sum::<usize>());
        data.push(fields.len() as u8);
        
        data.extend(fields.iter().map(|f| match &**f {
            ObjectField::Bool(_) => FieldType::Bool,
            ObjectField::I32(_) => FieldType::I32,
            ObjectField::I64(_) => FieldType::I64,
            ObjectField::Decimal(_) => FieldType::Decimal,
            ObjectField::Id(_) => FieldType::Id,
            ObjectField::Bytes(_) => FieldType::Bytes,
            ObjectField::String(_) => FieldType::String,
        } as u8));
        
        for field in fields {
            match &*field {
                ObjectField::Bool(b) => data.push(if *b { 1 } else { 0 }),
                ObjectField::I32(i) => data.extend(i.to_le_bytes()),
                ObjectField::I64(i) => data.extend(i.to_le_bytes()),
                ObjectField::Decimal(f) => data.extend(f.to_le_bytes()),
                ObjectField::Id(id) => data.extend(id.as_bytes()),
                ObjectField::Bytes(bytes) => {
                    data.push(bytes.len() as u8);
                    data.extend(bytes.iter());
                },
                ObjectField::String(str) => {
                    data.push(str.len() as u8);
                    data.extend(str.as_bytes())
                },
            };
        }
        let len = data.len();
        EntryFields(Range { start: 0, end: len }, data.into_boxed_slice().into())
    }
}

impl From<&[ObjectField<'_>]> for EntryFields {
    fn from(fields: &[ObjectField<'_>]) -> Self {
        let mut data = Vec::with_capacity(1 + fields.iter().map(|f| 1 + match f {
            ObjectField::Bool(_) => 1,
            ObjectField::I32(_) => 4,
            ObjectField::I64(_) => 8,
            ObjectField::Decimal(_) => 8,
            ObjectField::Id(_) => 16,
            ObjectField::Bytes(b) => 1 + b.len(),
            ObjectField::String(b) => 1 + b.len()
        }).sum::<usize>());
        data.push(fields.len() as u8);
        
        data.extend(fields.iter().map(|f| match f {
            ObjectField::Bool(_) => FieldType::Bool,
            ObjectField::I32(_) => FieldType::I32,
            ObjectField::I64(_) => FieldType::I64,
            ObjectField::Decimal(_) => FieldType::Decimal,
            ObjectField::Id(_) => FieldType::Id,
            ObjectField::Bytes(_) => FieldType::Bytes,
            ObjectField::String(_) => FieldType::String,
        } as u8));
        
        for field in fields {
            match field {
                ObjectField::Bool(b) => data.push(if *b { 1 } else { 0 }),
                ObjectField::I32(i) => data.extend(i.to_le_bytes()),
                ObjectField::I64(i) => data.extend(i.to_le_bytes()),
                ObjectField::Decimal(f) => data.extend(f.to_le_bytes()),
                ObjectField::Id(id) => data.extend(id.as_bytes()),
                ObjectField::Bytes(bytes) => {
                    data.push(bytes.len() as u8);
                    data.extend(bytes.iter());
                },
                ObjectField::String(str) => {
                    data.push(str.len() as u8);
                    data.extend(str.as_bytes())
                },
            };
        }
        let len = data.len();
        EntryFields(Range { start: 0, end: len }, data.into_boxed_slice().into())
    }
}

impl EntryFields {
    pub fn len(&self) -> usize {
        self.1[self.0.start].into()
    }

    pub fn is_empty(&self) -> bool {
        self.1[self.0.start] == 0
    }

    pub fn column_types(&self) -> &[FieldType] {
        unsafe { std::mem::transmute::<&[u8], &[FieldType]>(&self.1[self.0.start + 1..][..self.len()]) }
    }

    pub fn column(&'_ self, index: usize) -> ObjectField<'_> {
        let (field_type, bytes) = self.get_column_data(index);
        match field_type {
            FieldType::Bool => ObjectField::Bool(bytes[0] > 0),
            FieldType::I32 => ObjectField::I32(i32::from_le_bytes(bytes.try_into().log_unwrap())),
            FieldType::I64 => ObjectField::I64(i64::from_le_bytes(bytes.try_into().log_unwrap())),
            FieldType::Decimal => ObjectField::Decimal(f64::from_le_bytes(bytes.try_into().log_unwrap())),
            FieldType::Id => ObjectField::Id(Uuid::from_bytes_le(bytes.try_into().log_unwrap())),
            FieldType::String => ObjectField::String(Cow::Borrowed(unsafe { str::from_utf8_unchecked(bytes) })),
            FieldType::Bytes => ObjectField::Bytes(Cow::Borrowed(bytes))
        }
    }

    pub(crate) fn column_bytes(&self) -> &[u8] {
        &self.1[self.0.start + self.len() + 1..self.0.end]
    }

    pub(crate) fn into_yoke_vector(self) -> Yoke<DeserializedFields<'static>, Box<EntryFields>> {
        let that = Box::new(self);
        Yoke::attach_to_cart(that, |data| {
            let count = data.len();
            let types = data.column_types();
            let data = data.column_bytes();

            let mut result = Vec::with_capacity(count);
            let mut current_index: usize = 0;
            let mut current_pointer = 0;
            while current_index != count {
                let field_type = &types[current_index];
                let d = &data[current_pointer..];

                result.push(match field_type {
                    FieldType::Bool => ObjectField::Bool(d[0] > 0),
                    FieldType::I32 => ObjectField::I32(i32::from_le_bytes(d[..4].try_into().log_unwrap())),
                    FieldType::I64 => ObjectField::I64(i64::from_le_bytes(d[..8].try_into().log_unwrap())),
                    FieldType::Decimal => ObjectField::Decimal(f64::from_le_bytes(d[..8].try_into().log_unwrap())),
                    FieldType::Id => ObjectField::Id(Uuid::from_bytes_le(d[..16].try_into().log_unwrap())),
                    FieldType::String => {
                        let str = unsafe { str::from_utf8_unchecked(&d[1..][..d[0] as usize]) };
                        ObjectField::String(Cow::Borrowed(str))
                    },
                    FieldType::Bytes => {
                        let bytes = &d[1..][..d[0] as usize];
                        ObjectField::Bytes(Cow::Borrowed(bytes))
                    }
                });

                current_pointer += match field_type {
                    FieldType::Bool => 1,
                    FieldType::I32 => 4,
                    FieldType::I64 => 8,
                    FieldType::Decimal => 8,
                    FieldType::Id => 16,
                    FieldType::String | FieldType::Bytes => 1 + d[0] as usize,
                };
                current_index += 1;
            }
            DeserializedFields(result)
        })
    }

    pub(crate) fn get_column_data(&'_ self, index: usize) -> (FieldType, &'_ [u8]) {
        assert!(index < self.len(), "Accessed field outside of the entry");
        let types = self.column_types();
        let data = self.column_bytes();

        let mut current_index = 0;
        let mut current_pointer = 0;
        while current_index != index {
            let field_type = &types[current_index];
            current_pointer += match field_type {
                FieldType::Bool => 1,
                FieldType::I32 => 4,
                FieldType::I64 => 8,
                FieldType::Decimal => 8,
                FieldType::Id => 16,
                FieldType::String | FieldType::Bytes => 1 + data[current_pointer] as usize,
            };
            current_index += 1;
        }
        let field_type = &types[current_index];
        let d = &data[current_pointer..];
        let bytes = match field_type {
            FieldType::Bool => &d[..1],
            FieldType::I32 => &d[..4],
            FieldType::I64 => &d[..8],
            FieldType::Decimal => &d[..8],
            FieldType::Id => &d[..16],
            FieldType::String => &d[1..][..d[0] as usize],
            FieldType::Bytes => &d[1..][..d[0] as usize]
        };
        (*field_type, bytes)
    }

    /// Combines two entry fields into one, containing all the columns - first the first entries, then the second one's.
    pub(crate) fn combine(a: &EntryFields, b: &EntryFields) -> EntryFields {
        let mut fields_data = vec![0u8; a.0.len() + b.0.len() - 1].into_boxed_slice();
        let a_field_count = a.len();
        let b_field_count = b.len();

        let a_data = a.column_bytes();
        let b_data = b.column_bytes();

        fields_data[0] = (a_field_count + b_field_count) as u8;
        fields_data[1..][..a_field_count].copy_from_slice(unsafe { std::mem::transmute::<&[FieldType], &[u8]>(a.column_types()) });
        fields_data[1+a_field_count..][..b_field_count].copy_from_slice(unsafe { std::mem::transmute::<&[FieldType], &[u8]>(b.column_types()) });
        
        fields_data[1+a_field_count+b_field_count..][..a_data.len()].copy_from_slice(a_data);
        fields_data[1+a_field_count+b_field_count+a_data.len()..][..b_data.len()].copy_from_slice(b_data);

        EntryFields(0..fields_data.len(), fields_data.into())
    }

    pub(crate) fn byte_size(&self) -> usize {
        self.0.len() + std::mem::size_of::<Self>()
    }
}

impl Display for EntryFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Row<")?;
        for (index, field) in (0..self.len()).map(|i|self.column(i)).enumerate() {
            if index > 0 {
                write!(f, ", ")?;
            }
            match field {
                ObjectField::Bool(b) => write!(f, "Bool: {}", b),
                ObjectField::I32(i) => write!(f, "I32: {}", i),
                ObjectField::I64(i) => write!(f, "I64: {}", i),
                ObjectField::Decimal(d) => write!(f, "Decimal: {}", d),
                ObjectField::Id(uuid) => write!(f, "UUID: {}", uuid),
                ObjectField::Bytes(bytes) => write!(f, "Bytes: {:02X?}{}", &bytes[..24], if bytes.len() > 24 { "..." } else { "" }),
                ObjectField::String(s) => write!(f, "String: {}", s),
            }?;
        }
        write!(f, ">")?;
        Ok(())
    }
}

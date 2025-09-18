use std::{array::TryFromSliceError, cmp::Ordering, fmt::Display, sync::Arc};
use uuid::Uuid;

use crate::errors::storage_error::CompressionError;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ObjectField {
    String(Arc<str>),
    I32(i32),
    I64(i64),
    Decimal(f64),
    Bytes(Arc<[u8]>),
    Id(Uuid),
    Bool(bool)
}

fn take_bytes<const N: usize>(data: &[u8]) -> Result<[u8; N], CompressionError> {
    let bytes: Result<[u8; N], TryFromSliceError> = data[0..N].try_into();
    bytes.map_err(CompressionError::wrap)
}

impl ObjectField {
    pub const fn value_id(&self) -> u8 {
        match self {
            ObjectField::String(_) => 0,
            ObjectField::I32(_) => 1,
            ObjectField::I64(_) => 2,
            ObjectField::Decimal(_) => 3,
            ObjectField::Bytes(_) => 4,
            ObjectField::Id(_) => 5,
            ObjectField::Bool(_) => 6
        }
    }

    pub fn byte_size(&self) -> u64 {
        (std::mem::size_of::<Self>() as u64) + match self {
            ObjectField::String(s) => s.len() as u64,
            ObjectField::Bytes(b) => b.len() as u64,
            _ => 0
        }
    }

    pub fn compress_to(&self, to: &mut Vec<u8>) {
        to.push(self.value_id());
        match self {
            ObjectField::String(str) => {
                let compressed_length = vint64::encode(str.len() as u64);
                to.extend_from_slice(compressed_length.as_ref());
                to.extend_from_slice(str.as_bytes());
            },
            ObjectField::I32(i) => to.extend_from_slice(&i.to_le_bytes()),
            ObjectField::I64(i) => to.extend_from_slice(&i.to_le_bytes()),
            ObjectField::Decimal(d) => to.extend_from_slice(&d.to_le_bytes()),
            ObjectField::Bytes(b) => {
                let compressed_length = vint64::encode(b.len() as u64);
                to.extend_from_slice(compressed_length.as_ref());
                to.extend_from_slice(b);
            },
            ObjectField::Id(id) => to.extend_from_slice(id.as_bytes()),
            ObjectField::Bool(b) => to.push(*b as u8)
        }
    }

    pub fn decompress(data: &[u8]) -> Result<(ObjectField, usize), CompressionError> {
        let id = data[0];
        let data = &data[1..];
        match id {
            0 => {
                let vint_len = vint64::decoded_len(data[0]);
                let mut vint_slice = &data[0..vint_len];
                let bytes: u64 = vint64::decode(&mut vint_slice).map_err(CompressionError::wrap)?;
                let len = bytes as usize;
                let string = unsafe { std::str::from_utf8_unchecked(&data[vint_len..][..len]) };
                let field = ObjectField::String(string.into());
                Ok((field, vint_len + len))
            },
            1 => {
                let field = ObjectField::I32(i32::from_le_bytes(take_bytes(data)?));
                Ok((field, 4))
            },
            2 => {
                let field = ObjectField::I64(i64::from_le_bytes(take_bytes(data)?));
                Ok((field, 8))
            },
            3 => {
                let field = ObjectField::Decimal(f64::from_le_bytes(take_bytes(data)?));
                Ok((field, 8))
            },
            4 => {
                let vint_len = vint64::decoded_len(data[0]);
                let mut vint_slice = &data[0..vint_len];
                let bytes: u64 = vint64::decode(&mut vint_slice).map_err(CompressionError::wrap)?;
                let len = bytes as usize;
                let field = ObjectField::Bytes(data[vint_len..][..len].into());
                Ok((field, vint_len + len))
            },
            5 => {
                let field = ObjectField::Id(Uuid::from_bytes(take_bytes(data)?));
                Ok((field, 16))
            },
            6 => {
                let field = ObjectField::Bool(data[0] > 0);
                Ok((field, 1))
            }
            n => Err(CompressionError::from_string(format!("Invalid object field denominator: {}", n)))
        }
    }
}

const DB_EPSILON: f64 = 0.000001;

fn check_decimal_equal(a: &f64, b: &f64) -> bool {
    (a.is_nan() && b.is_nan()) || (a - b).abs() < DB_EPSILON
}

impl PartialEq for ObjectField {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::I32(l0), Self::I32(r0)) => l0 == r0,
            (Self::I64(l0), Self::I64(r0)) => l0 == r0,
            (Self::Decimal(l0), Self::Decimal(r0)) => check_decimal_equal(l0, r0),
            (Self::Bytes(l0), Self::Bytes(r0)) => l0 == r0,
            (Self::Id(l0), Self::Id(r0)) => l0 == r0,
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl Eq for ObjectField {}

impl PartialOrd for ObjectField {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ObjectField {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::String(s1), Self::String(s2)) => s1.cmp(s2),
            (Self::String(_), _) => Ordering::Greater,

            (Self::I32(i1), Self::I32(i2)) => i1.cmp(i2),
            (Self::I32(_), _) => Ordering::Greater,

            (Self::I64(i1), Self::I64(i2)) => i1.cmp(i2),
            (Self::I64(_), _) => Ordering::Greater,

            (Self::Decimal(d1), Self::Decimal(d2)) => d1.partial_cmp(d2)
                .unwrap_or_else(|| if check_decimal_equal(d1, d2) { Ordering::Equal } else { Ordering::Greater }),
            (Self::Decimal(_), _) => Ordering::Greater,

            (Self::Bytes(b1), Self::Bytes(b2)) => b1.cmp(b2),
            (Self::Bytes(_), _) => Ordering::Greater,

            (Self::Id(i1), Self::Id(i2)) => i1.cmp(i2),
            (Self::Id(_), _) => Ordering::Greater,

            (Self::Bool(b1), Self::Bool(b2)) => b1.cmp(b2),

            (_, _) => Ordering::Greater,
        }
    }
}

impl Display for ObjectField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectField::String(s) => f.write_fmt(format_args!("String: \"{}\"", s)),
            ObjectField::I32(i) => f.write_fmt(format_args!("I32: \"{}\"", i)),
            ObjectField::I64(i) => f.write_fmt(format_args!("I64: \"{}\"", i)),
            ObjectField::Decimal(d) => f.write_fmt(format_args!("Decimal: \"{}\"", d)),
            ObjectField::Bytes(b) => f.write_fmt(format_args!("Bytes: \"{:?}\"", b)),
            ObjectField::Id(id) => f.write_fmt(format_args!("Id: \"{}\"", id)),
            ObjectField::Bool(b) => f.write_fmt(format_args!("Bool: \"{}\"", b))
        }
    }
}
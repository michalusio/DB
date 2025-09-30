use std::{borrow::Cow, cmp::Ordering, fmt::Display, hash::Hash, rc::Rc};
use uuid::Uuid;

type Decimal = f64;

#[derive(Clone, Debug)]
pub enum ObjectField {
    Bool(bool),
    I32(i32),
    I64(i64),
    Decimal(Decimal),
    Id(Uuid),
    Bytes(Rc<[u8]>),
    String(Rc<str>),
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum FieldType {
    Bool = 0,
    I32 = 1,
    I64 = 2,
    Decimal = 3,
    Id = 4,
    Bytes = 5,
    String = 6,
}

impl ObjectField {
    pub fn as_bool(&self) -> Option<bool> {
        if let ObjectField::Bool(b) = self {
            Some(*b)
        } else {
            None
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        if let ObjectField::I32(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        if let ObjectField::I64(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_decimal(&self) -> Option<f64> {
        if let ObjectField::Decimal(d) = self {
            Some(*d)
        } else {
            None
        }
    }

    pub fn as_id(&self) -> Option<Uuid> {
        if let ObjectField::Id(uuid) = self {
            Some(*uuid)
        } else {
            None
        }
    }

    pub fn as_bytes(&self) -> Option<Rc<[u8]>> {
        if let ObjectField::Bytes(bytes) = self {
            Some(bytes.clone())
        } else {
            None
        }
    }

    pub fn as_string(&self) -> Option<Rc<str>> {
        if let ObjectField::String(string) = self {
            Some(string.clone())
        } else {
            None
        }
    }
}

pub(crate) const DB_EPSILON: f64 = 0.000001;

fn check_decimal_equal(a: &f64, b: &f64) -> bool {
    (a.is_nan() && b.is_nan()) || (a - b).abs() < DB_EPSILON
}

impl PartialEq for ObjectField {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            (Self::I32(l0), Self::I32(r0)) => l0 == r0,
            (Self::I64(l0), Self::I64(r0)) => l0 == r0,
            (Self::Decimal(l0), Self::Decimal(r0)) => check_decimal_equal(l0, r0),
            (Self::Id(l0), Self::Id(r0)) => l0 == r0,
            (Self::Bytes(l0), Self::Bytes(r0)) => l0 == r0,
            (Self::String(l0), Self::String(r0)) => l0 == r0,
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
            (Self::Bool(b1), Self::Bool(b2)) => b1.cmp(b2),
            (Self::Bool(_), _) => Ordering::Less,

            (Self::I32(i1), Self::I32(i2)) => i1.cmp(i2),
            (Self::I32(_), _) => Ordering::Less,

            (Self::I64(i1), Self::I64(i2)) => i1.cmp(i2),
            (Self::I64(_), _) => Ordering::Less,

            (Self::Decimal(d1), Self::Decimal(d2)) => d1.partial_cmp(d2)
                .unwrap_or_else(|| if check_decimal_equal(d1, d2) { Ordering::Equal } else { Ordering::Greater }),
            (Self::Decimal(_), _) => Ordering::Less,

            (Self::Id(i1), Self::Id(i2)) => i1.cmp(i2),
            (Self::Id(_), _) => Ordering::Less,

            (Self::Bytes(b1), Self::Bytes(b2)) => b1.cmp(b2),
            (Self::Bytes(_), _) => Ordering::Less,

            (Self::String(s1), Self::String(s2)) => s1.cmp(s2),
            (Self::String(_), _) => Ordering::Less,
        }
    }
}

impl Display for ObjectField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectField::Bool(b) => f.write_fmt(format_args!("Bool: \"{}\"", b)),
            ObjectField::I32(i) => f.write_fmt(format_args!("I32: \"{}\"", i)),
            ObjectField::I64(i) => f.write_fmt(format_args!("I64: \"{}\"", i)),
            ObjectField::Decimal(d) => f.write_fmt(format_args!("Decimal: \"{}\"", d)),
            ObjectField::Id(id) => f.write_fmt(format_args!("Id: \"{}\"", id)),
            ObjectField::Bytes(b) => f.write_fmt(format_args!("Bytes: \"{:?}\"", b)),
            ObjectField::String(s) => f.write_fmt(format_args!("String: \"{}\"", s)),
        }
    }
}

impl Hash for ObjectField {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            ObjectField::Bool(b) => b.hash(state),
            ObjectField::I32(i) => i.hash(state),
            ObjectField::I64(i) => i.hash(state),
            ObjectField::Decimal(d) => f64::to_bits(*d).hash(state),
            ObjectField::Id(uuid) => uuid.hash(state),
            ObjectField::Bytes(cow) => cow.hash(state),
            ObjectField::String(cow) => cow.hash(state),
        };
    }
}

impl From<String> for ObjectField {
    fn from(value: String) -> Self {
        ObjectField::String(value.as_str().into())
    }
}

impl<'a> From<&'a str> for ObjectField {
    fn from(value: &'a str) -> Self {
        ObjectField::String(value.into())
    }
}

impl From<i32> for ObjectField {
    fn from(value: i32) -> Self {
        ObjectField::I32(value)
    }
}

impl From<i64> for ObjectField {
    fn from(value: i64) -> Self {
        ObjectField::I64(value)
    }
}

impl From<f64> for ObjectField {
    fn from(value: f64) -> Self {
        ObjectField::Decimal(value)
    }
}

impl From<bool> for ObjectField {
    fn from(value: bool) -> Self {
        ObjectField::Bool(value)
    }
}

impl From<Uuid> for ObjectField {
    fn from(value: Uuid) -> Self {
        ObjectField::Id(value)
    }
}

impl<'a> From<&'a [u8]> for ObjectField {
    fn from(value: &'a [u8]) -> Self {
        ObjectField::Bytes(value.into())
    }
}

impl<'a> From<Cow<'a, [u8]>> for ObjectField {
    fn from(value: Cow<'a, [u8]>) -> Self {
        ObjectField::Bytes(value.into())
    }
}

impl<'a> From<Cow<'a, str>> for ObjectField {
    fn from(value: Cow<'a, str>) -> Self {
        ObjectField::String(value.into())
    }
}

use std::marker::PhantomData;
use bumpalo::collections::Vec;

use serde::{Deserializer, forward_to_deserialize_any, de::SeqAccess};

use crate::errors::query_error::DeserializerError;

use super::ObjectField;

pub struct ObjectDeserializer<'a> {
    data: Vec<'a, ObjectField>,
    index: usize,
    phantom: PhantomData<&'a ObjectField>
}

impl<'a> ObjectDeserializer<'a> {
    #[inline]
    pub fn new(data: Vec<'a, ObjectField>) -> Self {
        ObjectDeserializer {
            data,
            index: 0,
            phantom: PhantomData
        }
    }

    #[inline]
    fn next_item(&mut self) -> Option<&ObjectField> {
        let item = self.data.get(self.index);
        self.index += 1;
        item
    }
}

impl<'de> Deserializer<'de> for &mut ObjectDeserializer<'de> {
    type Error = DeserializerError;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        match self.next_item() {
            Some(ObjectField::String(s)) => visitor.visit_borrowed_str(s),
            Some(ObjectField::I32(i)) => visitor.visit_i32(*i),
            Some(ObjectField::I64(i)) => visitor.visit_i64(*i),
            Some(ObjectField::Decimal(d)) => visitor.visit_f64(*d),
            Some(ObjectField::Bytes(b)) => visitor.visit_borrowed_bytes(b),
            Some(ObjectField::Id(id)) => visitor.visit_bytes(id.as_bytes()),
            Some(ObjectField::Bool(b)) => visitor.visit_bool(*b),
            None => Err(DeserializerError::from_str("Error - no more columns in row")),
        }
    }

    #[inline]
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        visitor.visit_seq(self)
    }

    #[inline]
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if _fields.len() > self.data.len() {
            Err(DeserializerError::from_str("Error - not enough columns in the row to deserialize the struct"))
        } else {
            self.deserialize_seq(visitor)
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct tuple
        tuple_struct map enum identifier ignored_any
    }
}

impl<'de> SeqAccess<'de> for ObjectDeserializer<'de> {
    type Error = DeserializerError;

    #[inline]
    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de> {
        seed.deserialize(self).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use bumpalo::{vec, Bump};
    use serde::Deserialize;
    use serial_test::serial;

    use crate::objects::{ObjectDeserializer, ObjectField};

    #[test]
    #[serial]
    fn test_deserialization() {
        #[derive(Deserialize)]
        struct TestStruct {
            a: i64,
            b: String,
            c: f64
        }

        let arena = Bump::new();

        let data = vec![in &arena;
            ObjectField::I64(12),
            ObjectField::String("test string"),
            ObjectField::Decimal(4.55)
        ];

        let mut deserializer = ObjectDeserializer::new(data);
        let t = TestStruct::deserialize(&mut deserializer).unwrap();

        assert_eq!(12, t.a);
        assert_eq!("test string", t.b);
        assert_eq!(4.55, t.c);
    }

    #[test]
    #[serial]
    fn test_deserialization_more_columns() {
        #[derive(Deserialize)]
        struct TestStruct {
            a: i64,
            b: String,
            c: f64
        }

        let arena = Bump::new();

        let data = vec![in &arena;
            ObjectField::I64(12),
            ObjectField::String("test string"),
            ObjectField::Decimal(4.55),
            ObjectField::I32(5),
        ];

        let mut deserializer = ObjectDeserializer::new(data);
        let t = TestStruct::deserialize(&mut deserializer).unwrap();

        assert_eq!(12, t.a);
        assert_eq!("test string", t.b);
        assert_eq!(4.55, t.c);
    }

    #[test]
    #[serial]
    fn test_deserialization_not_enough_columns() {
        #[derive(Deserialize)]
        struct TestStruct {
            a: i64,
            b: String,
            c: f64
        }

        let arena = Bump::new();

        let data = vec![in &arena;
            ObjectField::I64(12),
            ObjectField::String("test string")
        ];

        let mut deserializer = ObjectDeserializer::new(data);
        let res = TestStruct::deserialize(&mut deserializer);

        assert!(res.is_err());
    }

    #[test]
    #[serial]
    fn test_deserialization_different_column_format() {
        #[derive(Deserialize)]
        struct TestStruct {
            a: i64,
            b: String
        }

        let arena = Bump::new();

        let data = vec![in &arena;
            ObjectField::I32(12),
            ObjectField::String("test string"),
        ];

        let mut deserializer = ObjectDeserializer::new(data);
        let t = TestStruct::deserialize(&mut deserializer).unwrap();

        assert_eq!(12, t.a);
        assert_eq!("test string", t.b);
    }
}

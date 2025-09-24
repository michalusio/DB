use serde::{de::{Error, SeqAccess, Unexpected}, forward_to_deserialize_any, Deserializer};
use yoke::Yoke;

use crate::{errors::query_error::DeserializerError, storage::log_file::entry_fields::{DeserializedFields, EntryFields}};

use super::ObjectField;

pub struct ObjectDeserializer {
    data: Yoke<DeserializedFields<'static>, Box<EntryFields>>,
    index: usize
}

impl ObjectDeserializer {
    pub fn new(data: EntryFields) -> Self {
        ObjectDeserializer {
            data: data.into_yoke_vector(),
            index: 0
        }
    }

    fn next_item(&'_ mut self) -> Option<&'_ ObjectField<'_>> {
        let item = self.data.get().get(self.index);
        self.index += 1;
        item
    }
}

impl<'de> Deserializer<'de> for &mut ObjectDeserializer {
    type Error = DeserializerError;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        match self.next_item() {
            Some(ObjectField::Bool(b)) => visitor.visit_bool(*b),
            Some(ObjectField::I32(i)) => visitor.visit_i32(*i),
            Some(ObjectField::I64(i)) => visitor.visit_i64(*i),
            Some(ObjectField::Decimal(d)) => visitor.visit_f64(*d),
            Some(ObjectField::Id(id)) => visitor.visit_bytes(id.as_bytes()),
            Some(ObjectField::Bytes(b)) => visitor.visit_bytes(b),
            Some(ObjectField::String(s)) => visitor.visit_str(s),
            None => Err(DeserializerError::missing_field("Error - no more columns in row")),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        match self.next_item() {
            Some(field) => match field {
                ObjectField::Bool(b) => visitor.visit_bool(*b),
                ObjectField::I32(i) => Err(DeserializerError::invalid_type(Unexpected::Signed((*i).into()), &visitor)),
                ObjectField::I64(i) => Err(DeserializerError::invalid_type(Unexpected::Signed(*i), &visitor)),
                ObjectField::Decimal(d) => Err(DeserializerError::invalid_type(Unexpected::Float(*d), &visitor)),
                ObjectField::Id(_) => Err(DeserializerError::invalid_type(Unexpected::Other("uuid"), &visitor)),
                ObjectField::Bytes(bytes) => Err(DeserializerError::invalid_type(Unexpected::Bytes(bytes), &visitor)),
                ObjectField::String(str) => Err(DeserializerError::invalid_type(Unexpected::Str(str), &visitor)),
            },
            None => Err(DeserializerError::missing_field("Error - no more columns in row"))
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        match self.next_item() {
            Some(field) => match field {
                ObjectField::Bool(b) => Err(DeserializerError::invalid_type(Unexpected::Bool(*b), &visitor)),
                ObjectField::I32(i) => visitor.visit_i32(*i),
                ObjectField::I64(i) => Err(DeserializerError::invalid_type(Unexpected::Signed(*i), &visitor)),
                ObjectField::Decimal(d) => Err(DeserializerError::invalid_type(Unexpected::Float(*d), &visitor)),
                ObjectField::Id(_) => Err(DeserializerError::invalid_type(Unexpected::Other("uuid"), &visitor)),
                ObjectField::Bytes(bytes) => Err(DeserializerError::invalid_type(Unexpected::Bytes(bytes), &visitor)),
                ObjectField::String(str) => Err(DeserializerError::invalid_type(Unexpected::Str(str), &visitor)),
            },
            None => Err(DeserializerError::missing_field("Error - no more columns in row"))
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        match self.next_item() {
            Some(field) => match field {
                ObjectField::Bool(b) => Err(DeserializerError::invalid_type(Unexpected::Bool(*b), &visitor)),
                ObjectField::I32(i) => visitor.visit_i32(*i),
                ObjectField::I64(i) => visitor.visit_i64(*i),
                ObjectField::Decimal(d) => Err(DeserializerError::invalid_type(Unexpected::Float(*d), &visitor)),
                ObjectField::Id(_) => Err(DeserializerError::invalid_type(Unexpected::Other("uuid"), &visitor)),
                ObjectField::Bytes(bytes) => Err(DeserializerError::invalid_type(Unexpected::Bytes(bytes), &visitor)),
                ObjectField::String(str) => Err(DeserializerError::invalid_type(Unexpected::Str(str), &visitor)),
            },
            None => Err(DeserializerError::missing_field("Error - no more columns in row"))
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        match self.next_item() {
            Some(field) => match field {
                ObjectField::Bool(b) => Err(DeserializerError::invalid_type(Unexpected::Bool(*b), &visitor)),
                ObjectField::I32(i) => Err(DeserializerError::invalid_type(Unexpected::Signed((*i).into()), &visitor)),
                ObjectField::I64(i) => Err(DeserializerError::invalid_type(Unexpected::Signed(*i), &visitor)),
                ObjectField::Decimal(d) => visitor.visit_f64(*d),
                ObjectField::Id(_) => Err(DeserializerError::invalid_type(Unexpected::Other("uuid"), &visitor)),
                ObjectField::Bytes(bytes) => Err(DeserializerError::invalid_type(Unexpected::Bytes(bytes), &visitor)),
                ObjectField::String(str) => Err(DeserializerError::invalid_type(Unexpected::Str(str), &visitor)),
            },
            None => Err(DeserializerError::missing_field("Error - no more columns in row"))
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        match self.next_item() {
            Some(field) => match field {
                ObjectField::Bool(b) => Err(DeserializerError::invalid_type(Unexpected::Bool(*b), &visitor)),
                ObjectField::I32(i) => Err(DeserializerError::invalid_type(Unexpected::Signed((*i).into()), &visitor)),
                ObjectField::I64(i) => Err(DeserializerError::invalid_type(Unexpected::Signed(*i), &visitor)),
                ObjectField::Decimal(d) => Err(DeserializerError::invalid_type(Unexpected::Float(*d), &visitor)),
                ObjectField::Id(_) => Err(DeserializerError::invalid_type(Unexpected::Other("uuid"), &visitor)),
                ObjectField::Bytes(bytes) => Err(DeserializerError::invalid_type(Unexpected::Bytes(bytes), &visitor)),
                ObjectField::String(str) => visitor.visit_str(str),
            },
            None => Err(DeserializerError::missing_field("Error - no more columns in row"))
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        match self.next_item() {
            Some(field) => match field {
                ObjectField::Bool(b) => Err(DeserializerError::invalid_type(Unexpected::Bool(*b), &visitor)),
                ObjectField::I32(i) => Err(DeserializerError::invalid_type(Unexpected::Signed((*i).into()), &visitor)),
                ObjectField::I64(i) => Err(DeserializerError::invalid_type(Unexpected::Signed(*i), &visitor)),
                ObjectField::Decimal(d) => Err(DeserializerError::invalid_type(Unexpected::Float(*d), &visitor)),
                ObjectField::Id(_) => Err(DeserializerError::invalid_type(Unexpected::Other("uuid"), &visitor)),
                ObjectField::Bytes(bytes) => Err(DeserializerError::invalid_type(Unexpected::Bytes(bytes), &visitor)),
                ObjectField::String(str) => visitor.visit_string(str.clone().into_owned()),
            },
            None => Err(DeserializerError::missing_field("Error - no more columns in row"))
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        match self.next_item() {
            Some(field) => match field {
                ObjectField::Bool(b) => Err(DeserializerError::invalid_type(Unexpected::Bool(*b), &visitor)),
                ObjectField::I32(i) => Err(DeserializerError::invalid_type(Unexpected::Signed((*i).into()), &visitor)),
                ObjectField::I64(i) => Err(DeserializerError::invalid_type(Unexpected::Signed(*i), &visitor)),
                ObjectField::Decimal(d) => Err(DeserializerError::invalid_type(Unexpected::Float(*d), &visitor)),
                ObjectField::Id(uuid) => visitor.visit_bytes(uuid.as_bytes()),
                ObjectField::Bytes(bytes) => visitor.visit_bytes(bytes),
                ObjectField::String(str) => visitor.visit_bytes(str.as_bytes()),
            },
            None => Err(DeserializerError::missing_field("Error - no more columns in row"))
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: serde::de::Visitor<'de> {
        visitor.visit_seq(self)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de> {
        if _fields.len() > self.data.get().len() {
            Err(DeserializerError::from_str("Error - not enough columns in the row to deserialize the struct"))
        } else {
            self.deserialize_seq(visitor)
        }
    }

    forward_to_deserialize_any! {
        i8 i16 i128 u8 u16 u32 u64 u128 f32 char
        byte_buf option unit unit_struct newtype_struct tuple
        tuple_struct map enum identifier ignored_any
    }
}

impl<'de> SeqAccess<'de> for ObjectDeserializer {
    type Error = DeserializerError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de> {
        seed.deserialize(self).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serial_test::parallel;
    use crate::{objects::ObjectDeserializer, storage::log_file::entry_fields::EntryFields, ObjectField};

    #[test]
    #[parallel]
    fn test_deserialization() {
        #[derive(Deserialize)]
        struct TestStruct {
            a: i64,
            b: String,
            c: f64
        }

        let data: Vec<ObjectField> = vec![
            12.into(),
            "test string".into(),
            4.55.into()
        ];

        let entry = EntryFields::from(data);
        let mut deserializer = ObjectDeserializer::new(entry);
        let t = TestStruct::deserialize(&mut deserializer).unwrap();

        assert_eq!(12, t.a);
        assert_eq!("test string", t.b);
        assert_eq!(4.55, t.c);
    }

    #[test]
    #[parallel]
    fn test_deserialization_more_columns() {
        #[derive(Deserialize)]
        struct TestStruct {
            a: i64,
            b: String,
            c: f64
        }

        let data: Vec<ObjectField> = vec![
            12.into(),
            "test string".into(),
            4.55.into(),
            5.into(),
        ];

        let entry = EntryFields::from(data);
        let mut deserializer = ObjectDeserializer::new(entry);
        let t = TestStruct::deserialize(&mut deserializer).unwrap();

        assert_eq!(12, t.a);
        assert_eq!("test string", t.b);
        assert_eq!(4.55, t.c);
    }

    #[test]
    #[parallel]
    fn test_deserialization_not_enough_columns() {
        #[derive(Deserialize)]
        struct TestStruct {
            a: i64,
            b: String,
            c: f64
        }

        let data: Vec<ObjectField> = vec![
            12.into(),
            "test string".into()
        ];

        let entry = EntryFields::from(data);
        let mut deserializer = ObjectDeserializer::new(entry);
        let res = TestStruct::deserialize(&mut deserializer);

        assert!(res.is_err());
    }

    #[test]
    #[parallel]
    fn test_deserialization_different_column_format() {
        #[derive(Deserialize)]
        struct TestStruct {
            a: i64,
            b: String
        }

        let data: Vec<ObjectField> = vec![
            12.into(),
            "test string".into(),
        ];

        let entry = EntryFields::from(data);
        let mut deserializer = ObjectDeserializer::new(entry);
        let t = TestStruct::deserialize(&mut deserializer).unwrap();

        assert_eq!(12, t.a);
        assert_eq!("test string", t.b);
    }
}

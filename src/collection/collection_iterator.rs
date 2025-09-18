use std::iter::FusedIterator;
use std::marker::PhantomData;

use serde::Deserialize;

use super::native_collection_iterator::NativeCollectionIterator;
use crate::errors::DatabaseError;
use crate::objects::ObjectDeserializer;
use crate::utils::DBResult;

pub struct CollectionIterator<'a, T: Deserialize<'a>> {
    iterator: NativeCollectionIterator<'a>,
    data: PhantomData<T>
}

impl<'a, T: Deserialize<'a>> CollectionIterator<'a, T> {

    pub(crate) fn new(value: NativeCollectionIterator<'a>) -> Self {
        Self {
            iterator: value,
            data: PhantomData
        }
    } 
}

impl<'a, T: Deserialize<'a>> Iterator for CollectionIterator<'a, T> {
    type Item = DBResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iterator.next() {
            None => None,
            Some(Err(err)) => Some(Err(err)),
            Some(Ok((_, values))) => {
                let mut deserializer = ObjectDeserializer::new(values);
                let value = T::deserialize(&mut deserializer)
                    .map_err(|err| DatabaseError::Query(err.into()));
                Some(value)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iterator.size_hint()
    }
}

impl<'a, T: Deserialize<'a>> FusedIterator for CollectionIterator<'a, T> {}
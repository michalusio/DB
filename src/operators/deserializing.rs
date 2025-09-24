use std::iter::FusedIterator;
use std::marker::PhantomData;

use serde::Deserialize;

use crate::errors::DatabaseError;
use crate::objects::ObjectDeserializer;
use crate::utils::DBResult;
use crate::DBOperator;

pub struct Deserializing<'a, Iter: DBOperator, T: Deserialize<'a>> {
    iterator: Iter,
    data: PhantomData<&'a T>
}

impl<'a, Iter: DBOperator, T: Deserialize<'a>> Deserializing<'a, Iter, T> {

    pub(crate) fn new(iterator: Iter) -> Self {
        Self {
            iterator,
            data: PhantomData
        }
    } 
}

impl<'a, Iter: DBOperator, T: Deserialize<'a>> Iterator for Deserializing<'a, Iter, T> {
    type Item = DBResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iterator.next() {
            Ok(None) => None,
            Err(err) => Some(Err(err)),
            Ok(Some(row)) => {
                let mut deserializer = ObjectDeserializer::new(row.fields);
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

impl<'a, Iter: DBOperator, T: Deserialize<'a>> FusedIterator for Deserializing<'a, Iter, T> {}

use std::hash::Hash;

use gxhash::HashMap;
use gxhash::HashMapExt;
use log_err::LogErrOption;

use crate::EntryFields;
use crate::IntoHashable;
use crate::{DBOperator, DBResult, Row};

pub struct HashMatch<Iter, HashedIter, Key, IterGetter, HashedGetter>
where   Iter: DBOperator,
        HashedIter: DBOperator,
        Key: Hash + Clone + Eq,
        IterGetter: Clone + Fn(&EntryFields) -> Key,
        HashedGetter: Clone + Fn(&EntryFields) -> Key
{
    iterator: Iter,
    current_row: Option<Row>,
    current_vector: Option<Vec<Row>>,
    hashed_iterator: HashedIter,
    first_getter: IterGetter,
    hashed_getter: HashedGetter,
    hash_map: Option<HashMap<Key, Vec<Row>>>
}

impl<Iter, HashedIter, Key, IterGetter, HashedGetter> Clone for HashMatch<Iter, HashedIter, Key, IterGetter, HashedGetter>
where   Iter: DBOperator,
        HashedIter: DBOperator,
        Key: Hash + Clone + Eq,
        IterGetter: Clone + Fn(&EntryFields) -> Key,
        HashedGetter: Clone + Fn(&EntryFields) -> Key
{
    fn clone(&self) -> Self {
        Self {
            iterator: self.iterator.clone(),
            current_row: self.current_row.clone(),
            current_vector: self.current_vector.clone(),
            hashed_iterator: self.hashed_iterator.clone(),
            first_getter: self.first_getter.clone(),
            hashed_getter: self.hashed_getter.clone(),
            hash_map: self.hash_map.clone()
        }
    }
}

impl<Iter, HashedIter, Key, IterGetter, HashedGetter> HashMatch<Iter, HashedIter, Key, IterGetter, HashedGetter>
where   Iter: DBOperator,
        HashedIter: DBOperator,
        Key: Hash + Clone + Eq,
        IterGetter: Clone + Fn(&EntryFields) -> Key,
        HashedGetter: Clone + Fn(&EntryFields) -> Key
{
    pub fn new(iterator: Iter, hashed_iterator: HashedIter, first_getter: IterGetter, hashed_getter: HashedGetter) -> Self {
        HashMatch {
            iterator,
            current_row: None,
            current_vector: None,
            hashed_iterator,
            first_getter,
            hashed_getter,
            hash_map: None
        }
    }
}

impl<Iter, HashedIter, Key, IterGetter, HashedGetter> DBOperator for HashMatch<Iter, HashedIter, Key, IterGetter, HashedGetter>
where   Iter: DBOperator,
        HashedIter: DBOperator,
        Key: Hash + Clone + Eq,
        IterGetter: Clone + Fn(&EntryFields) -> Key,
        HashedGetter: Clone + Fn(&EntryFields) -> Key
{

    fn next(&mut self) -> DBResult<Option<Row>> {
        if self.hash_map.is_none() {
            let mut map = HashMap::<Key, Vec<Row>>::new();
            while let Some(row) = self.hashed_iterator.next()? {
                let clone = row.clone();
                let key = (self.hashed_getter)(&row.fields).to_hashable();
                match map.get_mut(&key) {
                    Some(vec) => {
                        vec.push(clone);
                    },
                    None => {
                        map.insert(key, vec![clone]);
                    }
                };
            }
            self.hash_map = Some(map);
        }

        loop {
            if self.current_row.is_none() {
                match self.iterator.next()? {
                    Some(row) => {
                        self.current_row = Some(row);
                    },
                    None => {
                        return Ok(None);
                    }
                }
            }

            let row = self.current_row.as_mut().log_unwrap();
            if self.current_vector.is_none() {
                let map = self.hash_map.as_mut().log_unwrap();
                let key = (self.first_getter)(&row.fields).to_hashable();
                if let Some(vec) = map.get_mut(&key) {
                    self.current_vector = Some(vec.clone());
                } else {
                    self.current_row = None;
                }
            } else {
                let vec  = self.current_vector.as_mut().log_unwrap();
                if let Some(second_row) = vec.pop() {
                    let combined_row = Row::combine(row, &second_row);
                    return Ok(Some(combined_row));
                } else {
                    self.current_row = None;
                    self.current_vector = None;
                };
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let hint1 = self.iterator.size_hint();
        let hint2 = self.hashed_iterator.size_hint();
        (
            hint1.0 * hint2.0,
            hint1.1.zip(hint2.1).map(|(a, b)| a.saturating_mul(b))
        )
    }
}

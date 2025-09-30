use std::collections::VecDeque;

use log_err::LogErrOption;

use crate::{DBOperator, DBResult, EntryFields, Row, SortDirection};

#[derive(Clone)]
pub struct InMemorySort<Iter, Key, KeyFunction>
where   Iter: DBOperator,
        Key: Ord + Clone,
        KeyFunction: Clone + Fn(&EntryFields) -> Key
 {
    iterator: Iter,
    key_function: KeyFunction,
    sort_direction: SortDirection,
    sorted_data: Option<VecDeque<Row>>
}

impl <Iter, Key, KeyFunction> InMemorySort<Iter, Key, KeyFunction>
where   Iter: DBOperator,
        Key: Ord + Clone,
        KeyFunction: Clone + Fn(&EntryFields) -> Key
 {
    pub fn new(iterator: Iter, key_function: KeyFunction, sort_direction: SortDirection) -> Self {
        InMemorySort {
            iterator,
            key_function,
            sort_direction,
            sorted_data: None
        }
    }
}

impl<Iter, Key, KeyFunction> DBOperator for InMemorySort<Iter, Key, KeyFunction>
where   Iter: DBOperator,
        Key: Ord + Clone,
        KeyFunction: Clone + Fn(&EntryFields) -> Key
 {

    fn next(&mut self) -> DBResult<Option<Row>> {
        if self.sorted_data.is_none() {
            let mut data = VecDeque::<Row>::with_capacity(self.iterator.size_hint().0 + 1);
            loop {
                match self.iterator.next()? {
                    Some(row) => {
                        data.push_back(row);
                    },
                    None => {
                        data.make_contiguous().sort_unstable_by_key(|row| (self.key_function)(&row.fields));
                        break;
                    }
                }
            };
            self.sorted_data = Some(data);
        }

        let sorted = self.sorted_data.as_mut().log_unwrap();
        if self.sort_direction == SortDirection::Ascending {
            Ok(sorted.pop_back())
        } else {
            Ok(sorted.pop_front())
        }
    }

    fn reset(&mut self) {
        self.iterator.reset();
        self.sorted_data = None;
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iterator.size_hint()
    }
}

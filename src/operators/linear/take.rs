use std::{cmp::min};

use crate::{DBOperator, DBResult, Row};

#[derive(Clone)]
pub struct Take<Iter: DBOperator> {
    iterator: Iter,
    items: usize,
    taken: usize
}

impl<Iter: DBOperator> Take<Iter> {
    pub fn new(iterator: Iter, items: usize) -> Self {
        Take {
            iterator,
            items,
            taken: 0
        }
    }
}

impl<Iter: DBOperator> DBOperator for Take<Iter> {
    fn next(&mut self) -> DBResult<Option<Row>> {
        let next_item = self.iterator.next();
        match next_item? {
            Some(data) => {
                if self.taken >= self.items {
                    return Ok(None);
                }
                self.taken += 1;
                Ok(Some(data))
            },
            None => Ok(None)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let iterator_left = self.items - self.taken;
        let (min_size, max_size) = self.iterator.size_hint();
        (
            min(min_size, iterator_left),
            Some(max_size.map_or(iterator_left, |max_size| min(max_size, iterator_left)))
        )
    }
}

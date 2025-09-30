use crate::{DBOperator, DBResult, Row};

#[derive(Clone)]
pub struct Skip<Iter: DBOperator> {
    iterator: Iter,
    items: usize,
    skipped: usize
}

impl<Iter: DBOperator> Skip<Iter> {
    pub fn new(iterator: Iter, items: usize) -> Self {
        Skip {
            iterator,
            items,
            skipped: 0
        }
    }
}

impl<Iter: DBOperator> DBOperator for Skip<Iter> {

    fn next(&mut self) -> DBResult<Option<Row>> {
        let next_item = self.iterator.next();
        match next_item? {
            Some(data) => {
                if self.skipped >= self.items {
                    return Ok(Some(data));
                }
                self.skipped += 1;
                Ok(None)
            },
            None => Ok(None)
        }
    }

    fn reset(&mut self) {
        self.iterator.reset();
        self.skipped = 0;
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (min_size, max_size) = self.iterator.size_hint();
        (
            min_size.saturating_sub(self.skipped),
            max_size.map(|max_size| max_size.saturating_sub(self.skipped))
        )
    }
}

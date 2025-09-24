use crate::{DBOperator, DBResult, EntryFields, Row};

#[derive(Clone)]
pub struct Filter<Iter: DBOperator, Predicate>
where Predicate: Clone + Fn(&EntryFields) -> bool
{
    iterator: Iter,
    predicate: Predicate
}

impl <Iter: DBOperator, Predicate> Filter<Iter, Predicate>
where Predicate: Clone + Fn(&EntryFields) -> bool
{
    pub fn new(iterator: Iter, predicate: Predicate) -> Self {
        Filter {
            iterator,
            predicate
        }
    }
}

impl<Iter: DBOperator, Predicate> DBOperator for Filter<Iter, Predicate>
where Predicate: Clone + Fn(&EntryFields) -> bool
{
    fn next(&mut self) -> DBResult<Option<Row>> {
        loop {
            let next_item = self.iterator.next();
            match next_item? {
                Some(data) => {
                    if (self.predicate)(&data.fields) {
                        return Ok(Some(data));
                    } else {
                        continue;
                    }
                },
                None => return Ok(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iterator.size_hint()
    }
}

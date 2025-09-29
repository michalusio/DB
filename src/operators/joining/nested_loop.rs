use log_err::LogErrOption;

use crate::{DBOperator, DBResult, Row};

#[derive(Clone)]
pub struct NestedLoop<Iter: DBOperator, Iter2: DBOperator> {
    iterator: Iter,
    second_iterator: Iter2,
    first_column: usize,
    second_column: usize,
    current_first_value: Option<Row>,
    current_second_iterator: Option<Iter2>
}

impl<Iter: DBOperator, Iter2: DBOperator> NestedLoop<Iter, Iter2> {
    pub fn new(iterator: Iter, second_iterator: Iter2, first_column: usize, second_column: usize) -> Self {
        NestedLoop {
            iterator,
            second_iterator,
            first_column,
            second_column,
            current_first_value: None,
            current_second_iterator: None
        }
    }
}

impl<Iter: DBOperator, Iter2: DBOperator> DBOperator for NestedLoop<Iter, Iter2> {

    fn next(&mut self) -> DBResult<Option<Row>> {
        loop {
            if self.current_first_value.is_none() {
                let next_item = self.iterator.next()?;
                match next_item {
                    Some(data) => {
                        self.current_first_value = Some(data);
                    },
                    None => {
                        return Ok(None);
                    }
                };
            }
            let first_value = self.current_first_value.as_mut().log_unwrap();

            if self.current_second_iterator.is_none() {
                self.current_second_iterator = Some(self.second_iterator.clone());
            }
            let second_iter = self.current_second_iterator.as_mut().log_unwrap();

            let second_item = second_iter.next()?;
            if let Some(item) = second_item {
                if first_value.fields.column(self.first_column) == item.fields.column(self.second_column) {
                    let return_value = Row::combine(first_value, &item);
                    return Ok(Some(return_value));
                };
            } else {
                self.current_first_value = None;
                self.current_second_iterator = None;
                continue;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let hint1 = self.iterator.size_hint();
        let iter2 = self.current_second_iterator.as_ref().unwrap_or(&self.second_iterator);
        let hint2 = iter2.size_hint();
        (
            hint1.0 * hint2.0,
            hint1.1.zip(hint2.1).map(|(a, b)| a.saturating_mul(b))
        )
    }
}

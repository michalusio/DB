use std::{borrow::Borrow, ops::Deref};

use itertools::Either;
use uuid::Uuid;

use crate::{objects::DB_EPSILON, DBOperator, DBResult, EntryFields, ObjectField, Row};

#[derive(Clone)]
pub struct Select<'a, Iter: DBOperator, Selector>
where for<'x> Selector: Clone + FnOnce(SelectBuilder<'x>) -> SelectBuilder<'x>
{
    iterator: Iter,
    aggregator: SelectAggregator<'a>,
    selector: Selector
}

impl <'a, Iter: DBOperator, Selector> Select<'a, Iter, Selector>
where for<'x> Selector: Clone + FnOnce(SelectBuilder<'x>) -> SelectBuilder<'x>
{
    pub fn new(iterator: Iter, selector: Selector) -> Self {
        Select {
            iterator,
            aggregator: SelectAggregator::new(),
            selector
        }
    }
}

impl<'a, Iter: DBOperator, Selector> DBOperator for Select<'a, Iter, Selector>
where for<'x> Selector: Clone + FnOnce(SelectBuilder<'x>) -> SelectBuilder<'x>
{
    fn next(&mut self) -> DBResult<Option<Row>> {
        loop {
            let next_item = self.iterator.next();
            match next_item? {
                Some(data) => {
                    let builder = SelectBuilder::new(data.fields);
                    let builder = (self.selector.clone())(builder);
                    match builder.get_row() {
                        Either::Left(row) => {
                            return Ok(Some(row));
                        },
                        Either::Right(builder) => {
                            if let Some(row) = self.aggregator.aggregate(builder) {
                                return Ok(Some(row));
                            } else {
                                continue;
                            }
                        }
                    }
                },
                None => {
                    if self.aggregator.is_empty() {
                        return Ok(None);
                    } else {
                        let mut fields = vec![];
                        std::mem::swap(&mut self.aggregator.fields, &mut fields);
                        return Ok(Some(Row {
                            id: Uuid::new_v4(),
                            fields: fields.into()
                        }));
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iterator.size_hint()
    }
}

#[derive(Clone)]
pub struct SelectAggregator<'a> {
    fields: Vec<SelectField<'a>>
}

impl<'a> SelectAggregator<'a> {
    fn new() -> Self {
        SelectAggregator {
            fields: vec![]
        }
    }

    fn aggregate(&mut self, builder: SelectBuilder<'a>) -> Option<Row> {
        if self.fields.is_empty() {
            // Aggregator is empty - the received row becomes the aggregator
            self.fields = builder.fields;
            None
        } else if self.fields.len() != builder.fields.len()
    || self.fields.iter()
        .zip(builder.fields.iter())
        .any(|(a, b)| !SelectField::groups(a, b)) {
            // New row has different length from aggregator, or the grouping columns do not match:
            //  emit the aggregated row and save the builder's fields as aggregator's
            let mut fields = builder.fields;
            std::mem::swap(&mut self.fields, &mut fields);

            // convert count(bool) to count(i64)
            for ele in self.fields.iter_mut() {
                if let SelectField::Count(ObjectField::Bool(b)) = ele {
                    *ele = SelectField::Count(ObjectField::I64(if *b { 1 } else { 0 }));
                }
            }

            let row = Row {
                id: Uuid::new_v4(),
                fields: fields.into()
            };
            Some(row)
        } else {
            // All grouping columns match - combine the aggregator with received row
            for (aggregate, next) in self.fields.iter_mut().zip(builder.fields.into_iter()) {
                aggregate.combine(next);
            }
            None
        }
    }

    fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

pub struct SelectBuilder<'a> {
    pub row: EntryFields,
    fields: Vec<SelectField<'a>>
}

#[derive(Clone)]
pub enum SelectField<'a> {
    Field(ObjectField<'a>),
    Sum(ObjectField<'a>),
    Count(ObjectField<'a>),
    Max(ObjectField<'a>),
    Min(ObjectField<'a>),
}

impl<'a> Deref for SelectField<'a> {
    type Target = ObjectField<'a>;
    
    fn deref(&self) -> &Self::Target {
        match self {
            SelectField::Field(object_field) => object_field,
            SelectField::Sum(object_field) => object_field,
            SelectField::Count(object_field) => object_field,
            SelectField::Max(object_field) => object_field,
            SelectField::Min(object_field) => object_field,
        }
    }
}

impl<'a> SelectField<'a> {

    fn groups(a: &SelectField, b: &SelectField) -> bool {
        match (a, b) {
            (SelectField::Field(field_a), SelectField::Field(field_b)) => field_a == field_b,
            (SelectField::Count(_), SelectField::Count(_)) => true,
            (SelectField::Max(_), SelectField::Max(_)) => true,
            (SelectField::Min(_), SelectField::Min(_)) => true,
            (SelectField::Sum(_), SelectField::Sum(_)) => true,
            (_, _) => false,
        }
    }

    fn combine(&mut self, next: SelectField<'a>) {
        if let SelectField::Field(_) = self {
            return;
        }
        let field = match (&self, next) {
            (SelectField::Sum(self_field), SelectField::Sum(next_field)) => {
                SelectField::Sum(match (self_field, next_field) {
                    (ObjectField::I32(a), ObjectField::I32(b)) => ObjectField::I32(*a + b),
                    (ObjectField::I64(a), ObjectField::I64(b)) => ObjectField::I64(*a + b),
                    (ObjectField::Decimal(a), ObjectField::Decimal(b)) => ObjectField::Decimal(*a + b),
                    (_, _) => unimplemented!(),
                })
            },
            (SelectField::Count(ObjectField::I64(self_i64)), SelectField::Count(ObjectField::Bool(b))) => {
                SelectField::Count(ObjectField::I64(*self_i64 + b as i64))
            },
            (SelectField::Max(self_field), SelectField::Max(next_field)) => {
                let field: &ObjectField<'_> = self_field;
                if field < &next_field {
                    SelectField::Max(next_field.clone())
                } else {
                    SelectField::Max(self_field.clone())
                }
            },
            (SelectField::Min(self_field), SelectField::Min(next_field)) => {
                let field: &ObjectField<'_> = self_field;
                if field > &next_field {
                    SelectField::Max(next_field.clone())
                } else {
                    SelectField::Max(self_field.clone())
                }
            },
            (_, _) => unreachable!()
        };
        *self = field;
    }
}

impl<'a> SelectBuilder<'a> {
    fn new(row: EntryFields) -> Self {
        SelectBuilder {
            row,
            fields: vec![]
        }
    }
    fn get_row(self) -> Either<Row, Self> {
        if self.fields.iter().any(|f| !matches!(f, SelectField::Field(_))) {
            Either::Right(self)
        } else {
            Either::Left(Row {
                id: Uuid::new_v4(),
                fields: self.fields.into()
            })
        }
    }

    pub fn value<'x, T: Borrow<impl Into<ObjectField<'x>> + Clone>>(mut self, value: T) -> Self {
        self.fields.push(SelectField::Field(value.borrow().clone().into().change_lifetime()));
        self
    }

    pub fn sum_value<'x, T: Borrow<impl Into<ObjectField<'x>> + Clone>>(mut self, value: T) -> Self {
        self.fields.push(SelectField::Sum(value.borrow().clone().into().change_lifetime()));
        self
    }
    
    pub fn max_value<'x, T: Borrow<impl Into<ObjectField<'x>> + Clone>>(mut self, value: T) -> Self {
        self.fields.push(SelectField::Max(value.borrow().clone().into().change_lifetime()));
        self
    }
    
    pub fn min_value<'x, T: Borrow<impl Into<ObjectField<'x>> + Clone>>(mut self, value: T) -> Self {
        self.fields.push(SelectField::Min(value.borrow().clone().into().change_lifetime()));
        self
    }

    pub fn count(mut self) -> Self {
        self.fields.push(SelectField::Count(true.into()));
        self
    }

    pub fn count_when<'x: 'a>(mut self, value: impl Into<ObjectField<'x>> + Clone) -> Self {
        self.fields.push(SelectField::Count(match value.clone().into() {
            ObjectField::Bool(b) => b,
            ObjectField::I32(i) => i != 0,
            ObjectField::I64(i) => i != 0,
            ObjectField::Decimal(d) => !d.is_nan() && d.abs() < DB_EPSILON,
            ObjectField::Id(uuid) => !uuid.is_nil(),
            ObjectField::Bytes(cow) => !cow.is_empty(),
            ObjectField::String(cow) => !cow.is_empty(),
        }.into()));
        self
    }

    pub fn column(self, index: usize) -> Self {
        let column = self.row.column(index);
        let column = column.change_lifetime();
        self.value(column)
    }

}

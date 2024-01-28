use std::{marker::PhantomData, collections::BTreeSet};

use serde::Deserialize;

use crate::{set, utils::DBResult};

use self::condition::{Condition, Normalizable};

use super::collection::Collection;

pub mod condition;
pub mod binary_expression;
pub mod unary_expression;

#[derive(Clone)]
pub struct Query<'a, Item: Deserialize<'a>> {
    on: &'a Collection,
    /**
     * All filters made on the query, in DNF form
     */
    conditions: BTreeSet<Condition>,
    phantom: PhantomData<Item>
}

impl<'a, Item: Deserialize<'a> + 'a> Query<'a, Item> {

    pub(crate) fn from_collection(collection: &'a Collection) -> Self {
        Query {
            on: collection,
            conditions: set!(),
            phantom: PhantomData::<Item>
        }
    }

    pub fn filter(mut self, condition: Condition) -> Self {
        let condition = condition.normalize();
        match condition {
            Condition::And(conditions) => {
                self.conditions.extend(conditions);
            },
            c => {
                self.conditions.insert(c);
            },
        };
        self
    }

    pub fn collect(self) -> DBResult<Vec<Item>> {
        self.on.iterate::<Item>().collect()
    }
}

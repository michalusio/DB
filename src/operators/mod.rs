use std::hash::Hash;
use serde::Deserialize;
use crate::{storage::log_file::log_entry::Row, DBResult, EntryFields};

mod deserializing; pub use deserializing::*;
mod sourcing; pub use sourcing::*;
mod linear; pub use linear::*;
mod joining; pub use joining::*;
mod sorting; pub use sorting::*;
mod aggregation; pub use aggregation::*;

pub trait DBOperator: Sized + Clone {
    fn next(&mut self) -> DBResult<Option<Row>>;

    /// Returns the bounds on the remaining length of the operator.
    ///
    /// Specifically, `size_hint()` returns a tuple where the first element
    /// is the lower bound, and the second element is the upper bound.
    ///
    /// The second half of the tuple that is returned is an <code>[Option]<[usize]></code>.
    /// A [`None`] here means that either there is no known upper bound, or the
    /// upper bound is larger than [`usize`].
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }

    /// Returns the estimate on the remaining cost of the operator.
    ///
    /// Specifically, `cost_hint()` returns an <code>[Option]<[usize]></code>.
    /// A [`None`] here means that either the cost estimate is not known, or the estimate is larger than [`usize`].
    /// A [`Some`] specifies the estimated cost of executing the rest of the operator, in not-specified units relative to the other operators.
    fn cost_hint(&self) -> Option<usize> {
        None
    }

    fn filter<Predicate: Clone + Fn(&EntryFields) -> bool>(self, predicate: Predicate) -> Filter<Self, Predicate> {
        Filter::new(self, predicate)
    }

    fn deserialize<'a, D: Deserialize<'a>>(self) -> Deserializing<'a, Self, D> {
        Deserializing::new(self)
    }
    
    fn take(self, items: usize) -> Take<Self> {
        Take::new(self, items)
    }
    
    fn skip(self, items: usize) -> Skip<Self> {
        Skip::new(self, items)
    }

    fn collect(mut self) -> DBResult<Vec<Row>> {
        if cfg!(debug_assertions) {
            let hint = self.size_hint();
            assert!(hint.1.is_none_or(|high| high >= hint.0), "Malformed size_hint {hint:?}");
        }
        let mut result = Vec::with_capacity(self.size_hint().0.saturating_add(1));
        loop {
            match self.next()? {
                Some(data) => result.push(data),
                None => return Ok(result),
            }
        }
    }

    fn nested_loop<Iter2: DBOperator>(self, iter2: Iter2, a_column_index: usize, b_column_index: usize) -> NestedLoop<Self, Iter2> {
        NestedLoop::new(self, iter2, a_column_index, b_column_index)
    }

    fn hash_match<HashedIter: DBOperator, Key: Hash + Clone + Eq, IterGetter: Clone + Fn(&EntryFields) -> Key, HashedGetter: Clone + Fn(&EntryFields) -> Key>(self, hashed_iterator: HashedIter, iter_getter: IterGetter, hashed_getter: HashedGetter) -> HashMatch<Self, HashedIter, Key, IterGetter, HashedGetter> {
        HashMatch::new(self, hashed_iterator, iter_getter, hashed_getter)
    }

    fn in_memory_sort<Key: Clone + Ord, KeyFunction: Clone + Fn(&EntryFields) -> Key>(self, key_function: KeyFunction, sort_direction: SortDirection) -> InMemorySort<Self, Key, KeyFunction> {
        InMemorySort::new(self, key_function, sort_direction)
    }
}

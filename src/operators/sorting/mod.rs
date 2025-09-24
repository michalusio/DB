mod in_memory_sort; pub use in_memory_sort::InMemorySort;

#[derive(Clone, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending
}

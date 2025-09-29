# Rust Query Engine

A Rust library for **composable, iterator-style query execution**.
Instead of parsing SQL strings, you construct queries with Rust method chains and closures. This gives you type safety, inline logic, and seamless integration with the rest of your Rust code.

---

## Features

* **Library-first design**: Import like a library, use like a database.
* **Iterator-style execution**: Query operators are lazy and composable, just like Rust’s standard iterators.
* **ACID-compliant**: Based on a log-structured approach, with Atomicity, Consistency, Isolation and Durability built-in from the get-go.
* **Type-safe row access**: Use strongly typed getters like `as_i32()`, `as_str()`, etc. or deserialize into your Serde-compatible structs.
* **Composable pipelines**: Build complex plans by chaining operators - you don't need a query planner to tell you what to do.

---

## Example

```rust
use uuid::Uuid;

// Instantiate the database engine
let mut engine = Storage::new().unwrap();

let transaction = Uuid::now_v7();

// Get collections from the engine
let table1 = engine.get_collection("table").unwrap().read().unwrap();
let table2 = engine.get_collection("table2").unwrap().read().unwrap();

// Build and execute a query
let rows = table1
    .table_scan(transaction)
    .hash_match(
        table2.table_scan(transaction),
        |row| row.column(3).as_i32().unwrap(),            // join key from table1
        |other| other.column(0).as_i32().unwrap(),        // join key from table2
    )
    .in_memory_sort(|row| row.column(4).as_i32().unwrap(), SortDirection::Descending)
    .select(|builder| {
        let i = builder.row.column(4).as_i32().unwrap() * 3;
        builder
            .column(1)
            .column(2)
            .column(8)
            .max_value(i)
    })
    .collect()
    .unwrap();
```

---

## Why a Library Instead of SQL?

* **Rust-native**: Build queries with closures, no string parsing.
* **Type safety**: Query logic is checked at compile time, not at runtime.
* **Flexibility**: You can easily extend the engine with your own operators and functions. Need a vector similarity match? Just add it in!
* **Integration**: Works naturally inside Rust apps without extra parsing layers - it's a library after all.

---

## Roadmap

- :heavy_check_mark: On-disk persistence
- :heavy_check_mark: Parallel execution
- Column Types:
  - :heavy_check_mark: bool
  - :heavy_check_mark: i32
  - :heavy_check_mark: i64
  - :hammer: decimal
  - :heavy_check_mark: Uuid
  - :heavy_check_mark: Byte array
  - :heavy_check_mark: String
- :heavy_check_mark: Serde support
- Operators:
  - Sourcing:
    - :o: Table Seek
    - :heavy_check_mark: Table Scan
    - :o: Index Seek
    - :o: Index Scan
    - :o: Concatenate
    - :o: Constant Scan
  - Spools:
    - :o: Table Spool
    - :o: Row Count Spool
    - :o: Index Spool
    - :o: Merge Intervals
  - Linear:
    - :heavy_check_mark: Filter
    - :heavy_check_mark: Take
    - :heavy_check_mark: Skip
    - :heavy_check_mark: Select & Aggregate
    - :o: Distinct
  - Sorting:
    - :heavy_check_mark: In-Memory Std Sort
    - :o: In-Memory Bucket Sort
  - Joining:
    - :heavy_check_mark: Nested Loop
    - :heavy_check_mark: Hash Match
    - :o: Merge Join
    - :o: Adaptive Join
- Indexes:
  - :o: Sorted
  - :o: Reverse
  - :o: Bitmap
  - :o: Hash
- :o: Statistics

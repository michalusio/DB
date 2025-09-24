use criterion::{criterion_group, criterion_main, Criterion};
use db::{DBResult, ObjectField, Row, Storage};
use uuid::Uuid;

use crate::utils::{generate_sample_data, init_benchmark, wipe_log_files};
use db::DBOperator;

mod utils;

fn criterion_benchmark(c: &mut Criterion) {
    init_benchmark();
    wipe_log_files();
    let mut engine = Storage::new().unwrap();

    let true_entries = {
        let mut collection = engine
            .create_new_collection("table")
            .unwrap()
            .write()
            .unwrap();

        // Setup
        let data = generate_sample_data(10_000);
        let filtered = data.iter().filter(|d| d.fields.get_field(3) == ObjectField::Bool(true)).count();

        collection.set_objects(Uuid::nil(), data).unwrap();
        collection.print_debug_info();
        collection.clear_cache();
        filtered
    };

    c.bench_function("10K collection iteration with filter", |b| {
        b.iter(|| {
            let collection = engine
                .get_collection("table")
                .unwrap()
                .read()
                .unwrap();

            let data: DBResult<Vec<Row>> = collection
                .table_scan(Uuid::now_v7())
                .filter(|row| row.get_field(3).as_bool().unwrap())
                .collect();
            let data = data.unwrap();
            assert_eq!(data.len(), true_entries);
            std::mem::drop(data);
        });
    });
}

criterion_group!{
    name = collection_iteration_series;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(collection_iteration_series);

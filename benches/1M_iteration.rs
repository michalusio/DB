use std::{time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use db::{DBOperator, DBResult, Row, Storage};
use uuid::Uuid;

use crate::utils::{generate_sample_data, init_benchmark, wipe_log_files};

mod utils;

fn criterion_benchmark(c: &mut Criterion) {
    init_benchmark();
    wipe_log_files();
    let mut engine = Storage::new().unwrap();

    {
        let mut collection = engine
            .create_new_collection("table")
            .unwrap()
            .write()
            .unwrap();

        // Setup
        let data = generate_sample_data(1_000_000);
        collection.set_objects(Uuid::nil(), data).unwrap();
        collection.print_debug_info();
        collection.clear_cache();
    }

    c.bench_function("1M collection iteration", |b| {
        b.iter(|| {
            let collection = engine
                .get_collection("table")
                .unwrap()
                .read()
                .unwrap();

            let data: DBResult<Vec<Row>> = collection.table_scan(Uuid::now_v7()).collect();
            let data = data.unwrap();
            assert_eq!(data.len(), 1_000_000);
            std::mem::drop(data);
        });
    });
}

criterion_group!{
    name = big_collection_iteration;
    config = Criterion::default().measurement_time(Duration::from_secs(10)).sample_size(15);
    targets = criterion_benchmark
}
criterion_main!(big_collection_iteration);

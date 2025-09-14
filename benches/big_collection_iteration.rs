use std::{borrow::Cow, sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use db::{DBResult, ObjectField, Storage};
use serde::Deserialize;
use uuid::Uuid;

use crate::utils::{wipe_log_files, generate_sample_data};

mod utils;

fn criterion_benchmark(c: &mut Criterion) {
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
        collection.clear_cache();
    }

    #[derive(Deserialize)]
    struct TestStruct {
        _a: Cow<'static, str>,
        _b: i32,
        _c: f64,
        _d: bool,
        _e: Cow<'static, str>
    }

    c.bench_function("big collection iteration (one million entries)", |b| {
        b.iter(|| {
            let collection = engine
                .get_collection("table")
                .unwrap()
                .read()
                .unwrap();

            let data: DBResult<Vec<(Uuid, Arc<[ObjectField]>)>> = collection.iterate_native().collect();
            let data = data.unwrap();
            assert_eq!(data.len(), 1_000_000);
            std::mem::drop(data);
        });
    });

    c.bench_function("big collection iteration (one million entries) with deserialization", |b| {
        b.iter(|| {
            let collection = engine
                .get_collection("table")
                .unwrap()
                .read()
                .unwrap();

            let data: DBResult<Vec<TestStruct>> = collection.iterate::<TestStruct>().collect();
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
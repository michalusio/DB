use std::sync::Arc;

use bumpalo::Bump;
use criterion::{criterion_group, criterion_main, Criterion};
use db::{Storage, DBResult};
use serde::Deserialize;

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
        let arena = Bump::new();
        let data = generate_sample_data(1_000_000, &arena);
        collection.set_objects(data.into_iter()).unwrap();
        collection.print_debug_info();
    }

    #[derive(Deserialize)]
    struct TestStruct {
        _a: String,
        _b: i32,
        _c: f64,
        _d: bool,
        _e: String
    }

    c.bench_function("big collection iteration", |b| {
        b.iter(|| {
            let collection = engine
                .get_collection("table")
                .unwrap()
                .read()
                .unwrap();
            let collection = Arc::new(collection);

            let data: DBResult<Vec<TestStruct>> = collection.iterate::<TestStruct>().collect();
            let data = data.unwrap();
            std::mem::drop(data);
        });
    });
}

criterion_group!(big_collection_iteration, criterion_benchmark);
criterion_main!(big_collection_iteration);
use criterion::{criterion_group, criterion_main, Criterion};
use db::{DBResult, ObjectField, Storage};
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
        let data = generate_sample_data(10_000);
        collection.set_objects(Uuid::nil(), data).unwrap();
        collection.print_debug_info();
        collection.clear_cache();
    }

    c.bench_function("10K collection iteration", |b| {
        b.iter(|| {
            let collection = engine
                .get_collection("table")
                .unwrap()
                .read()
                .unwrap();

            let data: DBResult<Vec<(Uuid, Vec<ObjectField>)>> = collection.iterate_native(Uuid::now_v7()).collect();
            let data = data.unwrap();
            assert_eq!(data.len(), 10_000);
            std::mem::drop(data);
        });
    });
}

criterion_group!{
    name = collection_iteration;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(collection_iteration);
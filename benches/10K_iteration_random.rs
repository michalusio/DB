use std::{borrow::Cow};

use criterion::{criterion_group, criterion_main, Criterion};
use db::{DBOperator, DBResult, ObjectField, Row, Storage};
use serde::Deserialize;
use uuid::Uuid;
use fakeit::{name};

use crate::utils::{init_benchmark, wipe_log_files};

mod utils;

pub fn generate_sample_data(n: u32) -> Vec<Row> {
    let data: Vec<Row> = (0..n)
    .map(|_| {
        let id = Uuid::new_v4();
        let first = name::first();
        let last = name::last();
        
        let state = vec![
            first.into(),
            last.into(),
            ObjectField::I32(rand::random()),
            ObjectField::Decimal(rand::random::<f64>() * 1000.0),
            ObjectField::Bool(rand::random()),
            ObjectField::Id(Uuid::new_v4()),
            ObjectField::I32(rand::random()),
            ObjectField::I32(rand::random()),
            ObjectField::I32(rand::random()),
        ];
        Row { id, fields: state.into() }
    })
    .collect();
    data
}

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
        let data = generate_sample_data(10_000);
        collection.set_objects(Uuid::nil(), data).unwrap();
        collection.print_debug_info();
        collection.clear_cache();
    }

    #[derive(Deserialize)]
    struct TestStruct<'a> {
        _a: Cow<'a, str>,
        _b: Cow<'a, str>,
        _c: i32,
        _d: f64,
        _e: bool,
        _f: Uuid,
        _g: i32,
        _h: i32,
        _i: i32
    }

    c.bench_function("10K collection iteration with a lot of random columns", |b| {
        b.iter(|| {
            let collection = engine
                .get_collection("table")
                .unwrap()
                .read()
                .unwrap();

            let data: DBResult<Vec<TestStruct>> = collection
                .table_scan(Uuid::now_v7())
                .deserialize::<TestStruct>()
                .collect();
            let data = data.unwrap();
            assert_eq!(data.len(), 10_000);
            std::mem::drop(data);
        });
    });
}

criterion_group!{
    name = collection_iteration_random;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(collection_iteration_random);

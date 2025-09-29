use criterion::{criterion_group, criterion_main, Criterion};
use db::{DBOperator, ObjectField, Row, SortDirection, Storage};
use uuid::Uuid;
use fakeit::{address::country, name};

use crate::utils::{init_benchmark, wipe_log_files};

mod utils;

fn criterion_benchmark(c: &mut Criterion) {
    init_benchmark();
    wipe_log_files();
    let mut engine = Storage::new().unwrap();

    {
        pub fn generate_sample_data_1() -> Vec<Row> {
            let data: Vec<Row> = (0..10000)
            .map(|_| {
                let id = Uuid::new_v4();
                let fields: Vec<ObjectField> = vec![
                    ObjectField::I32(rand::random()),
                    name::first().into(),
                    name::last().into(),
                    ObjectField::I32(rand::random_range(0..100)),
                    ObjectField::I32(rand::random_range(0..500)),
                    ObjectField::Decimal(rand::random::<f64>() * 1000f64),
                    ObjectField::Bool(rand::random()),
                ];
                Row { id, fields: fields.into() }
            })
            .collect();
            data
        }

        pub fn generate_sample_data_2() -> Vec<Row> {
            let data: Vec<Row> = (0..100)
            .map(|i| {
                let id = Uuid::new_v4();
                let fields: Vec<ObjectField> = vec![
                    ObjectField::I32(i),
                    country().into(),
                ];
                Row { id, fields: fields.into() }
            })
            .collect();
            data
        }

        {
            let mut collection1 = engine
                .create_new_collection("table")
                .unwrap()
                .write()
                .unwrap();
            
            collection1.set_objects(Uuid::nil(), generate_sample_data_1()).unwrap();
            collection1.print_debug_info();
            collection1.clear_cache();
        }
        {
            let mut collection2 = engine
                .create_new_collection("table2")
                .unwrap()
                .write()
                .unwrap();
            collection2.set_objects(Uuid::nil(), generate_sample_data_2()).unwrap();
            collection2.print_debug_info();
            collection2.clear_cache();
        }
    }

    c.bench_function("10K table hash match iteration with 100 rows table - then sorted on one of the fields", |b| {
        b.iter(|| {
            let transaction = Uuid::now_v7();

            let table1 = engine.get_collection("table")
                .unwrap()
                .read()
                .unwrap();

            let table2 = engine.get_collection("table2")
                .unwrap()
                .read()
                .unwrap();

            let rows = table1
                .table_scan(transaction)
                .hash_match(
                    table2.table_scan(transaction),
                    |row| row.column(3).as_i32().unwrap(),
                    |hashed_row| hashed_row.column(0).as_i32().unwrap()
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

            assert_eq!(rows.len(), 10000);
            std::mem::drop(rows);
        });
    });
}

criterion_group!{
    name = collection_hash_match;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(collection_hash_match);

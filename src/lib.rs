#[warn(clippy::pedantic)]
mod errors;
mod utils;
mod objects;
mod storage;
mod collection;
mod operators;
mod transaction;
mod query;

pub use storage::{Storage};
pub use objects::{ObjectField};
pub use utils::DBResult;
pub use storage::log_file::entry_fields::EntryFields;
pub use storage::log_file::log_entry::Row;

pub use operators::*;

#[cfg(test)]
mod tests {
    use crate::{DBOperator, ObjectField, Row};
    use std::{borrow::Cow, fs, time::{Duration, Instant}};

    use fakeit::{address::country, name};
    use log::{info};
    use serde::Deserialize;
    use serial_test::serial;
    use uuid::Uuid;

    use crate::{storage::Storage, utils::DBResult};

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1"); }
        colog::basic_builder()
            .filter_level(log::LevelFilter::Debug)
            .default_format()
            .init();
    }

    fn wipe_log_files() {
        fs::remove_dir_all("./logfile").unwrap();
        fs::create_dir("./logfile").unwrap();
    }

    fn generate_sample_data() -> (Vec<Row>, Vec<Uuid>) {
        let data: Vec<_> = (1..10000)
        .map(|_| {
            let id = Uuid::new_v4();
            let state: Vec<ObjectField<'_>> = vec![
                "Michał".into(),
                26.into(),
                654.645.into()
            ];
            Row { id, fields: state.into() }
        })
        .collect();
        let chosen_ids: Vec<_> = data
            .iter()
            .step_by(10)
            .map(|d| d.id)
            .collect();
        (data, chosen_ids)
    }

    #[test]
    #[serial(engine)]
    fn save_speed_test() {
        wipe_log_files();
        let mut engine = Storage::new().unwrap();
        let mut collection = engine
            .create_new_collection("table")
            .unwrap()
            .write()
            .unwrap();

        let mut duration = Duration::default();
        
        for _ in 1..1000 {
            let id = Uuid::new_v4();
            let state: Vec<ObjectField> = vec![
                "Michał".into(),
                26.into(),
                654.645.into()
            ];
            
            let instant = Instant::now();
            collection.set_object(Uuid::nil(), id, state.into()).unwrap();
            duration += Instant::now() - instant;
        }
        info!("Duration of save speed test: {:#?}", duration);
    }

    #[test]
    #[serial(engine)]
    fn save_multiple_speed_test() {
        wipe_log_files();
        let mut engine = Storage::new().unwrap();
        let mut collection = engine
            .create_new_collection("table")
            .unwrap()
            .write()
            .unwrap();

        let data: Vec<_> = (1..10000).map(|_| {
            let id = Uuid::new_v4();
            let state: Vec<ObjectField> = vec![
                "Michał".into(),
                26.into(),
                654.645.into()
            ];
            Row { id, fields: state.into() }
        }).collect();
        let instant = Instant::now();
        collection.set_objects(Uuid::nil(), data).unwrap();
        info!("Duration of save multiple speed test: {:#?}", Instant::now() - instant);
    }

    #[test]
    #[serial(engine)]
    fn save_multiple_tables_speed_test() {
        wipe_log_files();
        let mut engine = Storage::new().unwrap();
        let instant = Instant::now();
        let id = Uuid::new_v4();

        let state: Vec<ObjectField> = vec![
            "Michał".into(),
            26.into(),
            654.645.into()
        ];
        {
            let mut collection1 = engine
                .create_new_collection("table")
                .unwrap()
                .write()
                .unwrap();
            
            collection1.set_object(Uuid::nil(), id, state.clone().into()).unwrap();
        }
        {
            let mut collection2 = engine
                .create_new_collection("table2")
                .unwrap()
                .write()
                .unwrap();
            collection2.set_object(Uuid::nil(), id, state.into()).unwrap();
        }
        info!("Duration of save multiple tables speed test: {:#?}", Instant::now() - instant);
    }

    #[test]
    #[serial(engine)]
    fn collection_iteration() {
        wipe_log_files();
        let mut engine = Storage::new().unwrap();
        let mut collection = engine
            .create_new_collection("table")
            .unwrap()
            .write()
            .unwrap();

        {
            // Setup
            let (data, _ids) = generate_sample_data();
            info!("Inserting {} objects", collection.set_objects(Uuid::nil(), data.into_iter()).unwrap());
        }
    
        #[derive(Deserialize)]
        struct TestStruct<'a> {
            a: Cow<'a, str>,
            b: i32,
            c: f64
        }

        // Test
        info!("Beginning the test");
        let instant = Instant::now();
        let data: DBResult<Vec<TestStruct>> = collection
            .table_scan(Uuid::now_v7())
            .deserialize::<TestStruct>()
            .collect();
        let data = data.unwrap();
        assert_eq!(data.len(), 9999);
        info!("Duration of collection iteration: {:#?}", Instant::now() - instant);
        collection.print_debug_info();
    }

    #[test]
    #[serial(engine)]
    fn nested_loop_test() {
        wipe_log_files();
        let mut engine = Storage::new().unwrap();
        let instant = Instant::now();

        pub fn generate_sample_data_1() -> Vec<Row> {
            let data: Vec<Row> = (0..10000)
            .map(|_| {
                let id = Uuid::new_v4();
                let fields: Vec<ObjectField> = vec![
                    ObjectField::I32(rand::random()),
                    name::first().into(),
                    name::last().into(),
                    ObjectField::I32(rand::random_range(0..100)),
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
        }
        {
            let mut collection2 = engine
                .create_new_collection("table2")
                .unwrap()
                .write()
                .unwrap();
            collection2.set_objects(Uuid::nil(), generate_sample_data_2()).unwrap();
        }

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
            .nested_loop(table2.table_scan(transaction), 3, 0)
            .collect()
            .unwrap();

        assert_eq!(rows.len(), 10000);

        info!("Duration of nested loop test: {:#?}", Instant::now() - instant);
    }

}

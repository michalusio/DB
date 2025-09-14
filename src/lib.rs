#[warn(clippy::pedantic)]
mod errors;
mod objects;
mod storage;
mod utils;

pub use storage::{Storage, collection::Collection};
pub use objects::{ObjectField};
pub use utils::DBResult;

#[cfg(test)]
mod tests {
    use crate::{ObjectField};
    use std::{fs, sync::Arc, time::{Duration, Instant}};

    use serde::Deserialize;
    use serial_test::serial;
    use uuid::Uuid;

    use crate::{storage::Storage, utils::DBResult};

    fn wipe_log_files() {
        fs::remove_dir_all("./logfile").unwrap();
        fs::create_dir("./logfile").unwrap();
    }

    fn generate_sample_data() -> (Vec<(Uuid, Arc<[ObjectField]>)>, Vec<Uuid>) {
        let data: Vec<_> = (1..10000)
        .map(|_| {
            let id = Uuid::new_v4();
            let state: Arc<[ObjectField]> = vec![
                ObjectField::String("Michał".into()),
                ObjectField::I32(26),
                ObjectField::Decimal(654.645)
            ].into_boxed_slice().into();
            (id, state)
        })
        .collect();
        let chosen_ids: Vec<_> = data
            .iter()
            .step_by(10)
            .map(|d| d.0)
            .collect();
        (data, chosen_ids)
    }

    #[test]
    #[serial]
    fn save_speed_test() {
        wipe_log_files();
        let mut engine = Storage::new().unwrap();
        let mut collection = engine
            .create_new_collection("table")
            .unwrap()
            .write()
            .unwrap();

        let mut duration = Duration::default();
        for _ in 1..10000 {
            let id = Uuid::new_v4();
            let state: Arc<[ObjectField]> = vec![
                ObjectField::String("Michał".into()),
                ObjectField::I32(26),
                ObjectField::Decimal(654.645)
            ].into_boxed_slice().into();
            let instant = Instant::now();
            collection.set_object(Uuid::nil(), id, state).unwrap();
            duration += Instant::now() - instant;
        }
        println!("Duration of save speed test: {:#?}", duration);
    }

    #[test]
    #[serial]
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
            let state: Arc<[ObjectField]> = vec![
                ObjectField::String("Michał".into()),
                ObjectField::I32(26),
                ObjectField::Decimal(654.645)
            ].into_boxed_slice().into();
            (id, state)
        }).collect();
        let instant = Instant::now();
        collection.set_objects(Uuid::nil(), data.into_iter()).unwrap();
        println!("Duration of save multiple speed test: {:#?}", Instant::now() - instant);
    }

    #[test]
    #[serial]
    fn save_multiple_tables_speed_test() {
        wipe_log_files();
        let mut engine = Storage::new().unwrap();
        let instant = Instant::now();
        let id = Uuid::new_v4();

        let state: Arc<[ObjectField]> = vec![
            ObjectField::String("Michał".into()),
            ObjectField::I32(26),
            ObjectField::Decimal(654.645)
        ].into_boxed_slice().into();
        {
            let mut collection1 = engine
                .create_new_collection("table")
                .unwrap()
                .write()
                .unwrap();
            
            collection1.set_object(Uuid::nil(), id, state.clone()).unwrap();
        }
        {
            let mut collection2 = engine
                .create_new_collection("table2")
                .unwrap()
                .write()
                .unwrap();
            collection2.set_object(Uuid::nil(), id, state).unwrap();
        }
        println!("Duration of save multiple tables speed test: {:#?}", Instant::now() - instant);
    }

    #[test]
    #[serial]
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
            println!("Inserting {} objects", collection.set_objects(Uuid::nil(), data.into_iter()).unwrap());
        }
    
        #[derive(Deserialize)]
        struct TestStruct {
            a: String,
            b: i32,
            c: f64
        }

        // Test
        println!("Beginning the test");
        let instant = Instant::now();
        let data: DBResult<Vec<TestStruct>> = collection.iterate::<TestStruct>().collect();
        let data = data.unwrap();
        assert_eq!(data.len(), 9999);
        println!("Duration of collection iteration: {:#?}", Instant::now() - instant);
        collection.print_debug_info();
    }

}
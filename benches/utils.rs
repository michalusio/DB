use std::{fs};
use db::{ObjectField, Row};
use uuid::Uuid;
use fakeit::name;

pub fn wipe_log_files() {
    let _ = fs::remove_dir_all("./logfile");
    let _ = fs::create_dir("./logfile");
}

pub fn init_benchmark() {
    unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    colog::basic_builder()
        .filter_level(log::LevelFilter::Debug)
        .default_format()
        .init();
}

pub fn generate_sample_data(n: u32) -> Vec<Row> {
    let data: Vec<Row> = (0..n)
    .map(|_| {
        let id = Uuid::new_v4();
        let state: Vec<ObjectField> = vec![
            name::first().into(),
            ObjectField::I32(rand::random()),
            ObjectField::Decimal(rand::random::<f64>() * 1000f64),
            ObjectField::Bool(rand::random()),
            name::last().into()
        ];
        Row { id, fields: state.into() }
    })
    .collect();
    data
}

use std::{fs};

use db::{ObjectField};
use uuid::Uuid;

pub fn wipe_log_files() {
    fs::remove_dir_all("./logfile").unwrap();
    fs::create_dir("./logfile").unwrap();
}

const STRINGS: [&str; 6] = [
    "Michał",
    "Robert",
    "Marcin",
    "Jakub",
    "Mieczysław",
    "Jormungander"
];

pub fn generate_sample_data(n: u32) -> Vec<(Uuid, Vec<ObjectField>)> {
    let data: Vec<_> = (0..n)
    .map(|_| {
        let id = Uuid::new_v4();
        let state = vec![
            ObjectField::String(STRINGS[(rand::random::<u64>() % STRINGS.len() as u64) as usize].into()),
            ObjectField::I32(rand::random()),
            ObjectField::Decimal(rand::random::<f64>() * 1000f64),
            ObjectField::Bool(rand::random()),
            ObjectField::String(STRINGS[(rand::random::<u64>() % STRINGS.len() as u64) as usize].into())
        ];
        (id, state)
    })
    .collect();
    data
}
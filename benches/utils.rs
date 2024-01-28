use std::fs;

use bumpalo::{Bump, vec};
use db::{ObjectState, ObjectField};
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

pub fn generate_sample_data(n: u32, arena: &Bump) -> Vec<(Uuid, ObjectState)> {
    let data: Vec<_> = (0..n)
    .map(|_| {
        let id = Uuid::new_v4();
        let state = ObjectState::ObjectValues(
            vec![in arena;
                ObjectField::String(STRINGS[rand::random::<usize>() % STRINGS.len()]),
                ObjectField::I32(rand::random()),
                ObjectField::Decimal(rand::random::<f64>() * 1000f64),
                ObjectField::Bool(rand::random()),
                ObjectField::String(STRINGS[rand::random::<usize>() % STRINGS.len()])
            ]
        );
        (id, state)
    })
    .collect();
    data
}
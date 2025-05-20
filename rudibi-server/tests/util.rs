
use rudibi_server::engine::*;

pub fn fruits_schema() -> TableSchema {
    TableSchema::new("Fruits",
        vec![
            ColumnSchema::new("id", DataType::U32),
            ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 }),
        ]
    )
}

pub fn fruits_table(storage: StorageConfig) -> Database {
    let mut db = Database::new();
    db.in_mem(&fruits_schema()).unwrap();

    let rows = vec![
        (100u32, "apple"),
        (200u32, "banana"),
        (300u32, "banana"),
        (400u32, "cherry"),
    ];

    for (id, name) in rows {
        let row = StoredRow::of_columns(&[&id.to_le_bytes(), name.as_bytes()]);
        db.store(StoreCommand::new("Fruits", &["id", "name"], vec![row])).unwrap();
    }

    return db;
}

pub fn empty_table(storage: StorageConfig) -> Database {
    let mut db = Database::new();
    db.in_mem(&TableSchema::new("EmptyTable", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();
    return db;
}


// Fuck you, Rust, I won't be using a dependency just to generate a random number 
use std::{fs, env};
pub fn random_temp_file() -> String {
    let mut num = 0;
    let tmp = env::temp_dir();
    loop {
        let fname = format!("{}/test_{}.db", tmp.display(), num);
        if !fs::exists(&fname).unwrap() {
            return fname;
        }
        num += 1;
    }
}
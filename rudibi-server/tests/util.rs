
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



use std::{env, fs::{self, File}};

// A retarted way to obtain a new random file for testing purposes
// Fuck you, Rust, I won't be using a dependency just to create a file.
// The commonly used "tempfile" also tries to make a new file in a loop, this isn't that much worse!
// https://github.com/Stebalien/tempfile/blob/99ffea61ade621161db326b6745c7b36a90ddbd0/src/util.rs#L40
// FIXME: There should be at least *some* randomness in generating a new filename
pub fn random_temp_file() -> String {
    let mut num = 0;
    let tmp = env::temp_dir();
    let new_file = loop {
        let fname = format!("{}/test_{}.db", tmp.display(), num);
        match File::create_new(fname.clone()) {
            Ok(_) => {
                break fname;
            }
            Err(_) => (),
        }
        num += 1;
    };
    fs::remove_file(new_file.clone()).unwrap();
    new_file
}
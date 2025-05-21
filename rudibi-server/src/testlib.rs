
use crate::engine::*;


pub fn fruits_schema() -> TableSchema {
    TableSchema::new("Fruits",
        vec![
            ColumnSchema::new("id", DataType::U32),
            ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 }),
        ]
    )
}

trait Store<'a> : Sized {
    fn to_storable(&'a self) -> &'a [u8];
}

impl<'a> Store<'a> for u32 {
    fn to_storable(&'a self) -> &'a [u8] {
        unsafe {
            // Rust dark "unsafe" magic just to be able to view u32 as a byte ptr 
            // (u32::to_le_bytes makes a copy)
            // FIXME: Will this fail on big endian systems?
            std::slice::from_raw_parts(self as *const u32 as *const u8, std::mem::size_of::<u32>())
        }
    }
}

#[test]
fn storable_u32_is_le_bytes() {
    let val = 100u32;
    assert_eq!(&val.to_le_bytes(), val.to_storable());
}

impl <'a> Store<'a> for &'a str {
    fn to_storable(&'a self) -> &'a [u8] {
        str::as_bytes(self)
    }
}

macro_rules! rows {
    ($([$($x:expr),+ $(,)?]),* $(,)?) => {
        vec![
            $(StoredRow::of_columns(&[$($x.to_storable()),+])),*
        ]
    };
}


pub fn fruits_table(storage: StorageConfig) -> Database {
    let mut db = Database::new();
    db.new_table(&fruits_schema(), storage).unwrap();

    let rows = rows![
        [100u32, "apple"],
        [200u32, "banana"],
        [300u32, "banana"],
        [400u32, "cherry"]
    ];

    db.store(StoreCommand::new("Fruits", &["id", "name"], rows)).unwrap();

    return db;
}

pub fn empty_table(storage: StorageConfig) -> Database {
    let mut db = Database::new();
    db.in_mem(&TableSchema::new("EmptyTable", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();
    return db;
}

use std::{env, slice};
use std::fs::File;
use std::time::{SystemTime, UNIX_EPOCH};

// A retarted way to obtain a new random file for testing purposes
// Fuck you, Rust, I won't be using a dependency just to create a file.
// The commonly used "tempfile" also tries to make a new file in a loop, this isn't that much worse!
// https://github.com/Stebalien/tempfile/blob/99ffea61ade621161db326b6745c7b36a90ddbd0/src/util.rs#L40
pub fn random_temp_file() -> String {
    let tmp = env::temp_dir();
    let new_file = loop {
        let unix_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let fname = format!("{}/test_{}.db", tmp.display(), unix_timestamp.as_nanos());
        match File::create_new(fname.clone()) {
            Ok(_) => {
                break fname;
            }
            Err(_) => (),
        }
    };
    new_file
}
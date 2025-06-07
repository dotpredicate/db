
use crate::dtype::*;
use crate::engine::*;

pub fn get_column_value<'schema, 'row>(schema: &'schema Table, row: &'row Row, col_idx: usize) -> ColumnValue<'row> {
    let col_scheme = &schema.column_layout[col_idx];
    canonical_column(&col_scheme.dtype, row.get_column(col_idx)).unwrap()
}

pub fn fruits_schema() -> Table {
    Table::new("Fruits",
        vec![
            Column::new("id", DataType::U32),
            Column::new("name", DataType::UTF8 { max_bytes: 20 }),
        ]
    )
}

#[macro_export]
macro_rules! rows {
    ($([$($x:expr),+ $(,)?]),* $(,)?) => {
        &[
            $( Row::of_columns(&[$( $crate::serial::Serializable::serialized(&$x) ),+]) ),*
        ]
    };
}

#[macro_export]
macro_rules! assert_rows {
    () => {
        
    };
}

pub fn fruits_table(storage: StorageCfg) -> Database {
    let mut db = Database::new();
    db.new_table(&fruits_schema(), storage).unwrap();

    let rows = rows![
        [100u32, "apple"],
        [200u32, "banana"],
        [300u32, "banana"],
        [400u32, "cherry"]
    ];

    db.insert("Fruits", &["id", "name"], rows).unwrap();

    return db;
}

pub fn empty_table(storage: StorageCfg) -> Database {
    let mut db = Database::new();
    db.new_table(&Table::new("EmptyTable", vec![Column::new("id", DataType::U32)]), storage).unwrap();
    return db;
}

use std::env;
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
        let fname = format!("{}/test_{}", tmp.display(), unix_timestamp.as_nanos());
        match File::create_new(fname.clone()) {
            Ok(_) => {
                // println!("Created new file {}", fname);
                break fname;
            }
            Err(_) => (),
        }
    };
    new_file
}

pub fn with_tmp(fun: fn(StorageCfg)) {
    let file_path =  random_temp_file();
    fun(StorageCfg::Disk { path: file_path.clone() });
    std::fs::remove_file(file_path).unwrap();
}
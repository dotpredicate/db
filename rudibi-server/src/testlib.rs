
use crate::dtype::*;
use crate::engine::*;

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

pub fn check_equality<const COLS: usize>(results: &ResultSet, expected: &[[ColumnValue; COLS]]) {
    assert_eq!(results.data.len(), expected.len());
    for (row_idx, (expected_row, result_row)) in expected.iter().zip(results.data.iter()).enumerate() {
        assert_eq!(result_row.offsets.len() - 1, COLS);
        for col_idx in 0..COLS {
            let expected_col = expected_row[col_idx];
            let result_col_raw = result_row.get_column(col_idx);
            let result_col_schema = &results.schema[col_idx];
            let result_col_canonical = canonical_column(&result_col_schema.dtype, &result_col_raw).unwrap();
            assert_eq!(result_col_canonical, expected_col, "Column {} ({}) at row {} not equal", col_idx, result_col_schema.name, row_idx);
        }
    }
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

use rudibi_server::dtype::{ColumnValue::*, DataType};
use rudibi_server::engine::{Database, Table, Column, Row, StorageCfg, DbError};
use rudibi_server::query::{Bool::*, Value::*};
use rudibi_server::testlib::{empty_table, fruits_schema, check_equality, with_tmp};
use rudibi_server::rows;

#[test]
fn store_unknown_table() {
    let mut db = Database::new();
    let result = db.insert("UnknownTable", &["id"], rows![]);
    assert_eq!(result, Err(DbError::TableNotFound("UnknownTable".to_string())));
}

fn store_nothing(storage: StorageCfg) {
    let mut db = empty_table(storage);
    let result = db.insert("EmptyTable", &["id"], rows![]);
    assert!(matches!(result, Ok(0)));
}

#[test]
fn store_nothing_in_mem() {
    store_nothing(StorageCfg::InMemory);
}

#[test]
fn store_nothing_on_disk() {
    with_tmp(store_nothing);
}


fn test_all_data_types(storage: StorageCfg) {
    let mut db = Database::new();
    db.new_table(&Table::new("MixedTypes",
        vec![
            Column::new("int", DataType::U32),
            Column::new("float", DataType::F64),
            Column::new("text", DataType::UTF8 { max_bytes: 10 }),
            Column::new("binary", DataType::VARBINARY { max_length: 5 }),
            Column::new("buffer", DataType::BUFFER { length: 3 }),
        ]
    ), storage).unwrap();

    let rows = rows![
        [ 42u32, 3.14f64, "hello", [0x01, 0x02, 0x03, 0x04, 0x05], [0xAA, 0xBB, 0xCC]]
    ];

    let result = db.insert("MixedTypes", &["int", "float", "text", "binary", "buffer"], rows);
    assert!(result.is_ok(), "{result:#?}");

    let results = db.select(&[ColumnRef("int"), ColumnRef("float"), ColumnRef("text"), ColumnRef("binary"), ColumnRef("buffer")], "MixedTypes", &True).unwrap();
    check_equality(&results, &[
        [U32(42), F64(3.14), UTF8("hello"), Bytes(&[0x01, 0x02, 0x03, 0x04, 0x05]), Bytes(&[0xAA, 0xBB, 0xCC])]
    ]);
}

#[test]
fn test_all_data_types_in_mem() {
    test_all_data_types(StorageCfg::InMemory);
}

#[test]
fn test_all_data_types_on_disk() {
    with_tmp(test_all_data_types);
}


fn test_column_size_limits(storage: StorageCfg) {
    let mut db = Database::new();
    db.new_table(&Table::new("SizeTest",
        vec![
            Column::new("utf8", DataType::UTF8 { max_bytes: 5 }),
            Column::new("varbinary", DataType::VARBINARY { max_length: 5 }),
            Column::new("buffer", DataType::BUFFER { length: 3 }),
        ]
    ), storage).unwrap();

    // Test valid sizes
    let utf8_val = "abc".as_bytes().to_vec(); // 3 bytes, within 0-5
    let varbinary_val = vec![1, 2, 3, 4, 5]; // 5 bytes, at max
    let buffer_val = vec![6, 7, 8]; // 3 bytes, exact length
    let rows = rows![[utf8_val, varbinary_val, buffer_val]];
    let result = db.insert("SizeTest", &["utf8", "varbinary", "buffer"], rows);
    assert!(result.is_ok(), "{result:#?}");

    // Test invalid size (varbinary too long)
    let invalid_varbinary = vec![1, 2, 3, 4, 5, 6]; // 6 bytes, exceeds max_length 5
    let invalid_rows = rows![[utf8_val, invalid_varbinary, buffer_val]];

    let result = db.insert("SizeTest", &["utf8", "varbinary", "buffer"], invalid_rows);
    assert_eq!(result, Err(DbError::ColumnSizeOutOfBounds { column: "varbinary".into(), got: 6, min: 0, max: 5 }), "{result:#?}");

    // Test invalid size (buffer too short)
    let short_buffer = vec![1, 2]; // 2 bytes, less than length 3
    let short_row = rows![[utf8_val, varbinary_val, short_buffer]];
    let result = db.insert("SizeTest", &["utf8", "varbinary", "buffer"], short_row);
    assert_eq!(result, Err(DbError::ColumnSizeOutOfBounds { column: "buffer".into(), got: 2, min: 3, max: 3 }));
}

#[test]
fn test_column_size_limits_in_mem() {
    test_column_size_limits(StorageCfg::InMemory);
}

#[test]
fn test_column_size_limits_on_disk() {
    with_tmp(test_column_size_limits);
}

fn test_out_of_order_store(storage: StorageCfg) {
    // GIVEN
    let mut db = Database::new();
    db.new_table(&fruits_schema(), storage).unwrap();

    // WHEN
    db.insert("Fruits", &["name", "id"], rows![ 
        ["banana", 100u32],
        ["apple", 200u32]
    ]).unwrap();

    // THEN
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &True).unwrap();
    check_equality(&results, &[
        [U32(100), UTF8("banana")],
        [U32(200), UTF8("apple")]
    ]);
}

#[test]
fn test_out_of_order_store_in_mem() {
    test_out_of_order_store(StorageCfg::InMemory);
}

#[test]
fn test_out_of_order_store_on_disk() {
    with_tmp(test_out_of_order_store);
}
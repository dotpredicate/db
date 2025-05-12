
mod util;
use rudibi_server::engine::*;

#[test]
fn store_unknown_table() {
    let mut db = Database::new();
    let result = db.store(StoreCommand::new("UnknownTable", &["id"], vec![]));
    assert_eq!(result, Err(DatabaseError::TableNotFound("UnknownTable".to_string())));
}

#[test]
fn store_nothing() {
    let mut db = util::empty_table();
    let result = db.store(StoreCommand::new("EmptyTable", &["id"], vec![]));
    assert!(matches!(result, Ok(0)));
}


#[test]
fn test_all_data_types() {
    let mut db = Database::new();
    db.new_table(&TableSchema::new("MixedTypes",
        vec![
            ColumnSchema::new("int", DataType::U32),
            ColumnSchema::new("float", DataType::F64),
            ColumnSchema::new("text", DataType::UTF8 { max_bytes: 10 }),
            ColumnSchema::new("binary", DataType::VARBINARY { max_length: 5 }),
            ColumnSchema::new("buffer", DataType::BUFFER { length: 3 }),
        ]
    )).unwrap();

    let row = StoredRow::of_columns(&[
        &42u32.to_le_bytes(),
        &3.14f64.to_le_bytes(),
        "hello".as_bytes(),
        &[0x01, 0x02, 0x03, 0x04, 0x05],
        &[0xAA, 0xBB, 0xCC],
    ]);

    let result = db.store(StoreCommand::new("MixedTypes", &["int", "float", "text", "binary", "buffer"], vec![row]));
    assert!(result.is_ok(), "{result:#?}");

    let results = db.get(GetCommand::new("MixedTypes", &["int", "float", "text", "binary", "buffer"], vec![])).unwrap();
    assert_eq!(results.len(), 1);
    let row = &results[0];
    let schema = db.schema_for("MixedTypes").unwrap();
    assert!(matches!(db.get_column_value(&schema, &row, 0).unwrap(), ColumnValue::U32(42)));
    assert!(matches!(db.get_column_value(&schema, row, 1).unwrap(), ColumnValue::F64(3.14)));
    assert!(matches!(db.get_column_value(&schema, &row, 2).unwrap(), ColumnValue::String(ref s) if s == "hello"));
    assert!(matches!(db.get_column_value(&schema, &row, 3).unwrap(), ColumnValue::Bytes(ref v) if v == &[0x01, 0x02, 0x03, 0x04, 0x05]));
    assert!(matches!(db.get_column_value(&schema, row, 4).unwrap(), ColumnValue::Bytes(ref v) if v == &[0xAA, 0xBB, 0xCC]));
}

#[test]
fn test_column_size_limits() {
    let mut db = Database::new();
    db.new_table(&TableSchema::new("SizeTest",
        vec![
            ColumnSchema::new("utf8", DataType::UTF8 { max_bytes: 5 }),
            ColumnSchema::new("varbinary", DataType::VARBINARY { max_length: 5 }),
            ColumnSchema::new("buffer", DataType::BUFFER { length: 3 }),
        ]
    )).unwrap();

    // Test valid sizes
    let utf8_val = "abc".as_bytes().to_vec(); // 3 bytes, within 0-5
    let varbinary_val = vec![1, 2, 3, 4, 5]; // 5 bytes, at max
    let buffer_val = vec![6, 7, 8]; // 3 bytes, exact length
    let row = StoredRow::of_columns(&[&utf8_val, &varbinary_val, &buffer_val]);
    let result = db.store(StoreCommand::new("SizeTest", &["utf8", "varbinary", "buffer"], vec![row]));
    assert!(result.is_ok(), "{result:#?}");

    // Test invalid size (varbinary too long)
    let invalid_varbinary = vec![1, 2, 3, 4, 5, 6]; // 6 bytes, exceeds max_length 5
    let invalid_row = StoredRow::of_columns(&[&utf8_val, &invalid_varbinary, &buffer_val]);

    let result = db.store(StoreCommand::new("SizeTest", &["utf8", "varbinary", "buffer"], vec![invalid_row]));
    assert_eq!(result, Err(DatabaseError::ColumnSizeOutOfBounds { column: "varbinary".into(), got: 6, min: 0, max: 5 }), "{result:#?}");

    // Test invalid size (buffer too short)
    let short_buffer = vec![1, 2]; // 2 bytes, less than length 3
    let short_row = StoredRow::of_columns(&[&utf8_val, &varbinary_val, &short_buffer]);
    let result = db.store(StoreCommand::new("SizeTest", &["utf8", "varbinary", "buffer"], vec![short_row]));
    assert_eq!(result, Err(DatabaseError::ColumnSizeOutOfBounds { column: "buffer".into(), got: 2, min: 3, max: 3 }));
}

#[test]
fn test_out_of_order_store() {
    // GIVEN
    let mut db = Database::new();
    db.new_table(&util::fruits_schema()).unwrap();

    // WHEN
    db.store(StoreCommand::new("Fruits", &["name", "id"], 
        vec![
            StoredRow::of_columns(&["banana".as_bytes(), &100u32.to_le_bytes()]),
            StoredRow::of_columns(&["apple".as_bytes(), &200u32.to_le_bytes()]),
        ]
    )).unwrap();

    // THEN
    let results = db.get(GetCommand::new("Fruits", &["id", "name"], vec![])).unwrap();
    assert_eq!(results.len(), 2);
    let schema = db.schema_for("Fruits").unwrap();
    let names: Vec<String> = results.iter().map(|row| {
        match db.get_column_value(&schema, &row, 1).unwrap() {
            ColumnValue::String(name) => name,
            x => panic!("Expected String, got {:?}", x),
        }
    }).collect();
    assert_eq!(names, vec!["banana", "apple"]);
}
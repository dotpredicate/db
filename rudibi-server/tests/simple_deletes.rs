
use rudibi_server::dtype::ColumnValue;
use rudibi_server::engine::*;
use rudibi_server::testlib;
use rudibi_server::serial::Serializable;

#[test]
fn test_delete_non_existent_table() {
    // GIVEN
    let mut db = Database::new();
    
    // WHEN
    let result = db.delete("NonExistent", &[]);

    // THEN
    assert!(matches!(result, Err(DbError::TableNotFound(ref s)) if s == "NonExistent"));
}


fn test_delete_empty(storage: StorageCfg) {
    // GIVEN
    let mut db = testlib::empty_table(storage);

    // WHEN
    let deleted_count = db.delete("EmptyTable", &[]).unwrap();

    // THEN
    assert_eq!(deleted_count, 0);
}

#[test]
fn test_delete_empty_in_mem() {
    test_delete_empty(StorageCfg::InMemory);
}

#[test]
fn test_delete_empty_on_disk() {
    test_delete_empty(StorageCfg::Disk { path: testlib::random_temp_file() });
}


fn test_delete_with_equality_filter(storage: StorageCfg) {
    // GIVEN
    let mut db = testlib::fruits_table(storage);

    // WHEN
    let deleted_count = db.delete("Fruits",
        &[Filter::Equal { column: "name".into(), value: "banana".as_bytes().to_vec() }],
    ).unwrap();

    // THEN
    assert_eq!(deleted_count, 2);
    let results = db.select("Fruits", &["id", "name"], &[]).unwrap();
    assert_eq!(results.len(), 2);
    let schema = db.schema_for("Fruits").unwrap();
    let names: Vec<String> = results.iter().map(|row| {
        match testlib::get_column_value(&schema, &row, 1) {
            ColumnValue::String(name) => name,
            x => panic!("Expected String, got {:?}", x),
        }
    }).collect();
    assert_eq!(names, vec!["apple", "cherry"]);
}

#[test]
fn test_delete_with_equality_filter_in_mem() {
    test_delete_with_equality_filter(StorageCfg::InMemory);
}

#[test]
fn test_delete_with_equality_filter_on_disk() {
    test_delete_with_equality_filter(StorageCfg::Disk { path: testlib::random_temp_file() });
}


fn test_delete_with_greater_than_filter(storage: StorageCfg) {
    // GIVEN
    let mut db = testlib::fruits_table(storage);

    // WHEN
    let deleted_count = db.delete("Fruits",
        &[Filter::GreaterThan { column: "id".into(), value: 200u32.serialized().to_vec() }],
    ).unwrap();
    
    // THEN
    assert_eq!(deleted_count, 2);
    let results = db.select("Fruits", &["id", "name"], &[]).unwrap();
    assert_eq!(results.len(), 2);
    let schema = db.schema_for("Fruits").unwrap();
    let ids: Vec<u32> = results.iter().map(|row| {
        if let ColumnValue::U32(id) = testlib::get_column_value(&schema, &row, 0) {
            id
        } else {
            panic!("Expected U32");
        }
    }).collect();
    assert_eq!(ids, vec![100, 200]);
}

#[test]
fn test_delete_with_greater_than_filter_in_mem() {
    test_delete_with_greater_than_filter(StorageCfg::InMemory);
}

#[test]
fn test_delete_with_greater_than_filter_on_disk() {
    test_delete_with_greater_than_filter(StorageCfg::Disk { path: testlib::random_temp_file() });
}

fn test_delete_all_rows(storage: StorageCfg) {
    // GIVEN
    let mut db = testlib::fruits_table(storage);

    // WHEN
    let deleted_count = db.delete("Fruits", &[]).unwrap();

    // THEN
    assert_eq!(deleted_count, 4);
    let results = db.select("Fruits", &["id", "name"], &[]).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_delete_all_rows_in_mem() {
    test_delete_all_rows(StorageCfg::InMemory);
}

#[test]
fn test_delete_all_rows_on_disk() {
    test_delete_all_rows(StorageCfg::Disk { path: testlib::random_temp_file() });
}

fn test_delete_with_invalid_column(storage: StorageCfg) {
    // GIVEN
    let mut db = testlib::fruits_table(storage);

    // WHEN
    let result = db.delete("Fruits", &[Filter::Equal { column: "invalid".into(), value: vec![] }]);

    // THEN
    assert!(matches!(result, Err(DbError::ColumnNotFound(ref s)) if s == "invalid"));
}

#[test]
fn test_delete_with_invalid_column_in_mem() {
    test_delete_with_invalid_column(StorageCfg::InMemory);
}

#[test]
fn test_delete_with_invalid_column_on_disk() {
    test_delete_with_invalid_column(StorageCfg::Disk { path: testlib::random_temp_file() });
}

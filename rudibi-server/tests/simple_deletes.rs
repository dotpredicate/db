
use rudibi_server::dtype::ColumnValue::*;
use rudibi_server::engine::*;
use rudibi_server::query::{Bool::*, Value::*};
use rudibi_server::testlib;

#[test]
fn test_delete_non_existent_table() {
    // GIVEN
    let mut db = Database::new();
    
    // WHEN
    let result = db.delete("NonExistent", &True);

    // THEN
    assert!(matches!(result, Err(DbError::TableNotFound(ref s)) if s == "NonExistent"));
}


fn test_delete_empty(storage: StorageCfg) {
    // GIVEN
    let mut db = testlib::empty_table(storage);

    // WHEN
    let deleted_count = db.delete("EmptyTable", &True).unwrap();

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
    let deleted_count = db.delete("Fruits", &Eq(ColumnRef("name"), Const(UTF8("banana")))).unwrap();

    // THEN
    assert_eq!(deleted_count, 2);
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &True).unwrap();
    assert_eq!(results.len(), 2);
    let schema = db.schema_for("Fruits").unwrap();
    let names: Vec<&str> = results.iter().map(|row| {
        match testlib::get_column_value(&schema, &row, 1) {
            UTF8(name) => name,
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
    let deleted_count = db.delete("Fruits", &Gt(ColumnRef("id"), Const(U32(200)))).unwrap();
    
    // THEN
    assert_eq!(deleted_count, 2);
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits",  &True).unwrap();
    assert_eq!(results.len(), 2);
    let schema = db.schema_for("Fruits").unwrap();
    let ids: Vec<u32> = results.iter().map(|row| {
        if let U32(id) = testlib::get_column_value(&schema, &row, 0) {
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
    let deleted_count = db.delete("Fruits", &True).unwrap();

    // THEN
    assert_eq!(deleted_count, 4);
    let results = db.select(&[ColumnRef("id".into())], "Fruits", &True).unwrap();
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
    let result = db.delete("Fruits", &Eq(ColumnRef("invalid"), Const(U32(100))));

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

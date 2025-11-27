
use rudibi_server::dtype::{ColumnValue::*};
use rudibi_server::engine::{Database, StorageCfg, DbError};
use rudibi_server::query::{Bool::*, Value::*};
use rudibi_server::testlib::{empty_table, fruits_table, check_equality, with_tmp};

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
    let mut db = empty_table(storage);

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
    with_tmp(test_delete_empty);
}


fn test_delete_with_equality_filter(storage: StorageCfg) {
    // GIVEN
    let mut db = fruits_table(storage);

    // WHEN
    let deleted_count = db.delete("Fruits", &Eq(ColumnRef("name"), Const(UTF8("banana")))).unwrap();

    // THEN
    assert_eq!(deleted_count, 2);
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &True).unwrap();
    check_equality(&results, &[
        [U32(100), UTF8("apple")],
        [U32(400), UTF8("cherry")]
    ]);
}

#[test]
fn test_delete_with_equality_filter_in_mem() {
    test_delete_with_equality_filter(StorageCfg::InMemory);
}

#[test]
fn test_delete_with_equality_filter_on_disk() {
    with_tmp(test_delete_with_equality_filter);
}


fn test_delete_with_greater_than_filter(storage: StorageCfg) {
    // GIVEN
    let mut db = fruits_table(storage);

    // WHEN
    let deleted_count = db.delete("Fruits", &Gt(ColumnRef("id"), Const(U32(200)))).unwrap();
    
    // THEN
    assert_eq!(deleted_count, 2);
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits",  &True).unwrap();
    check_equality(&results, &[
        [U32(100), UTF8("apple")],
        [U32(200), UTF8("banana")]
    ]);
}

#[test]
fn test_delete_with_greater_than_filter_in_mem() {
    test_delete_with_greater_than_filter(StorageCfg::InMemory);
}

#[test]
fn test_delete_with_greater_than_filter_on_disk() {
    with_tmp(test_delete_with_greater_than_filter);
}

fn test_delete_all_rows(storage: StorageCfg) {
    // GIVEN
    let mut db = fruits_table(storage);

    // WHEN
    let deleted_count = db.delete("Fruits", &True).unwrap();

    // THEN
    assert_eq!(deleted_count, 4);
    let results = db.select(&[ColumnRef("id")], "Fruits", &True).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_delete_all_rows_in_mem() {
    test_delete_all_rows(StorageCfg::InMemory);
}

#[test]
fn test_delete_all_rows_on_disk() {
    with_tmp(test_delete_all_rows);
}

fn test_delete_with_invalid_column(storage: StorageCfg) {
    // GIVEN
    let mut db = fruits_table(storage);

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
    with_tmp(test_delete_with_invalid_column);
}

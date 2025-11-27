
use rudibi_server::dtype::{ColumnValue::*, TypeError};
use rudibi_server::engine::{Database, StorageCfg, DbError};
use rudibi_server::query::{Bool, Bool::*, Value::*};
use rudibi_server::testlib::{fruits_table, check_equality};

#[test]
fn test_equality() {
    // GIVEN
    let db = fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Eq(ColumnRef("name"), Const(UTF8("banana")))).unwrap();
    
    // THEN
    let expected = [
        [U32(200), UTF8("banana")],
        [U32(300), UTF8("banana")],
    ];
    check_equality(&results, &expected);
}

#[test]
fn test_gt() {
    // GIVEN
    let db = fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Gt(ColumnRef("id"), Const(U32(200)))).unwrap();

    // THEN
    let expected = [
        [U32(300), UTF8("banana")],
        [U32(400), UTF8("cherry")]
    ];
    check_equality(&results, &expected);
}

#[test]
fn test_gt_utf8_unsupported() {
    // GIVEN
    let db = fruits_table(StorageCfg::InMemory);

    // WHEN
    let result = db.select(&[ColumnRef("name")], "Fruits", &Gt(ColumnRef("name"), Const(UTF8("banana"))));

    // THEN
    assert!(matches!(result, Err(DbError::QueryError(TypeError::InvalidArgType(_, _, _)))), "{result:#?}");
}

#[test]
fn test_lt() {
    // GIVEN
    let db = fruits_table(StorageCfg::InMemory);

    // Test 3: LessThan filter on U32
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Lt(ColumnRef("id"), Const(U32(200)))).unwrap();
    check_equality(&results, &[[ U32(100), UTF8("apple") ]]);
}


#[test]
fn apply_projection() {
    // GIVEN
    let db = fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select(&[ColumnRef("name")], "Fruits", &Eq(ColumnRef("id"), Const(U32(100)))).unwrap();

    // THEN
    check_equality(&results, &[[ UTF8("apple") ]])
}

#[test]
fn test_multiple_filters() {
    // GIVEN
    let db = fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits", 
        &Bool::and(
            Gt(ColumnRef("id"), Const(U32(100))), 
            Neq(ColumnRef("name"), Const(UTF8("cherry")))
        )
    ).unwrap();

    // THEN
    check_equality(&results, &[
        [U32(200), UTF8("banana")],
        [U32(300), UTF8("banana")]
    ])
}

#[test]
fn test_no_matching_rows() {
    // GIVEN
    let db = fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Eq(ColumnRef("name"), Const(UTF8("orange")))).unwrap();
    
    // THEN
    assert_eq!(results.len(), 0);
}

#[test]
fn test_no_filters() {
    // GIVEN
    let db = fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Bool::True).unwrap();
    
    // THEN
    check_equality(&results, &[
        [U32(100), UTF8("apple")],
        [U32(200), UTF8("banana")],
        [U32(300), UTF8("banana")],
        [U32(400), UTF8("cherry")]
    ]);
}

#[test]
fn test_invalid_column() {
    // GIVEN
    let db = fruits_table(StorageCfg::InMemory);

    // WHEN
    let result = db.select(&[ColumnRef("invalid_column")], "Fruits", &True);

    // THEN
    assert_eq!(result.unwrap_err(), DbError::ColumnNotFound("invalid_column".into()));
}

#[test]
fn test_invalid_table() {
    // GIVEN
    let db = Database::new();

    // WHEN
    let result = db.select(&[ColumnRef("id")], "NonExistent", &True);

    // THEN
    assert_eq!(result.unwrap_err(), DbError::TableNotFound("NonExistent".into()));
}
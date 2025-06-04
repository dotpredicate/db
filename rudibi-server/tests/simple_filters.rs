
use rudibi_server::dtype::{ColumnValue::*, TypeError};
use rudibi_server::engine::*;
use rudibi_server::query::{Bool, Bool::*, Value::*};
use rudibi_server::testlib;
use rudibi_server::serial::Serializable;

#[test]
fn test_equality() {
    // GIVEN
    let db = testlib::fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select_new(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Eq(ColumnRef("name"), Const(UTF8("banana")))).unwrap();
    
    // THEN
    assert_eq!(results.len(), 2);
    let expected = vec![
        (200u32, "banana"),
        (300u32, "banana"),
    ];
    let mut result_pairs: Vec<_> = results.iter()
        .map(|row| {
            let id = u32::from_le_bytes(row.get_column(0).try_into().unwrap());
            let name = String::from_utf8(row.get_column(1).to_vec()).unwrap();
            (id, name)
        })
        .collect();
    result_pairs.sort_by_key(|&(id, _)| id);
    let expected_pairs: Vec<_> = expected.iter()
        .map(|&(id, name)| (id, name.to_string()))
        .collect();
    assert_eq!(result_pairs, expected_pairs);
}

#[test]
fn test_gt() {
    // GIVEN
    let db = testlib::fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select_new(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Gt(ColumnRef("id"), Const(U32(200)))).unwrap();

    // THEN
    let expected_names = vec!["banana", "cherry"];
    let expected_ids = vec![300u32, 400u32];
    assert_eq!(results.len(), 2);
    let mut result_pairs: Vec<_> = results.iter()
        .map(|row| {
            let id = u32::from_le_bytes(row.get_column(0).try_into().unwrap());
            let name = String::from_utf8(row.get_column(1).to_vec()).unwrap();
            (id, name)
        })
        .collect();
    result_pairs.sort_by_key(|&(id, _)| id);
    let expected_pairs: Vec<_> = expected_ids.iter()
        .zip(expected_names.iter())
        .map(|(&id, &name)| (id, name.to_string()))
        .collect();
    assert_eq!(result_pairs, expected_pairs);
}

#[test]
fn test_gt_utf8_unsupported() {
    // GIVEN
    let db = testlib::fruits_table(StorageCfg::InMemory);

    // WHEN
    let result = db.select_new(&[ColumnRef("name")], "Fruits", &Gt(ColumnRef("name"), Const(UTF8("banana"))));

    // THEN
    assert!(matches!(result, Err(DbError::QueryError(TypeError::InvalidArgType(_, _, _)))), "{result:#?}");
}

#[test]
fn test_lt() {
    // GIVEN
    let db = testlib::fruits_table(StorageCfg::InMemory);

    // Test 3: LessThan filter on U32
    let results = db.select_new(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Lt(ColumnRef("id"), Const(U32(200)))).unwrap();
    assert_eq!(results.len(), 1);
    let row = &results[0];
    assert_eq!(row.get_column(0), 100u32.serialized());
    assert_eq!(row.get_column(1), "apple".serialized());
}


#[test]
fn apply_projection() {
    // GIVEN
    let db = testlib::fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select_new(&[ColumnRef("name")], "Fruits", &Eq(ColumnRef("id"), Const(U32(100)))).unwrap();

    // THEN
    assert_eq!(results.len(), 1);
    let row = &results[0];
    assert_eq!(row.get_column(0), "apple".serialized());
}

#[test]
fn test_multiple_filters() {
    // GIVEN
    let db = testlib::fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select_new(&[ColumnRef("id"), ColumnRef("name")], "Fruits", 
        &Bool::and(
            Gt(ColumnRef("id"), Const(U32(100))), 
            Eq(ColumnRef("name"), Const(UTF8("banana")))
        )
    ).unwrap();

    // THEN
    assert_eq!(results.len(), 2);
    let schema = db.schema_for("Fruits").unwrap();
    for row in &results {
        let id = testlib::get_column_value(&schema, &row, 0);
        assert!(matches!(id, U32(val) if val > 100));
    }
}

#[test]
fn test_no_matching_rows() {
    // GIVEN
    let db = testlib::fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select_new(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Eq(ColumnRef("name"), Const(UTF8("orange")))).unwrap();
    
    // THEN
    assert_eq!(results.len(), 0);
}

#[test]
fn test_no_filters() {
    // GIVEN
    let db = testlib::fruits_table(StorageCfg::InMemory);

    // WHEN
    let results = db.select_new(&[ColumnRef("id"), ColumnRef("name")], "Fruits", &Bool::True).unwrap();
    
    // THEN
    assert_eq!(results.len(), 4);
}

#[test]
fn test_invalid_column() {
    // GIVEN
    let db = testlib::fruits_table(StorageCfg::InMemory);

    // WHEN
    let result = db.select_new(&[ColumnRef("invalid_column")], "Fruits", &True);

    // THEN
    assert_eq!(result.unwrap_err(), DbError::ColumnNotFound("invalid_column".into()));
}

#[test]
fn test_invalid_table() {
    // GIVEN
    let db = Database::new();

    // WHEN
    let result = db.select_new(&[ColumnRef("id")], "NonExistent", &True);

    // THEN
    assert_eq!(result.unwrap_err(), DbError::TableNotFound("NonExistent".into()));
}
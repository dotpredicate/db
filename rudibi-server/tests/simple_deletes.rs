
mod util;
use rudibi_server::engine::*;

#[test]
fn test_delete_empty() {
    // GIVEN
    let mut db = util::empty_table();

    // WHEN
    let deleted_count = db.delete(DeleteCommand::new("EmptyTable", vec![])).unwrap();

    // THEN
    assert_eq!(deleted_count, 0);
}

#[test]
fn test_delete_with_equality_filter() {
    // GIVEN
    let mut db = util::fruits_table();

    // WHEN
    let deleted_count = db.delete(DeleteCommand::new("Fruits",
        vec![Filter::Equal { column: "name".into(), value: "banana".as_bytes().to_vec() }],
    )).unwrap();

    // THEN
    assert_eq!(deleted_count, 2);
    let results = db.get(GetCommand::new("Fruits", &["id", "name"], vec![])).unwrap();
    assert_eq!(results.len(), 2);
    let schema = db.schema_for("Fruits").unwrap();
    let names: Vec<String> = results.iter().map(|row| {
        match db.get_column_value(&schema, &row, 1).unwrap() {
            ColumnValue::String(name) => name,
            x => panic!("Expected String, got {:?}", x),
        }
    }).collect();
    assert_eq!(names, vec!["apple", "cherry"]);
}

#[test]
fn test_delete_with_greater_than_filter() {
    // GIVEN
    let mut db = util::fruits_table();

    // WHEN
    let deleted_count = db.delete(DeleteCommand::new("Fruits",
        vec![Filter::GreaterThan { column: "id".into(), value: 200u32.to_le_bytes().to_vec() }],
    )).unwrap();
    
    // THEN
    assert_eq!(deleted_count, 2);
    let results = db.get(GetCommand::new("Fruits", &["id", "name"], vec![])).unwrap();
    assert_eq!(results.len(), 2);
    let schema = db.schema_for("Fruits").unwrap();
    let ids: Vec<u32> = results.iter().map(|row| {
        if let ColumnValue::U32(id) = db.get_column_value(&schema, &row, 0).unwrap() {
            id
        } else {
            panic!("Expected U32");
        }
    }).collect();
    assert_eq!(ids, vec![100, 200]);
}

#[test]
fn test_delete_all_rows() {
    // GIVEN
    let mut db = util::fruits_table();

    // WHEN
    let deleted_count = db.delete(DeleteCommand::new("Fruits", vec![])).unwrap();

    // THEN
    assert_eq!(deleted_count, 4);
    let results = db.get(GetCommand::new("Fruits", &["id", "name"], vec![])).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_delete_non_existent_table() {
    // GIVEN
    let mut db = Database::new();
    
    // WHEN
    let delete_cmd = DeleteCommand::new("NonExistent", vec![]);
    let result = db.delete(delete_cmd);

    // THEN
    assert!(matches!(result, Err(DatabaseError::TableNotFound(ref s)) if s == "NonExistent"));
}

#[test]
fn test_delete_with_invalid_column() {
    // GIVEN
    let mut db = util::fruits_table();

    // WHEN
    let result = db.delete(DeleteCommand::new("Fruits",
        vec![Filter::Equal { column: "invalid".into(), value: vec![] }],
    ));

    // THEN
    assert!(matches!(result, Err(DatabaseError::ColumnNotFound(ref s)) if s == "invalid"));
}
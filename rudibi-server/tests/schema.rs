
use rudibi_server::engine::*;

#[test]
fn create_duplicate_table() {
    let mut db = Database::new();
    db.new_table(&TableSchema::new("TestTable", vec![ColumnSchema::new("id", DataType::U32)]), StorageConfig::InMemory).unwrap();
    let result = db.new_table(&TableSchema::new("TestTable", vec![ColumnSchema::new("id", DataType::U32)]), StorageConfig::InMemory);
    assert_eq!(result.unwrap_err(), DatabaseError::TableAlreadyExists("TestTable".to_string()));
}

#[test]
fn create_empty_table() {
    let mut db = Database::new();
    let result = db.new_table(&TableSchema::new("EmptyTable", vec![]), StorageConfig::InMemory);
    assert_eq!(result.unwrap_err(), DatabaseError::EmptyTableSchema);
}
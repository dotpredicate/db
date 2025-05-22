
use rudibi_server::engine::*;

#[test]
fn create_duplicate_table() {
    let mut db = Database::new();
    db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), StorageCfg::InMemory).unwrap();
    let result = db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), StorageCfg::InMemory);
    assert_eq!(result.unwrap_err(), DbError::TableAlreadyExists("TestTable".to_string()));
}

#[test]
fn create_empty_table() {
    let mut db = Database::new();
    let result = db.new_table(&Table::new("EmptyTable", vec![]), StorageCfg::InMemory);
    assert_eq!(result.unwrap_err(), DbError::EmptyTableSchema);
}
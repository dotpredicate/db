

use rudibi_server::engine::*;

pub fn fruits_schema() -> TableSchema {
    TableSchema::new("Fruits",
        vec![
            ColumnSchema::new("id", DataType::U32),
            ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 }),
        ]
    )
}

pub fn fruits_table() -> Database {
    let mut db = Database::new();
    db.new_table(&fruits_schema()).unwrap();

    let rows = vec![
        (100u32, "apple"),
        (200u32, "banana"),
        (300u32, "banana"),
        (400u32, "cherry"),
    ];

    for (id, name) in rows {
        let row = StoredRow::of_columns(&[&id.to_le_bytes(), name.as_bytes()]);
        db.store(StoreCommand::new("Fruits", &["id", "name"], vec![row])).unwrap();
    }

    return db;
}

pub fn empty_table() -> Database {
    let mut db = Database::new();
    db.new_table(&TableSchema::new("EmptyTable", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();
    return db;
}
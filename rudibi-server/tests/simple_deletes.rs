
#[cfg(test)]
mod tests {

    use rudibi_server::engine::*;

    #[test]
    fn test_delete_with_equality_filter() {
        let mut db = Database::new();
        db.new_table(&TableSchema::new("Fruits",
            vec![
                ColumnSchema::new("id", DataType::U32),
                ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();

        let rows = vec![
            (100u32, "apple"),
            (200u32, "banana"),
            (300u32, "cherry"),
        ];
        for (id, name) in rows {
            let row = StoredRow::of_columns(&[&id.to_le_bytes(), name.as_bytes()]);
            db.store(StoreCommand::new("Fruits", &["id", "name"], vec![row])).unwrap();
        }

        let delete_cmd = DeleteCommand::new("Fruits",
            vec![Filter::Equal { column: "name".into(), value: "banana".as_bytes().to_vec() }],
        );
        let deleted_count = db.delete(delete_cmd).unwrap();
        assert_eq!(deleted_count, 1);

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
        let mut db = Database::new();
        db.new_table(&TableSchema::new("Fruits",
            vec![
                ColumnSchema::new("id", DataType::U32),
                ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();

        let rows = vec![
            (100u32, "apple"),
            (200u32, "banana"),
            (300u32, "cherry"),
        ];
        for (id, name) in rows {
            let row = StoredRow::of_columns(&[&id.to_le_bytes(), name.as_bytes()]);
            db.store(StoreCommand::new("Fruits", &["id", "name"], vec![row])).unwrap();
        }

        let delete_cmd = DeleteCommand::new("Fruits",
            vec![Filter::GreaterThan { column: "id".into(), value: 200u32.to_le_bytes().to_vec() }],
        );
        let deleted_count = db.delete(delete_cmd).unwrap();
        assert_eq!(deleted_count, 1);

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
        let mut db = Database::new();
        db.new_table(&TableSchema::new("Fruits",
            vec![
                ColumnSchema::new("id", DataType::U32),
                ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();

        let rows = vec![
            (100u32, "apple"),
            (200u32, "banana"),
        ];
        for (id, name) in rows {
            let row = StoredRow::of_columns(&[&id.to_le_bytes(), name.as_bytes()]);
            db.store(StoreCommand::new("Fruits", &["id", "name"], vec![row])).unwrap();
        }

        let delete_cmd = DeleteCommand::new("Fruits", vec![]);
        let deleted_count = db.delete(delete_cmd).unwrap();
        assert_eq!(deleted_count, 2);

        let results = db.get(GetCommand::new("Fruits", &["id", "name"], vec![])).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_delete_non_existent_table() {
        let mut db = Database::new();
        let delete_cmd = DeleteCommand::new("NonExistent", vec![]);
        let result = db.delete(delete_cmd);
        assert!(matches!(result, Err(DatabaseError::TableNotFound(ref s)) if s == "NonExistent"));
    }

    #[test]
    fn test_delete_with_invalid_column() {
        let mut db = Database::new();
        db.new_table(&TableSchema::new("Fruits", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();

        let delete_cmd = DeleteCommand::new("Fruits",
            vec![Filter::Equal { column: "invalid".into(), value: vec![] }],
        );
        let result = db.delete(delete_cmd);
        assert!(matches!(result, Err(DatabaseError::ColumnNotFound(ref s)) if s == "invalid"));
    }
}
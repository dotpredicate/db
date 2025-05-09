use rudibi_server::engine::*;

#[cfg(test)]
mod tests {

    use super::*;


    #[test]
    fn test_all_data_types() {
        let mut db = Database::new();
        db.new_table(Table::new("MixedTypes",
            vec![
                ColumnSchema::new("int", DataType::U32),
                ColumnSchema::new("float", DataType::F64),
                ColumnSchema::new("text", DataType::UTF8 { max_bytes: 10 }),
                ColumnSchema::new("binary", DataType::VARBINARY { max_length: 5 }),
                ColumnSchema::new("buffer", DataType::BUFFER { length: 3 }),
            ]
        )).unwrap();

        let row = StoredRow::of_columns(&[
            &42u32.to_le_bytes(),
            &3.14f64.to_le_bytes(),
            "hello".as_bytes(),
            &[0x01, 0x02, 0x03, 0x04, 0x05],
            &[0xAA, 0xBB, 0xCC],
        ]);

        let result = db.store(StoreCommand::new("MixedTypes", &["int", "float", "text", "binary", "buffer"], vec![row]));
        assert!(result.is_ok(), "{result:#?}");

        let results = db.get(GetCommand::new("MixedTypes", &["int", "float", "text", "binary", "buffer"], vec![]));

        let results = results.expect("results");
        assert_eq!(results.len(), 1);
        let row = &results[0];
        let table = db.get_table("MixedTypes").unwrap();
        assert!(matches!(table.get_column_value(row, 0).unwrap(), ColumnValue::U32(42)));
        assert!(matches!(table.get_column_value(row, 1).unwrap(), ColumnValue::F64(3.14)));
        assert!(matches!(table.get_column_value(row, 2).unwrap(), ColumnValue::String(ref s) if s == "hello"));
        assert!(matches!(table.get_column_value(row, 3).unwrap(), ColumnValue::Bytes(ref v) if v == &[0x01, 0x02, 0x03, 0x04, 0x05]));
        assert!(matches!(table.get_column_value(row, 4).unwrap(), ColumnValue::Bytes(ref v) if v == &[0xAA, 0xBB, 0xCC]));
    }

    #[test]
    fn test_column_size_limits() {
        let mut db = Database::new();
        db.new_table(Table::new("SizeTest",
            vec![
                ColumnSchema::new("utf8", DataType::UTF8 { max_bytes: 5 }),
                ColumnSchema::new("varbinary", DataType::VARBINARY { max_length: 5 }),
                ColumnSchema::new("buffer", DataType::BUFFER { length: 3 }),
            ]
        )).unwrap();

        // Test valid sizes
        let utf8_val = "abc".as_bytes().to_vec(); // 3 bytes, within 0-5
        let varbinary_val = vec![1, 2, 3, 4, 5]; // 5 bytes, at max
        let buffer_val = vec![6, 7, 8]; // 3 bytes, exact length
        let row = StoredRow::of_columns(&[&utf8_val, &varbinary_val, &buffer_val]);
        let result = db.store(StoreCommand::new("SizeTest", &["utf8", "varbinary", "buffer"], vec![row]));
        assert!(result.is_ok(), "{result:#?}");

        // Test invalid size (varbinary too long)
        let invalid_varbinary = vec![1, 2, 3, 4, 5, 6]; // 6 bytes, exceeds max_length 5
        let invalid_row = StoredRow::of_columns(&[&utf8_val, &invalid_varbinary, &buffer_val]);

        let result = db.store(StoreCommand::new("SizeTest", &["utf8", "varbinary", "buffer"], vec![invalid_row]));
        assert_eq!(result, Err(DatabaseError::ColumnSizeOutOfBounds { column: "varbinary".into(), got: 6, min: 0, max: 5 }), "{result:#?}");

        // Test invalid size (buffer too short)
        let short_buffer = vec![1, 2]; // 2 bytes, less than length 3
        let short_row = StoredRow::of_columns(&[&utf8_val, &varbinary_val, &short_buffer]);
        let result = db.store(StoreCommand::new("SizeTest", &["utf8", "varbinary", "buffer"], vec![short_row]));
        assert_eq!(result, Err(DatabaseError::ColumnSizeOutOfBounds { column: "buffer".into(), got: 2, min: 3, max: 3 }));
    }

    #[test]
    fn test_filter_operations() {
        let mut db = Database::new();
        
        db.new_table(Table::new("Fruits", 
            vec![
                ColumnSchema::new("id", DataType::U32),
                ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 })
            ]
        )).unwrap();
    
        let rows = vec![
            (100u32, "apple"),
            (200u32, "banana"),
            (300u32, "cherry"),
            (400u32, "date"),
            (200u32, "banana"),
        ];
    
        for (id, name) in rows {
            let row = StoredRow::of_columns(&[&id.to_le_bytes(), name.as_bytes()]);
            let result = db.store(StoreCommand::new("Fruits", &["id", "name"], vec![row]));
            assert!(result.is_ok(), "{result:#?}");
        }
    
        // Test 1: Equality filter on name (VARBINARY)
        let results = db.get(GetCommand::new(
            "Fruits",
            &["id", "name"],
            vec![Filter::Equal {
                column: "name".into(),
                value: "banana".as_bytes().to_vec(),
            }],
        ));
        let results = results.expect("results");
        assert_eq!(results.len(), 2, "Expected 2 rows for name = 'banana'");
        let table = db.get_table("Fruits").unwrap();
        for row in &results {
            assert!(matches!(table.get_column_value(row, 1).unwrap(), ColumnValue::String(s) if s == "banana"));
            assert!(matches!(table.get_column_value(row, 0).unwrap(), ColumnValue::U32(200)));
        }
    
        // Test 2: GreaterThan filter on id (BUFFER)
        let results = db.get(GetCommand::new(
            "Fruits",
            &["id", "name"],
            vec![Filter::GreaterThan {
                column: "id".into(),
                value: 200u32.to_le_bytes().to_vec(),
            }],
        ));
        let expected_names = vec!["cherry", "date"];
        let expected_ids = vec![300u32, 400u32];
        let results = results.expect("results");
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
    
        // Test 3: LessThan filter on id (BUFFER)
        let results = db.get(GetCommand::new(
            "Fruits",
            &["id", "name"],
            vec![Filter::LessThan {
                column: "id".into(),
                value: 200u32.to_le_bytes().to_vec(),
            }],
        ));
        let results = results.expect("results");
        assert_eq!(results.len(), 1, "Expected 1 row for id < 200");
        assert_eq!(
            u32::from_le_bytes(results[0].get_column(0).try_into().unwrap()),
            100u32,
            "Expected id to be 100"
        );
        assert_eq!(
            results[0].get_column(1),
            "apple".as_bytes(),
            "Expected name to be 'apple'"
        );
    
        // Test 4: Attempt GreaterThan on VARBINARY (should panic)
        let result = db.get(GetCommand::new(
            "Fruits",
            &["name"],
            vec![Filter::GreaterThan {
                column: "name".into(),
                value: "banana".as_bytes().to_vec(),
            }],
        ));
        // FIXME: { max_bytes: 20 } should not be printed
        assert_eq!(result.unwrap_err(), DatabaseError::UnsupportedOperation("GreaterThan filter not supported for data type UTF8 { max_bytes: 20 }".to_string()));
    }

    #[test]
    fn test_multiple_filters() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits",
            vec![
                ColumnSchema::new("id", DataType::U32),
                ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();
    
        let rows = vec![
            (100u32, "apple"),
            (200u32, "banana"),
            (300u32, "banana"),
            (400u32, "cherry"),
        ];
        for (id, name) in rows {
            let row = StoredRow::of_columns(&[&id.to_le_bytes(), name.as_bytes()]);
            db.store(StoreCommand::new(
                "Fruits",
                &["id", "name"],
                vec![row],
            )).unwrap();
        }
    
        let results = db.get(GetCommand::new(
            "Fruits",
            &["id", "name"],
            vec![
                Filter::GreaterThan { column: "id".into(), value: 100u32.to_le_bytes().to_vec() },
                Filter::Equal { column: "name".into(), value: "banana".as_bytes().to_vec() },
            ],
        )).expect("results");
    
        assert_eq!(results.len(), 2, "Expected 2 rows");
        let table = db.get_table("Fruits").unwrap();
        for row in &results {
            let id = table.get_column_value(row, 0).unwrap();
            assert!(matches!(id, ColumnValue::U32(val) if val > 100), "ID should be > 100");
            assert!(matches!(table.get_column_value(row, 1).unwrap(), ColumnValue::String(s) if s == "banana"), "Name should be 'banana'");
        }
    }

    #[test]
    fn test_no_matching_rows() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits",
            vec![
                ColumnSchema::new("id", DataType::U32),
                ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();

        let rows = vec![(100u32, "apple"), (200u32, "banana")];
        for (id, name) in rows {
            let row = StoredRow::of_columns(&[&id.to_le_bytes(), name.as_bytes()]);
            db.store(StoreCommand::new(
                "Fruits",
                &["id", "name"],
                vec![row],
            )).unwrap();
        }

        let results = db.get(GetCommand::new(
            "Fruits",
            &["id", "name"],
            vec![Filter::Equal { column: "name".into(), value: "orange".as_bytes().to_vec() }],
        ));
        let results = results.expect("results");
        assert_eq!(results.len(), 0, "Expected no rows for non-matching filter");
    }

    #[test]
    fn test_no_filters() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits",
            vec![
                ColumnSchema::new("id", DataType::U32),
                ColumnSchema::new("name", DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();

        let rows = vec![(100u32, "apple"), (200u32, "banana")];
        for (id, name) in rows {
            let row = StoredRow::of_columns(&[&id.to_le_bytes(), name.as_bytes()]);
            let result = db.store(StoreCommand::new(
                "Fruits",
                &["id", "name"],
                vec![row],
            ));
            assert!(result.is_ok(), "{result:#?}");
        }

        let results = db.get(GetCommand::new(
            "Fruits",
            &["id", "name"],
            vec![],
        ));
        assert_eq!(results.expect("results").len(), 2, "Expected all rows when no filters are applied");
    }

    #[test]
    fn test_invalid_column() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits",
            vec![ColumnSchema::new("id", DataType::U32)],
        )).unwrap();

        let result = db.get(GetCommand::new(
            "Fruits",
            &["invalid_column"],
            vec![],
        ));
        assert_eq!(result.expect_err("err"), DatabaseError::ColumnNotFound("invalid_column".into()));
    }

    #[test]
    fn test_invalid_table() {
        let db = Database::new();
        let result = db.get(GetCommand::new(
            "NonExistent",
            &["id"],
            vec![],
        ));
        assert_eq!(result.unwrap_err(), DatabaseError::TableNotFound("NonExistent".into()));
    }
}
use rudibi_server::engine::*;

#[cfg(test)]
mod tests {

    use super::*;


    #[test]
    fn test_all_data_types() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "MixedTypes".into(),
            vec![
                ColumnSchema::new("int".into(), DataType::U32),
                ColumnSchema::new("float".into(), DataType::F64),
                ColumnSchema::new("text".into(), DataType::UTF8 { max_bytes: 10 }),
                ColumnSchema::new("binary".into(), DataType::VARBINARY { max_length: 5 }),
                ColumnSchema::new("buffer".into(), DataType::BUFFER { length: 3 }),
            ],
        )).unwrap();
    
        let int_val = 42u32.to_le_bytes().to_vec();
        let float_val = 3.14f64.to_le_bytes().to_vec();
        let text_val = "hello".as_bytes().to_vec();
        let binary_val = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let buffer_val = vec![0xAA, 0xBB, 0xCC];
    
        let mut data = Vec::new();
        data.extend_from_slice(&int_val);
        data.extend_from_slice(&float_val);
        data.extend_from_slice(&text_val);
        data.extend_from_slice(&binary_val);
        data.extend_from_slice(&buffer_val);
    
        let row = StoredRow::new(
            data,
            vec![0, 4, 12, 17, 22, 25],
        );
    
        let result = db.store(StoreCommand::new(
            "MixedTypes".into(),
            vec!["int".into(), "float".into(), "text".into(), "binary".into(), "buffer".into()],
            vec![row],
        ));
        assert!(result.is_ok(), "{result:#?}");
    
        let results = db.get(GetCommand::new(
            "MixedTypes".into(),
            vec!["int".into(), "float".into(), "text".into(), "binary".into(), "buffer".into()],
            vec![],
        ));
    
        let results = results.expect("results");
        assert_eq!(results.len(), 1);
        let row = &results[0];
        let table = db.get_table("MixedTypes").unwrap();
        assert!(matches!(table.get_column_value(row, 0).unwrap(), ColumnValue::U32(42)));
        assert!(matches!(table.get_column_value(row, 1).unwrap(), ColumnValue::F64(3.14)));
        assert!(matches!(table.get_column_value(row, 2).unwrap(), ColumnValue::String(ref s) if s == "hello"));
        assert!(matches!(table.get_column_value(row, 3).unwrap(), ColumnValue::Bytes(ref v) if v == &binary_val));
        assert!(matches!(table.get_column_value(row, 4).unwrap(), ColumnValue::Bytes(ref v) if v == &buffer_val));
    }

    #[test]
    fn test_column_size_limits() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "SizeTest".into(),
            vec![
                ColumnSchema::new("utf8".into(), DataType::UTF8 { max_bytes: 5 }),
                ColumnSchema::new("varbinary".into(), DataType::VARBINARY { max_length: 5 }),
                ColumnSchema::new("buffer".into(), DataType::BUFFER { length: 3 }),
            ],
        )).unwrap();

        // Test valid sizes
        let utf8_val = "abc".as_bytes().to_vec(); // 3 bytes, within 0-5
        let varbinary_val = vec![1, 2, 3, 4, 5]; // 5 bytes, at max
        let buffer_val = vec![6, 7, 8]; // 3 bytes, exact length
        let mut data = Vec::new();
        data.extend_from_slice(&utf8_val);
        data.extend_from_slice(&varbinary_val);
        data.extend_from_slice(&buffer_val);
        let row = StoredRow::new(
            data,
            vec![0, 3, 8, 11],
        );
        let result = db.store(StoreCommand::new(
            "SizeTest".into(),
            vec!["utf8".into(), "varbinary".into(), "buffer".into()],
            vec![row],
        ));
        assert!(result.is_ok(), "{result:#?}");

        // Test invalid size (varbinary too long)
        let invalid_varbinary = vec![1, 2, 3, 4, 5, 6]; // 6 bytes, exceeds max_length 5
        let mut invalid_data = Vec::new();
        invalid_data.extend_from_slice(&utf8_val);
        invalid_data.extend_from_slice(&invalid_varbinary);
        invalid_data.extend_from_slice(&buffer_val);
        let invalid_row = StoredRow::new(
            invalid_data,
            vec![0, 3, 9, 12],
        );

        let result = db.store(StoreCommand::new(
            "SizeTest".into(),
            vec!["utf8".into(), "varbinary".into(), "buffer".into()],
            vec![invalid_row],
        ));
        assert_eq!(result, Err(DatabaseError::ColumnSizeOutOfBounds {
            column: "varbinary".into(),
            got: 6,
            min: 0,
            max: 5,
        }), "{result:#?}");

        // Test invalid size (buffer too short)
        let short_buffer = vec![1, 2]; // 2 bytes, less than length 3
        let mut short_data = Vec::new();
        short_data.extend_from_slice(&utf8_val);
        short_data.extend_from_slice(&varbinary_val);
        short_data.extend_from_slice(&short_buffer);
        let short_row = StoredRow::new(
            short_data,
            vec![0, 3, 8, 10],
        );
        let result = db.store(StoreCommand::new(
            "SizeTest".into(),
            vec!["utf8".into(), "varbinary".into(), "buffer".into()],
            vec![short_row],
        ));
        assert_eq!(result, Err(DatabaseError::ColumnSizeOutOfBounds{ column: "buffer".into(), got: 2, min: 3, max: 3 }));
    }

    #[test]
    fn test_filter_operations() {
        let mut db = Database::new();
        
        db.new_table(Table::new(
            "Fruits".into(),
            vec![
                ColumnSchema::new("id".into(), DataType::U32),
                ColumnSchema::new("name".into(), DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();
    
        let rows = vec![
            (100u32, "apple"),
            (200u32, "banana"),
            (300u32, "cherry"),
            (400u32, "date"),
            (200u32, "banana"),
        ];
    
        for (id, name) in rows {
            let id_bytes = id.to_le_bytes().to_vec();
            let name_bytes = name.as_bytes().to_vec();
            let mut data = Vec::new();
            data.extend_from_slice(&id_bytes);
            data.extend_from_slice(&name_bytes);
            let row = StoredRow::new(
                data,
                vec![0, 4, 4 + name_bytes.len()],
            );
            let result = db.store(StoreCommand::new(
                "Fruits".into(),
                vec!["id".into(), "name".into()],
                vec![row],
            ));
            assert!(result.is_ok(), "{result:#?}");
        }
    
        // Test 1: Equality filter on name (VARBINARY)
        let results = db.get(GetCommand::new(
            "Fruits".into(),
            vec!["id".into(), "name".into()],
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
            "Fruits".into(),
            vec!["id".into(), "name".into()],
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
            "Fruits".into(),
            vec!["id".into(), "name".into()],
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
            "Fruits".into(),
            vec!["name".into()],
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
            "Fruits".into(),
            vec![
                ColumnSchema::new("id".into(), DataType::U32),
                ColumnSchema::new("name".into(), DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();
    
        let rows = vec![
            (100u32, "apple"),
            (200u32, "banana"),
            (300u32, "banana"),
            (400u32, "cherry"),
        ];
        for (id, name) in rows {
            let mut data = Vec::new();
            data.extend_from_slice(&id.to_le_bytes());
            data.extend_from_slice(name.as_bytes());
            db.store(StoreCommand::new(
                "Fruits".into(),
                vec!["id".into(), "name".into()],
                vec![StoredRow::new(
                    data,
                    vec![0, 4, 4 + name.len()],
                )],
            )).unwrap();
        }
    
        let results = db.get(GetCommand::new(
            "Fruits".into(),
            vec!["id".into(), "name".into()],
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
            "Fruits".into(),
            vec![
                ColumnSchema::new("id".into(), DataType::U32),
                ColumnSchema::new("name".into(), DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();

        let rows = vec![(100u32, "apple"), (200u32, "banana")];
        for (id, name) in rows {
            let mut data = Vec::new();
            data.extend_from_slice(&id.to_le_bytes());
            data.extend_from_slice(name.as_bytes());
            db.store(StoreCommand::new(
                "Fruits".into(),
                vec!["id".into(), "name".into()],
                vec![StoredRow::new(
                    data,
                    vec![0, 4, 4 + name.len()],
                )],
            )).unwrap();
        }

        let results = db.get(GetCommand::new(
            "Fruits".into(),
            vec!["id".into(), "name".into()],
            vec![Filter::Equal { column: "name".into(), value: "orange".as_bytes().to_vec() }],
        ));
        let results = results.expect("results");
        assert_eq!(results.len(), 0, "Expected no rows for non-matching filter");
    }

    #[test]
    fn test_no_filters() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits".into(),
            vec![
                ColumnSchema::new("id".into(), DataType::U32),
                ColumnSchema::new("name".into(), DataType::UTF8 { max_bytes: 20 }),
            ],
        )).unwrap();

        let rows = vec![(100u32, "apple"), (200u32, "banana")];
        for (id, name) in rows {
            let mut data = Vec::new();
            data.extend_from_slice(&id.to_le_bytes());
            data.extend_from_slice(name.as_bytes());
            let result = db.store(StoreCommand::new(
                "Fruits".into(),
                vec!["id".into(), "name".into()],
                vec![StoredRow::new(
                    data,
                    vec![0, 4, 4 + name.len()],
                )],
            ));
            assert!(result.is_ok(), "{result:#?}");
        }

        let results = db.get(GetCommand::new(
            "Fruits".into(),
            vec!["id".into(), "name".into()],
            vec![],
        ));
        assert_eq!(results.expect("results").len(), 2, "Expected all rows when no filters are applied");
    }

    #[test]
    fn test_invalid_column() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits".into(),
            vec![ColumnSchema::new("id".into(), DataType::U32)],
        )).unwrap();

        let result = db.get(GetCommand::new(
            "Fruits".into(),
            vec!["invalid_column".into()],
            vec![],
        ));
        assert_eq!(result.expect_err("err"), DatabaseError::ColumnNotFound("invalid_column".into()));
    }

    #[test]
    fn test_invalid_table() {
        let db = Database::new();
        let result = db.get(GetCommand::new(
            "NonExistent".into(),
            vec!["id".into()],
            vec![],
        ));
        assert_eq!(result.unwrap_err(), DatabaseError::TableNotFound("NonExistent".into()));
    }
}
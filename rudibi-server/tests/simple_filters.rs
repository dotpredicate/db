
#[cfg(test)]
mod tests {

    use rudibi_server::engine::*;

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
        let table = db.require_table("Fruits").unwrap();
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
        let table = db.require_table("Fruits").unwrap();
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
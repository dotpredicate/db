
mod util;
use rudibi_server::engine::*;

#[test]
fn test_equality() {
    // GIVEN
    let db = util::fruits_table(StorageConfig::InMemory);

    // WHEN
    let results = db.get(GetCommand::new("Fruits", &["id", "name"],
        vec![Filter::Equal {
            column: "name".into(),
            value: "banana".as_bytes().to_vec(),
        }],
    )).unwrap();
    
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
    let db = util::fruits_table(StorageConfig::InMemory);

    // WHEN
    let results = db.get(GetCommand::new("Fruits", &["id", "name"],
        vec![Filter::GreaterThan {
            column: "id".into(),
            value: 200u32.to_le_bytes().to_vec(),
        }],
    )).unwrap();

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
    let db = util::fruits_table(StorageConfig::InMemory);

    // WHEN
    let result = db.get(GetCommand::new("Fruits", &["name"],
        vec![Filter::GreaterThan {
            column: "name".into(),
            value: "banana".as_bytes().to_vec(),
        }],
    ));

    // THEN
    // FIXME: { max_bytes: 20 } should not be printed
    assert!(result.is_err(), "{result:#?}");
}

#[test]
fn test_lt() {
    // GIVEN
    let db = util::fruits_table(StorageConfig::InMemory);

    // Test 3: LessThan filter on U32
    let results = db.get(GetCommand::new("Fruits", &["id", "name"],
        vec![Filter::LessThan {
            column: "id".into(),
            value: 200u32.to_le_bytes().to_vec(),
        }]
    )).unwrap();
    assert_eq!(results.len(), 1);
    let row = &results[0];
    assert_eq!(row.get_column(0), 100u32.to_le_bytes());
    assert_eq!(row.get_column(1), "apple".as_bytes());
}


#[test]
fn apply_projection() {
    // GIVEN
    let db = util::fruits_table(StorageConfig::InMemory);

    // WHEN
    let results = db.get(GetCommand::new("Fruits", &["name"],
        vec![Filter::Equal { column: "id".into(), value: 100u32.to_le_bytes().to_vec() }],
    )).unwrap();

    // THEN
    assert_eq!(results.len(), 1);
    let row = &results[0];
    assert_eq!(row.get_column(0), "apple".as_bytes());
}

#[test]
fn test_multiple_filters() {
    // GIVEN
    let db = util::fruits_table(StorageConfig::InMemory);

    // WHEN
    let results = db.get(GetCommand::new("Fruits", &["id", "name"],
        vec![
            Filter::GreaterThan { column: "id".into(), value: 100u32.to_le_bytes().to_vec() },
            Filter::Equal { column: "name".into(), value: "banana".as_bytes().to_vec() },
        ],
    )).unwrap();

    // THEN
    assert_eq!(results.len(), 2);
    let schema = db.schema_for("Fruits").unwrap();
    for row in &results {
        let id = db.get_column_value(&schema, &row, 0).unwrap();
        assert!(matches!(id, ColumnValue::U32(val) if val > 100));
    }
}

#[test]
fn test_no_matching_rows() {
    // GIVEN
    let db = util::fruits_table(StorageConfig::InMemory);

    // WHEN
    let results = db.get(GetCommand::new("Fruits", &["id", "name"],
        vec![Filter::Equal { column: "name".into(), value: "orange".as_bytes().to_vec() }],
    )).unwrap();
    
    // THEN
    assert_eq!(results.len(), 0);
}

#[test]
fn test_no_filters() {
    // GIVEN
    let db = util::fruits_table(StorageConfig::InMemory);

    // WHEN
    let results = db.get(GetCommand::new("Fruits", &["id", "name"], vec![])).unwrap();
    
    // THEN
    assert_eq!(results.len(), 4);
}

#[test]
fn test_invalid_column() {
    // GIVEN
    let db = util::fruits_table(StorageConfig::InMemory);

    // WHEN
    let result = db.get(GetCommand::new("Fruits", &["invalid_column"], vec![]));

    // THEN
    assert_eq!(result.unwrap_err(), DatabaseError::ColumnNotFound("invalid_column".into()));
}

#[test]
fn test_invalid_table() {
    // GIVEN
    let db = Database::new();

    // WHEN
    let result = db.get(GetCommand::new("NonExistent", &["id"], vec![]));

    // THEN
    assert_eq!(result.unwrap_err(), DatabaseError::TableNotFound("NonExistent".into()));
}
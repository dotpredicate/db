use std::collections::HashMap;

#[derive(Debug)]
enum DataType {
    U32,
    F64,
    UTF8 { max_bytes: usize },
    VARBINARY { max_length: usize },
    BUFFER { length: usize }
}

impl DataType {

    fn min_size(&self) -> usize {
        match self {
            DataType::U32 => size_of::<u32>(),
            DataType::F64 => size_of::<f64>(),
            DataType::UTF8 { max_bytes: _ } => 0,
            DataType::VARBINARY { max_length: _ } => 0,
            DataType::BUFFER { length } => *length
        }
    }

    fn max_size(&self) -> usize {
        match self {
            DataType::U32 => size_of::<u32>(),
            DataType::F64 => size_of::<f64>(),
            DataType::UTF8 { max_bytes } => *max_bytes,
            DataType::VARBINARY { max_length } => *max_length,
            DataType::BUFFER { length } => *length
        }
    }
}

#[derive(Debug, PartialEq)]
enum DatabaseError {
    TableNotFound(String),
    TableExists(String),
    ColumnNotFound(String),
    InvalidColumnCount { expected: usize, got: usize },
    RowSizeExceeded { got: usize, max: usize },
    RowSizeTooSmall { got: usize, min: usize },
    ColumnSizeOutOfBounds { column: String, got: usize, min: usize, max: usize },
    UnsupportedOperation(String),
    InvalidFilterValue(String),
    ConversionError,
}

#[derive(Debug)]
pub struct ColumnSchema {
    name: String,
    dtype: DataType,
}

#[derive(Debug)]
struct Table {
    name: String,
    schema: Vec<ColumnSchema>,
    contents: Vec<StoredRow>,
    min_row_size: usize,
    max_row_size: usize,
}

#[derive(Debug)]
enum FilterValue {
    U32(u32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl Table {

    pub fn new(name: String, schema: Vec<ColumnSchema>) -> Table {
        Table {
            name,
            min_row_size: schema.iter().map(|c| c.dtype.min_size()).sum(),
            max_row_size: schema.iter().map(|c| c.dtype.max_size()).sum(),
            contents: Vec::new(),
            schema: schema,
        }
    }

    pub fn store(&mut self, columns: Vec<String>, what: Vec<StoredRow>) -> Result<(), DatabaseError> {        
        let table_columns = self.schema.len();
        for (i, row) in what.iter().enumerate() {
            let input_offsets = row.offsets.len();
            let input_cols = input_offsets - 1;
            
            // Validate the number of columns
            // TODO: allow partial inserts
            if input_cols != table_columns {
                return Err(DatabaseError::InvalidColumnCount { expected: table_columns, got: input_cols }) ;
            }
            
            // Validate the row size
            let input_size = row.data.len();
            if input_size > self.max_row_size {
                return Err(DatabaseError::RowSizeExceeded {
                    got: input_size,
                    max: self.max_row_size,
                });
            } else if input_size < self.min_row_size {
                return Err(DatabaseError::RowSizeTooSmall {
                    got: input_size,
                    min: self.min_row_size,
                });
            }

            // Validate each column for size
            for (col_i, (offset, next_offset)) in row.offsets.iter().zip(row.offsets.iter().skip(1)).enumerate() {
                let column_size = next_offset - offset;
                let column = &self.schema[col_i];
                let dtype = &column.dtype;
                if column_size < dtype.min_size() || column_size > dtype.max_size() {
                    return Err(DatabaseError::ColumnSizeOutOfBounds {
                        column: column.name.clone(),
                        got: column_size,
                        min: dtype.min_size(),
                        max: dtype.max_size(),
                    });
                }
            }
            
            // Build the new StoredRow
            let data = row.data.clone();
            let offsets = row.offsets.clone();
            let new_row = StoredRow { data, offsets };
            self.contents.push(new_row);
        }
        Ok(())
    }

    pub fn get(&self, columns: Vec<String>, filters: Vec<Filter>) -> Result<Vec<StoredRow>, DatabaseError> {
        // Validate projection columns
        for column in columns {
            self.require_column(&column)?;
        }

        // Validate filter columns
        for filter in &filters {
            match filter {
                Filter::Equal { column, .. } => {
                    self.require_column(column)?;
                }
                Filter::GreaterThan { column, .. } => {
                    self.require_column(column)?;
                }
                Filter::LessThan { column, .. } => {
                    self.require_column(column)?;
                }
            }
        }
    
        // Filter and map rows
        let mut results = Vec::new();
        for row in &self.contents {
            if self.filter_row(&row, &filters)? {
                results.push(StoredRow {
                    data: row.data.clone(),
                    offsets: row.offsets.clone(),
                });
            }
        }
        Ok(results)
    }

    fn filter_row(&self, row: &StoredRow, filters: &[Filter]) -> Result<bool, DatabaseError> {
        for filter in filters {
            match filter {
                Filter::Equal { column, value } => {
                    let (col_idx, col_scheme) = self.require_column(column)?;
                    let col_data = row.get_column(col_idx);
                    let filter_val = self.convert_filter_value(value, &col_scheme.dtype);
                    if !self.compare_equal(col_data, &filter_val, &col_scheme.dtype)? { return Ok(false); }
                }
                Filter::GreaterThan { column, value } => {
                    let (col_idx, col_scheme) = self.require_column(column)?;
                    match col_scheme.dtype {
                        DataType::U32 => {
                            let col_val = row.get_column_u32(col_idx);
                            let filter_val = u32::from_le_bytes(value.as_slice().try_into().map_err(|_| DatabaseError::ConversionError)?);
                            if !(col_val > filter_val) { return Ok(false); }
                        }
                        DataType::F64 => {
                            let col_val = row.get_column_f64(col_idx);
                            let filter_val = f64::from_le_bytes(value.as_slice().try_into().map_err(|_| DatabaseError::ConversionError)?);
                            if !(col_val > filter_val) { return Ok(false); }
                            
                        }
                        _ => return Err(DatabaseError::UnsupportedOperation(format!("GreaterThan filter not supported for data type {:?}", col_scheme.dtype))),
                    }
                }
                Filter::LessThan { column, value } => {
                    let (col_idx, col_scheme) = self.require_column(column)?;
                    match col_scheme.dtype {
                        DataType::U32 => {
                            let col_val = row.get_column_u32(col_idx);
                            let filter_val = u32::from_le_bytes(value.as_slice().try_into().map_err(|_| DatabaseError::ConversionError)?);
                            if !(col_val < filter_val) { return Ok(false); }
                        }
                        DataType::F64 => {
                            let col_val = row.get_column_f64(col_idx);
                            let filter_val = f64::from_le_bytes(value.as_slice().try_into().map_err(|_| DatabaseError::ConversionError)?);
                            if !(col_val < filter_val) { return Ok(false); }
                        }
                        _ => return Err(DatabaseError::UnsupportedOperation(format!("LessThan filter not supported for data type {:?}", col_scheme.dtype))),
                    }
                }
            }
        } 
        Ok(true)
    }

    fn convert_filter_value(&self, value: &[u8], dtype: &DataType) -> FilterValue {
        match dtype {
            DataType::U32 => FilterValue::U32(u32::from_le_bytes(value.try_into().unwrap())),
            DataType::F64 => FilterValue::F64(f64::from_le_bytes(value.try_into().unwrap())),
            DataType::UTF8 { .. } => FilterValue::String(String::from_utf8(value.to_vec()).unwrap()),
            DataType::VARBINARY { .. } => FilterValue::Bytes(value.to_vec()),
            DataType::BUFFER { .. } => FilterValue::Bytes(value.to_vec()),
}
    }

    fn compare_equal(&self, col_data: &[u8], filter_value: &FilterValue, dtype: &DataType) -> Result<bool, DatabaseError> {
        let result;
        match (dtype, filter_value) {
            (DataType::U32, FilterValue::U32(fv)) => {
                result = u32::from_le_bytes(col_data.try_into().map_err(|_| DatabaseError::ConversionError)?) == *fv
            }
            (DataType::F64, FilterValue::F64(fv)) => {
                result = f64::from_le_bytes(col_data.try_into().map_err(|_| DatabaseError::ConversionError)?) == *fv
            }
            (DataType::UTF8 { .. }, FilterValue::String(fv)) => {
                result = String::from_utf8(col_data.to_vec()).map_err(|_| DatabaseError::ConversionError)? == *fv
            }
            (DataType::VARBINARY { .. }, FilterValue::Bytes(fv)) => result = col_data == fv.as_slice(),
            (DataType::BUFFER { .. }, FilterValue::Bytes(fv)) => result = col_data == fv.as_slice(),
            _ => panic!("Type mismatch"),
        }

        Ok(result)
    }

    fn require_column(&self, name: &str) -> Result<(usize, &ColumnSchema), DatabaseError> {
        self.schema.iter().enumerate()
            .find(|(_, col)| col.name == *name)
            .ok_or_else(|| DatabaseError::ColumnNotFound(name.to_string()))
    }
}

pub struct Database {
    tables: HashMap<String, Table>
}


#[derive(Debug)]
pub struct StoredRow {
    data: Vec<u8>,        // Contiguous buffer holding all column data
    offsets: Vec<usize>,  // Start offsets for each column, plus end of last column
}

impl StoredRow {

    pub fn get_column(&self, col_idx: usize) -> &[u8] {
        let start = self.offsets[col_idx];
        let end = self.offsets[col_idx + 1];
        return &self.data[start..end];
    }

    
    pub fn get_column_u32(&self, col_idx: usize) -> u32 {
        let data = self.get_column(col_idx);
        let bytes: [u8; 4] = data.try_into().unwrap();
        return u32::from_le_bytes(bytes)
    }

    pub fn get_column_f64(&self, col_idx: usize) -> f64 {
        let data = self.get_column(col_idx);
        let bytes: [u8; 8] = data.try_into().unwrap();
        return f64::from_le_bytes(bytes)
    }

    pub fn get_column_utf8(&self, col_idx: usize) -> String {
        let data = self.get_column(col_idx);
        return String::from_utf8(data.to_vec()).unwrap()
    }
}

pub struct StoreCommand {
    table_name: String,
    columns: Vec<String>,
    what: Vec<StoredRow>
}

#[derive(Debug)]
enum Filter {
    Equal { column: String, value: Vec<u8> },
    GreaterThan { column: String, value: Vec<u8> },
    LessThan { column: String, value: Vec<u8> },
}

#[derive(Debug)]
pub struct GetCommand {
    table_name: String,
    columns: Vec<String>,
    filters: Vec<Filter>
}

impl Database {
    pub fn new() -> Database {
        Database {
            tables: HashMap::new()
        }
    }

    pub fn new_table(&mut self, new_table: Table) -> Result<(), DatabaseError> {
        let table_name = &new_table.name;
        if let Some(_) = self.tables.get(table_name) {
            return Err(DatabaseError::TableExists(table_name.clone()));
        }
        self.tables.insert(table_name.to_owned(), new_table);
        return Ok(())
    }

    pub fn store(&mut self, cmd: StoreCommand) -> Result<(), DatabaseError> {
        let tbl = self.require_table_mut(&cmd.table_name)?;
        return tbl.store(cmd.columns, cmd.what);
    }

    pub fn get(&self, cmd: GetCommand) -> Result<Vec<StoredRow>, DatabaseError> {
        let tbl = self.require_table(&cmd.table_name)?;
        return tbl.get(cmd.columns, cmd.filters);
    }

    fn require_table(&self, table_name: &str) -> Result<&Table, DatabaseError> {
        self.tables
            .get(table_name)
            .ok_or_else(|| DatabaseError::TableNotFound(table_name.to_string()))
    }

    fn require_table_mut(&mut self, table_name: &str) -> Result<&mut Table, DatabaseError> {
        self.tables
            .get_mut(table_name)
            .ok_or_else(|| DatabaseError::TableNotFound(table_name.to_string()))
    }
}

mod tests {

    use super::*;


    #[test]
    fn test_all_data_types() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "MixedTypes".into(),
            vec![
                ColumnSchema { name: "int".into(), dtype: DataType::U32 },
                ColumnSchema { name: "float".into(), dtype: DataType::F64 },
                ColumnSchema { name: "text".into(), dtype: DataType::UTF8 { max_bytes: 10 } },
                ColumnSchema { name: "binary".into(), dtype: DataType::VARBINARY { max_length: 5 } },
                ColumnSchema { name: "buffer".into(), dtype: DataType::BUFFER { length: 3 } },
            ],
        ));
    
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
    
        let row = StoredRow {
            data,
            offsets: vec![0, 4, 12, 17, 22, 25],
        };
    
        let result = db.store(StoreCommand {
            table_name: "MixedTypes".into(),
            columns: vec!["int".into(), "float".into(), "text".into(), "binary".into(), "buffer".into()],
            what: vec![row],
        });
        assert!(result.is_ok(), "{result:#?}");
    
        let results = db.get(GetCommand {
            table_name: "MixedTypes".into(),
            columns: vec!["int".into(), "float".into(), "text".into(), "binary".into(), "buffer".into()],
            filters: vec![],
        });
    
        let results = results.expect("results");
        assert_eq!(results.len(), 1);
        let row = &results[0];
        assert_eq!(row.get_column_u32(0), 42);
        assert_eq!(row.get_column_f64(1), 3.14);
        assert_eq!(row.get_column_utf8(2), "hello");
        assert_eq!(row.get_column(3), &binary_val);
        assert_eq!(row.get_column(4), &buffer_val);
    }

    #[test]
    fn test_column_size_limits() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "SizeTest".into(),
            vec![
                ColumnSchema { name: "utf8".into(), dtype: DataType::UTF8 { max_bytes: 5 } },
                ColumnSchema { name: "varbinary".into(), dtype: DataType::VARBINARY { max_length: 5 } },
                ColumnSchema { name: "buffer".into(), dtype: DataType::BUFFER { length: 3 } },
            ],
        ));

        // Test valid sizes
        let utf8_val = "abc".as_bytes().to_vec(); // 3 bytes, within 0-5
        let varbinary_val = vec![1, 2, 3, 4, 5]; // 5 bytes, at max
        let buffer_val = vec![6, 7, 8]; // 3 bytes, exact length
        let mut data = Vec::new();
        data.extend_from_slice(&utf8_val);
        data.extend_from_slice(&varbinary_val);
        data.extend_from_slice(&buffer_val);
        let row = StoredRow {
            data,
            offsets: vec![0, 3, 8, 11],
        };
        let result = db.store(StoreCommand {
            table_name: "SizeTest".into(),
            columns: vec!["utf8".into(), "varbinary".into(), "buffer".into()],
            what: vec![row],
        });
        assert!(result.is_ok(), "{result:#?}");

        // Test invalid size (varbinary too long)
        let invalid_varbinary = vec![1, 2, 3, 4, 5, 6]; // 6 bytes, exceeds max_length 5
        let mut invalid_data = Vec::new();
        invalid_data.extend_from_slice(&utf8_val);
        invalid_data.extend_from_slice(&invalid_varbinary);
        invalid_data.extend_from_slice(&buffer_val);
        let invalid_row = StoredRow {
            data: invalid_data,
            offsets: vec![0, 3, 9, 12],
        };

        let result = db.store(StoreCommand {
            table_name: "SizeTest".into(),
            columns: vec!["utf8".into(), "varbinary".into(), "buffer".into()],
            what: vec![invalid_row],
        });
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
        let short_row = StoredRow {
            data: short_data,
            offsets: vec![0, 3, 8, 10],
        };
        let result = db.store(StoreCommand {
            table_name: "SizeTest".into(),
            columns: vec!["utf8".into(), "varbinary".into(), "buffer".into()],
            what: vec![short_row],
        });
        assert_eq!(result, Err(DatabaseError::ColumnSizeOutOfBounds{ column: "buffer".into(), got: 2, min: 3, max: 3 }));
    }

    #[test]
    fn test_filter_operations() {
        let mut db = Database::new();
        
        // Create Fruits table with id (BUFFER) and name (VARBINARY) columns
        db.new_table(Table::new(
            "Fruits".into(),
            vec![
                ColumnSchema { name: "id".into(), dtype: DataType::U32 },
                ColumnSchema { name: "name".into(), dtype: DataType::UTF8 { max_bytes: 20 } },
            ],
        ));
    
        // Insert test data
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
            let row = StoredRow {
                data,
                offsets: vec![0, 4, 4 + name_bytes.len()],
            };
            let result = db.store(StoreCommand {
                table_name: "Fruits".into(),
                columns: vec!["id".into(), "name".into()],
                what: vec![row],
            });
            assert!(result.is_ok(), "{result:#?}");
        }
    
        // Test 1: Equality filter on name (VARBINARY)
        let results = db.get(GetCommand {
            table_name: "Fruits".into(),
            columns: vec!["id".into(), "name".into()],
            filters: vec![Filter::Equal {
                column: "name".into(),
                value: "banana".as_bytes().to_vec(),
            }],
        });
        let results = results.expect("results");
        assert_eq!(results.len(), 2, "Expected 2 rows for name = 'banana'");
        for row in &results {
            assert_eq!(row.get_column(1), "banana".as_bytes(), "Expected name to be 'banana'");
            let id_bytes = row.get_column(0);
            assert_eq!(id_bytes, 200u32.to_le_bytes(), "Expected id to be 200");
        }
    
        // Test 2: GreaterThan filter on id (BUFFER)
        let results = db.get(GetCommand {
            table_name: "Fruits".into(),
            columns: vec!["id".into(), "name".into()],
            filters: vec![Filter::GreaterThan {
                column: "id".into(),
                value: 200u32.to_le_bytes().to_vec(),
            }],
        });
        let expected_names = vec!["cherry", "date"];
        let expected_ids = vec![300u32, 400u32];
        let results = results.expect("results");
        assert_eq!(results.len(), 2, "Expected 2 rows for id > 200");
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
        assert_eq!(result_pairs, expected_pairs, "Expected id,name pairs for id > 200");
    
        // Test 3: LessThan filter on id (BUFFER)
        let results = db.get(GetCommand {
            table_name: "Fruits".into(),
            columns: vec!["id".into(), "name".into()],
            filters: vec![Filter::LessThan {
                column: "id".into(),
                value: 200u32.to_le_bytes().to_vec(),
            }],
        });
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
        let result = db.get(GetCommand {
            table_name: "Fruits".into(),
            columns: vec!["name".into()],
            filters: vec![Filter::GreaterThan {
                column: "name".into(),
                value: "banana".as_bytes().to_vec(),
            }],
        });
        // FIXME: { max_bytes: 20 } should not be printed
        assert_eq!(result.unwrap_err(), DatabaseError::UnsupportedOperation("GreaterThan filter not supported for data type UTF8 { max_bytes: 20 }".to_string()));
    }

    #[test]
    fn test_multiple_filters() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits".into(),
            vec![
                ColumnSchema { name: "id".into(), dtype: DataType::U32 },
                ColumnSchema { name: "name".into(), dtype: DataType::UTF8 { max_bytes: 20 } },
            ],
        ));

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
            db.store(StoreCommand {
                table_name: "Fruits".into(),
                columns: vec!["id".into(), "name".into()],
                what: vec![StoredRow {
                    data,
                    offsets: vec![0, 4, 4 + name.len()],
                }],
            });
        }

        let results = db.get(GetCommand {
            table_name: "Fruits".into(),
            columns: vec!["id".into(), "name".into()],
            filters: vec![
                Filter::GreaterThan { column: "id".into(), value: 100u32.to_le_bytes().to_vec() },
                Filter::Equal { column: "name".into(), value: "banana".as_bytes().to_vec() },
            ],
        });

        let results = results.expect("results");
        assert_eq!(results.len(), 2);
        for row in &results {
            let id = row.get_column_u32(0);
            assert!(id > 100);
            assert_eq!(row.get_column_utf8(1), "banana");
        }
    }

    #[test]
    fn test_no_matching_rows() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits".into(),
            vec![
                ColumnSchema { name: "id".into(), dtype: DataType::U32 },
                ColumnSchema { name: "name".into(), dtype: DataType::UTF8 { max_bytes: 20 } },
            ],
        ));

        let rows = vec![(100u32, "apple"), (200u32, "banana")];
        for (id, name) in rows {
            let mut data = Vec::new();
            data.extend_from_slice(&id.to_le_bytes());
            data.extend_from_slice(name.as_bytes());
            db.store(StoreCommand {
                table_name: "Fruits".into(),
                columns: vec!["id".into(), "name".into()],
                what: vec![StoredRow {
                    data,
                    offsets: vec![0, 4, 4 + name.len()],
                }],
            }).unwrap();
        }

        let results = db.get(GetCommand {
            table_name: "Fruits".into(),
            columns: vec!["id".into(), "name".into()],
            filters: vec![Filter::Equal { column: "name".into(), value: "orange".as_bytes().to_vec() }],
        });
        let results = results.expect("results");
        assert_eq!(results.len(), 0, "Expected no rows for non-matching filter");
    }

    #[test]
    fn test_no_filters() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits".into(),
            vec![
                ColumnSchema { name: "id".into(), dtype: DataType::U32 },
                ColumnSchema { name: "name".into(), dtype: DataType::UTF8 { max_bytes: 20 } },
            ],
        ));

        let rows = vec![(100u32, "apple"), (200u32, "banana")];
        for (id, name) in rows {
            let mut data = Vec::new();
            data.extend_from_slice(&id.to_le_bytes());
            data.extend_from_slice(name.as_bytes());
            let result = db.store(StoreCommand {
                table_name: "Fruits".into(),
                columns: vec!["id".into(), "name".into()],
                what: vec![StoredRow {
                    data,
                    offsets: vec![0, 4, 4 + name.len()],
                }],
            });
            assert!(result.is_ok(), "{result:#?}");
        }

        let results = db.get(GetCommand {
            table_name: "Fruits".into(),
            columns: vec!["id".into(), "name".into()],
            filters: vec![],
        });
        assert_eq!(results.expect("results").len(), 2, "Expected all rows when no filters are applied");
    }

    #[test]
    fn test_invalid_column() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Fruits".into(),
            vec![ColumnSchema { name: "id".into(), dtype: DataType::U32 }],
        ));

        let result = db.get(GetCommand {
            table_name: "Fruits".into(),
            columns: vec!["invalid_column".into()],
            filters: vec![],
        });
        assert_eq!(result.expect_err("err"), DatabaseError::ColumnNotFound("invalid_column".into()));
    }

    #[test]
    fn test_invalid_table() {
        let db = Database::new();
        let result = db.get(GetCommand {
            table_name: "NonExistent".into(),
            columns: vec!["id".into()],
            filters: vec![],
        });
        assert_eq!(result.unwrap_err(), DatabaseError::TableNotFound("NonExistent".into()));
    }
}
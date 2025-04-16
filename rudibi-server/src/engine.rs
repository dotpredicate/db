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

impl Table {

    pub fn new(name: String, schema: Vec<ColumnSchema>) -> Table {
        return Table {
            name,
            min_row_size: schema.iter().map(|c| c.dtype.min_size()).sum(),
            max_row_size: schema.iter().map(|c| c.dtype.max_size()).sum(),
            contents: Vec::new(),
            schema: schema,
        }
    }

    pub fn store(&mut self, columns: Vec<String>, what: Vec<StoredRow>) {
        let table_columns = self.schema.len();
        for (i, row) in what.iter().enumerate() {
            let input_offsets = row.offsets.len();
            
            // Validate the number of columns
            if input_offsets - 1 != table_columns {
                panic!("Error at row {i} - invalid amount of columns: expected {table_columns}, got {input_offsets}");
            }
            
            // Validate the row size
            let input_size = row.data.len();
            if input_size > self.max_row_size {
                panic!("Row {}: max row size exceeded, got {} bytes, expected max {}", i, input_size, self.max_row_size);
            } else if input_size < self.min_row_size {
                panic!("Row {}: row too small, got {} bytes, expected max {}", i, input_size, self.max_row_size);
            }

            // Validate each column for size
            for (col_i, (offset, next_offset)) in row.offsets.iter().zip(row.offsets.iter().skip(1)).enumerate() {
                let column_size = next_offset - offset;
                let dtype = &self.schema[col_i].dtype;
                if column_size < dtype.min_size() || column_size > dtype.max_size() {
                    panic!("Row {}: column size out of bounds, got {} bytes, expected between {} and {}", i, column_size, self.schema[0].dtype.min_size(), self.schema[0].dtype.max_size());
                }
            }
            
            // Build the new StoredRow
            let data = row.data.clone();
            let offsets = row.offsets.clone();
            let new_row = StoredRow { data, offsets };
            self.contents.push(new_row);
        }
    }

    pub fn get(&self, columns: Vec<String>, filters: Vec<Filter>) -> Vec<StoredRow> {
        let col_idxs: Vec<_> = columns.iter().map(|req_col| self.require_column(req_col).0).collect();

        self.contents.iter()
            .filter(|row| {
                for filter in &filters {
                    match filter {
                        Filter::Equal { column, value } => {
                            let (col_idx, col_scheme) = self.require_column(column);
                            let col_data = row.get_column(col_idx);
                            let filter_value = self.convert_filter_value(value, &col_scheme.dtype);
                            if !self.compare_equal(col_data, &filter_value, &col_scheme.dtype) {
                                return false;
                            }
                        }
                        Filter::GreaterThan { column, value } => {
                            let (col_idx, col_scheme) = self.require_column(column);
                            match col_scheme.dtype {
                               DataType::U32 => {
                                    let col_val = row.get_column_u32(col_idx);
                                    let filter_val = self.convert_filter_value(value, &col_scheme.dtype).parse::<u32>().unwrap();
                                    if col_val <= filter_val {
                                        return false;
                                    }
                                },
                                DataType::F64 => {
                                    let col_val = row.get_column_f64(col_idx);
                                    let filter_val = self.convert_filter_value(value, &col_scheme.dtype).parse::<f64>().unwrap();
                                    if col_val <= filter_val {
                                        return false;
                                    }
                                },
                                _ => panic!("GreaterThan only supported for numeric types"),
                            }
                        }
                        Filter::LessThan { column, value } => {
                            let (col_idx, col_scheme) = self.require_column(column);
                            match col_scheme.dtype {
                               DataType::U32 => {
                                    let col_val = row.get_column_u32(col_idx);
                                    let filter_val = self.convert_filter_value(value, &col_scheme.dtype).parse::<u32>().unwrap();
                                    if col_val >= filter_val {
                                        return false;
                                    }
                                },
                                DataType::F64 => {
                                    let col_val = row.get_column_f64(col_idx);
                                    let filter_val = self.convert_filter_value(value, &col_scheme.dtype).parse::<f64>().unwrap();
                                    if col_val >= filter_val {
                                        return false;
                                    }
                                },
                                _ => panic!("LessThan only supported for numeric types"),
                            }
                        }
                    }
                }
                true
            })
            .map(|row| StoredRow {
                data: row.data.clone(),
                offsets: row.offsets.clone(),
            })
            .collect()
    }

    // Convert filter value from bytes to a string representation
    fn convert_filter_value(&self, value: &Vec<u8>, dtype: &DataType) -> String {
        match dtype {
            DataType::U32 => {
                let bytes: [u8; 4] = value.as_slice().try_into().unwrap();
                u32::from_le_bytes(bytes).to_string()
            }
            DataType::F64 => {
                let bytes: [u8; 8] = value.as_slice().try_into().unwrap();
                f64::from_le_bytes(bytes).to_string()
            }
            DataType::UTF8 { .. } => String::from_utf8(value.to_vec()).unwrap(),
            DataType::VARBINARY { .. } => String::from_utf8(value.to_vec()).unwrap(), // FIXME: write as hex
            DataType::BUFFER { .. } =>  String::from_utf8(value.to_vec()).unwrap(), // FIXME: write as hex
        }
    }

    // Compare column data with filter value
    fn compare_equal(&self, col_data: &[u8], filter_value: &str, dtype: &DataType) -> bool {
        match dtype {
            DataType::U32 => {
                let col_val = u32::from_le_bytes(col_data.try_into().unwrap());
                col_val.to_string() == filter_value
            }
            DataType::F64 => {
                let col_val = f64::from_le_bytes(col_data.try_into().unwrap());
                col_val.to_string() == filter_value
            }
            DataType::UTF8 { .. } => String::from_utf8(col_data.to_vec()).unwrap() == filter_value,
            DataType::VARBINARY { .. } => col_data == filter_value.as_bytes(),
            DataType::BUFFER { .. } => col_data == filter_value.as_bytes(),
        }
    }

    fn require_column(&self, name: &String) -> (usize, &ColumnSchema) {
        return self.schema.iter().enumerate()
            .find(|(_, col)| col.name == *name)
            .unwrap_or_else(|| panic!("Column {name} not found"))
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
        return Database {
            tables: HashMap::new()
        };
    }

    pub fn new_table(&mut self, new_table: Table) {
        let table_name = &new_table.name;
        if let Some(_) = self.tables.get(table_name) {
            panic!("Table {table_name:?} already exists");
        }
        self.tables.insert(table_name.to_owned(), new_table);
    }

    pub fn store(&mut self, cmd: StoreCommand) {
        let tbl = self.require_table_mut(&cmd.table_name);
        tbl.store(cmd.columns, cmd.what);
    }

    pub fn get(&self, cmd: GetCommand) -> Vec<StoredRow> {
        let tbl = self.require_table(&cmd.table_name);
        return tbl.get(cmd.columns, cmd.filters);
    }

    fn require_table(&self, table_name: &String) -> &Table {
        if let Some(table) = self.tables.get(table_name) {
            return table;
        }
        panic!("Table {table_name} not found");
    }

    fn require_table_mut(&mut self, table_name: &String) -> &mut Table {
        if let Some(table) = self.tables.get_mut(table_name) {
            return table;
        }
        panic!("Table {table_name} not found");
    }
}

mod tests {

    use super::*;


    #[test]
    fn test_simple_image_store() {
        let mut db = Database::new();
        db.new_table(Table::new(
            "Images".into(),
            vec![
                ColumnSchema { name: "id".into(), dtype: DataType::BUFFER { length: 4 } },
                ColumnSchema { name: "image".into(), dtype: DataType::VARBINARY { max_length: 16 * 1024 * 1024 } }
            ]
        ));
        

        let id: u32 = 1337;
        let id_bytes = id.to_le_bytes().to_vec();

        let img_size = 16 * 1024 * 1024;
        let img = vec![0 as u8; img_size];

        let mut data = Vec::with_capacity(id_bytes.len() + img.len());
        data.extend_from_slice(&id_bytes);
        data.extend_from_slice(&img);
        let row = StoredRow {
            data,
            offsets: vec![0, 4, 4 + img_size]
        };

        db.store(StoreCommand {
            table_name: "Images".into(),
            columns: vec!["id".into(), "image".into()],
            what: vec![row]
        });
        let result = db.get(GetCommand {
            table_name: "Images".into(),
            columns: vec!["id".into(), "image".into()],
            filters: vec![Filter::Equal { column: "id".into(), value: id_bytes.clone() }]
        });
        assert_eq!(result.len(), 1);
        let result = &result[0];
        assert_eq!(result.get_column(0), id_bytes, "Expected id to match");
        assert!(result.get_column(1).len() == img_size, "Expected image size to match");
        assert!(result.get_column(1).iter().all(|&x| x == 0), "Expected image data to be all zeros");
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
            db.store(StoreCommand {
                table_name: "Fruits".into(),
                columns: vec!["id".into(), "name".into()],
                what: vec![row],
            });
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
        let result = std::panic::catch_unwind(|| {
            db.get(GetCommand {
                table_name: "Fruits".into(),
                columns: vec!["name".into()],
                filters: vec![Filter::GreaterThan {
                    column: "name".into(),
                    value: "banana".as_bytes().to_vec(),
                }],
            })
        });
        assert!(result.is_err(), "Expected panic for GreaterThan on VARBINARY");
    }
}
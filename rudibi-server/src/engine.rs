use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum DataType {
    U32,
    F64,
    UTF8 { max_bytes: usize },
    VARBINARY { max_length: usize },
    BUFFER { length: usize }
}

impl DataType {

    pub fn min_size(&self) -> usize {
        match self {
            DataType::U32 => size_of::<u32>(),
            DataType::F64 => size_of::<f64>(),
            DataType::UTF8 { max_bytes: _ } => 0,
            DataType::VARBINARY { max_length: _ } => 0,
            DataType::BUFFER { length } => *length
        }
    }

    pub fn max_size(&self) -> usize {
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
pub enum DatabaseError {
    TableNotFound(String),
    TableAlreadyExists(String),
    EmptyTableSchema,
    ColumnNotFound(String),
    InvalidColumnCount { expected: usize, got: usize },
    RowSizeExceeded { got: usize, max: usize },
    RowSizeTooSmall { got: usize, min: usize },
    ColumnSizeOutOfBounds { column: String, got: usize, min: usize, max: usize },
    UnsupportedOperation(String),
    ConversionError,
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    name: String,
    dtype: DataType,
}

impl ColumnSchema {
    pub fn new(name: &str, dtype: DataType) -> ColumnSchema {
        ColumnSchema { name: name.to_string(), dtype }
    }
}

#[derive(Debug)]
pub struct Table {
    name: String,
    schema: Vec<ColumnSchema>,
    contents: Vec<StoredRow>,
    min_row_size: usize,
    max_row_size: usize,
}

#[derive(Debug, PartialEq)]
pub enum ColumnValue {
    U32(u32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl Table {

    pub fn new(name: &str, schema: Vec<ColumnSchema>) -> Table {
        Table {
            name: name.to_string(),
            min_row_size: schema.iter().map(|c| c.dtype.min_size()).sum(),
            max_row_size: schema.iter().map(|c| c.dtype.max_size()).sum(),
            contents: Vec::new(),
            schema
        }
    }

    pub fn store(&mut self, _columns: Vec<String>, what: Vec<StoredRow>) -> Result<(), DatabaseError> {        
        let table_columns = self.schema.len();
        for row in what {
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
                return Err(DatabaseError::RowSizeExceeded { got: input_size, max: self.max_row_size });
            }
            if input_size < self.min_row_size {
                return Err(DatabaseError::RowSizeTooSmall { got: input_size, min: self.min_row_size });
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
            self.contents.push(row);
        }
        Ok(())
    }

    pub fn get(&self, columns: Vec<String>, filters: Vec<Filter>) -> Result<Vec<StoredRow>, DatabaseError> {
        // Validate projection columns
        for column in columns.iter() {
            self.require_column(&column)?;
        }

        // Validate filter columns
        let filter_columns: Vec<String> = filters.iter().map(|f| match f {
            Filter::Equal { column, .. } => column.clone(),
            Filter::GreaterThan { column, .. } => column.clone(),
            Filter::LessThan { column, .. } => column.clone(),
        }).collect();

        for col in &filter_columns {
            self.require_column(col)?;
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

    pub fn delete(&mut self, filters: Vec<Filter>) -> Result<usize, DatabaseError> {
        // Validate filter columns
        let filter_columns: Vec<String> = filters.iter().map(|f| match f {
            Filter::Equal { column, .. } => column.clone(),
            Filter::GreaterThan { column, .. } => column.clone(),
            Filter::LessThan { column, .. } => column.clone(),
        }).collect();

        for col in &filter_columns {
            self.require_column(col)?;
        }

        // Filter rows to remove
        let mut to_remove = Vec::new();
        for (i, row) in self.contents.iter().enumerate() {
            if self.filter_row(row, &filters)? {
                to_remove.push(i);
            }
        }
        
        // Remove rows in reverse order to avoid index shifting
        for i in to_remove.iter().rev() {
            self.contents.remove(*i);
        }
        Ok(to_remove.len())
    }

    pub fn get_column_value(&self, row: &StoredRow, col_idx: usize) -> Result<ColumnValue, DatabaseError> {
        let col_scheme = &self.schema[col_idx];
        let data = row.get_column(col_idx);
        match col_scheme.dtype {
            DataType::U32 => { Ok(ColumnValue::U32(u32::from_le_bytes(data.try_into().map_err(|_| DatabaseError::ConversionError)?))) }
            DataType::F64 => { Ok(ColumnValue::F64(f64::from_le_bytes(data.try_into().map_err(|_| DatabaseError::ConversionError)?))) }
            DataType::UTF8 { .. } => Ok(ColumnValue::String(
                String::from_utf8(data.to_vec()).map_err(|_| DatabaseError::ConversionError)?,
            )),
            DataType::VARBINARY { .. } => Ok(ColumnValue::Bytes(data.to_vec())),
            DataType::BUFFER { length } => {
                if data.len() != length {
                    return Err(DatabaseError::ConversionError);
                }
                Ok(ColumnValue::Bytes(data.to_vec()))
            }
        }
    }

    fn filter_row(&self, row: &StoredRow, filters: &[Filter]) -> Result<bool, DatabaseError> {
        for filter in filters {
            let (column, value) = match filter {
                Filter::Equal { column, value } => (column, value),
                Filter::GreaterThan { column, value } => (column, value),
                Filter::LessThan { column, value } => (column, value),
            };
            let (col_idx, col_scheme) = self.require_column(column)?;
            let col_value = self.get_column_value(row, col_idx)?;
            let filter_val = self.convert_filter_value(value, &col_scheme.dtype);
    
            let passes = match filter {
                Filter::Equal { .. } => col_value == filter_val,
                Filter::GreaterThan { .. } => match (col_value, filter_val) {
                    (ColumnValue::U32(col_val), ColumnValue::U32(filter_val)) => col_val > filter_val,
                    (ColumnValue::F64(col_val), ColumnValue::F64(filter_val)) => col_val > filter_val,
                    _ => return Err(DatabaseError::UnsupportedOperation(format!(
                        "GreaterThan filter not supported for data type {:?}", col_scheme.dtype
                    ))),
                },
                Filter::LessThan { .. } => match (col_value, filter_val) {
                    (ColumnValue::U32(col_val), ColumnValue::U32(filter_val)) => col_val < filter_val,
                    (ColumnValue::F64(col_val), ColumnValue::F64(filter_val)) => col_val < filter_val,
                    _ => return Err(DatabaseError::UnsupportedOperation(format!(
                        "LessThan filter not supported for data type {:?}", col_scheme.dtype
                    ))),
                },
            };
            if !passes {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn convert_filter_value(&self, value: &[u8], dtype: &DataType) -> ColumnValue {
        match dtype {
            DataType::U32 => ColumnValue::U32(u32::from_le_bytes(value.try_into().unwrap())),
            DataType::F64 => ColumnValue::F64(f64::from_le_bytes(value.try_into().unwrap())),
            DataType::UTF8 { .. } => ColumnValue::String(String::from_utf8(value.to_vec()).unwrap()),
            DataType::VARBINARY { .. } => ColumnValue::Bytes(value.to_vec()),
            DataType::BUFFER { .. } => ColumnValue::Bytes(value.to_vec()),
}
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
    
    pub fn of_columns(columns: &[&[u8]]) -> StoredRow {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        offsets.push(0);
        for col in columns {
            data.extend_from_slice(col);
            offsets.push(data.len());
        }
        StoredRow { data, offsets }
    }

    pub fn new(data: Vec<u8>, offsets: Vec<usize>) -> StoredRow {
        StoredRow { data, offsets }
    }

    pub fn get_column(&self, col_idx: usize) -> &[u8] {
        let start = self.offsets[col_idx];
        let end = self.offsets[col_idx + 1];
        return &self.data[start..end];
    }
}

pub struct StoreCommand {
    table_name: String,
    columns: Vec<String>,
    what: Vec<StoredRow>
}

impl StoreCommand {
    pub fn new(table_name: &str, columns: &[&str], what: Vec<StoredRow>) -> StoreCommand {
        StoreCommand { table_name: table_name.to_string(), columns: columns.iter().map(|s| s.to_string()).collect(), what }
    }
}


#[derive(Debug)]
pub enum Filter {
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

impl GetCommand {
    pub fn new(table_name: &str, columns: &[&str], filters: Vec<Filter>) -> GetCommand {
        GetCommand { table_name: table_name.to_string(), columns: columns.iter().map(|s| s.to_string()).collect(), filters }
    }
}

#[derive(Debug)]
pub struct DeleteCommand {
    table_name: String,
    filters: Vec<Filter>
}

impl DeleteCommand {
    pub fn new(table_name: &str, filters: Vec<Filter>) -> DeleteCommand {
        DeleteCommand { table_name: table_name.to_string(), filters }
    }
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
            return Err(DatabaseError::TableAlreadyExists(table_name.clone()));
        }
        if new_table.schema.is_empty() {
            return Err(DatabaseError::EmptyTableSchema);
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

    pub fn delete(&mut self, cmd: DeleteCommand) -> Result<usize, DatabaseError> {
        let tbl = self.require_table_mut(&cmd.table_name)?;
        tbl.delete(cmd.filters)
    }

    pub fn require_table(&self, table_name: &str) -> Result<&Table, DatabaseError> {
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

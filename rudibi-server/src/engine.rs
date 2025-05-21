use std::collections::HashMap;

use crate::storage::{DiskStorage, InMemoryStorage, RowContent, RowId, Storage};

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



#[derive(Debug, PartialEq)]
pub enum ColumnValue {
    U32(u32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub min_row_size: usize,
    pub max_row_size: usize,
}

impl TableSchema {

    pub fn new(name: &str, schema: Vec<ColumnSchema>) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            min_row_size: schema.iter().map(|c| c.dtype.min_size()).sum(),
            max_row_size: schema.iter().map(|c| c.dtype.max_size()).sum(),
            columns: schema
        }
    }

    // Projecting columns in select clauses, filters, etc.
    // Seen as projecting input columns to schema
    pub fn project_to_schema_optional(&self, columns: &Vec<String>) -> Result<Vec<usize>, DatabaseError> {
        // FIXME: O(n^2) check
        let mut indices = Vec::with_capacity(columns.len());
        for col in columns {
            let (col_idx, _) = self.require_column(col)?;
            indices.push(col_idx);
        }
        Ok(indices)
    }

    // Projecting columns in inserts where all columns are required
    // Seen as projecting schema to input columns
    // TODO: Allow partial inserts
    pub fn project_from_schema_required(&self, columns: &Vec<String>) -> Result<Vec<usize>, DatabaseError> {
        if columns.len() != self.columns.len() {
            // FIXME: Better error here. Missing required column.
            return Err(DatabaseError::InvalidColumnCount { expected: self.columns.len(), got: columns.len() });
        }
        // FIXME: O(n^2) check
        let mut indices = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            // FIXME: Better error here. Missing required column.
            let source_idx = columns.iter().position(|c| c == &col.name)
                .ok_or_else(|| DatabaseError::ColumnNotFound(col.name.clone()))?;
            indices.push(source_idx);
        }
        Ok(indices)
    }

    fn require_column(&self, name: &str) -> Result<(usize, &ColumnSchema), DatabaseError> {
        self.columns.iter().enumerate()
            .find(|(_, col)| col.name == *name)
            .ok_or_else(|| DatabaseError::ColumnNotFound(name.to_string()))
    }

    fn validate_input(&self, row: &StoredRow, column_mapping: &Vec<usize>) -> Result<(), DatabaseError> {
        // Validate the number of columns
        let input_offsets = row.offsets.len();
        let input_columns = input_offsets - 1;

        // Probably not needed here
        // TODO: allow partial inserts for optional columns
        if input_columns != column_mapping.len(){
            return Err(DatabaseError::InvalidColumnCount { expected: self.columns.len(), got: input_columns }) ;
        }
        
        // Validate the row size
        let input_size = row.content.len();
        if input_size > self.max_row_size {
            return Err(DatabaseError::RowSizeExceeded { got: input_size, max: self.max_row_size });
        }
        if input_size < self.min_row_size {
            return Err(DatabaseError::RowSizeTooSmall { got: input_size, min: self.min_row_size });
        }

        // Validate each column in schema for size in input
        for (idx, col) in self.columns.iter().enumerate() {
            let input_col_idx = column_mapping[idx];
            let input_col = row.get_column(input_col_idx);
            let input_col_size = input_col.len();
            let col_min = col.dtype.min_size();
            let col_max = col.dtype.max_size();
            if input_col_size < col_min || input_col_size > col_max {
                return Err(DatabaseError::ColumnSizeOutOfBounds { column: col.name.clone(), got: input_col_size, min: col_min, max: col_max });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StoredRow {
    pub content: Vec<u8>,        // Contiguous buffer holding all column data
    pub offsets: Vec<usize>,  // Start offsets for each column, plus end of last column
}

impl StoredRow {
    
    pub fn of_columns(columns: &[&[u8]]) -> StoredRow {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        // Preallocating is slower??
        // let mut data = Vec::with_capacity(columns.iter().map(|col| col.len()).sum());
        // let mut offsets = Vec::with_capacity(columns.len() + 1);
        offsets.push(0);
        for col in columns {
            data.extend_from_slice(col);
            offsets.push(data.len());
        }
        StoredRow { content: data, offsets }
    }

    pub fn new(data: Vec<u8>, offsets: Vec<usize>) -> StoredRow {
        StoredRow { content: data, offsets }
    }

    pub fn get_column(&self, col_idx: usize) -> &[u8] {
        let start = self.offsets[col_idx];
        let end = self.offsets[col_idx + 1];
        return &self.content[start..end];
    }
}

pub struct StoreCommand {
    table_name: String,
    columns: Vec<String>, // FIXME: Allow partial inserts
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

#[derive(Clone)]
pub enum StorageConfig {
    InMemory,
    Disk { path: String },
}



pub struct Database {
    schemas: HashMap<String, TableSchema>,
    storage: HashMap<String, Box<dyn Storage>>
}

impl Database {
    pub fn new() -> Database {
        Database {
            schemas: HashMap::new(),
            storage: HashMap::new(),
        }
    }

    pub fn in_mem(&mut self, new_table: &TableSchema) -> Result<(), DatabaseError> {
        self.new_table(new_table, StorageConfig::InMemory)
    }

    pub fn new_table(&mut self, new_table: &TableSchema, storage_cfg: StorageConfig) -> Result<(), DatabaseError> {
        let table_name = &new_table.name;
        if let Some(_) = self.schemas.get(table_name) {
            return Err(DatabaseError::TableAlreadyExists(table_name.clone()));
        }

        if new_table.columns.is_empty() {
            return Err(DatabaseError::EmptyTableSchema);
        }

        self.schemas.insert(table_name.to_owned(), new_table.clone());

        let storage: Box<dyn Storage> = match storage_cfg {
            StorageConfig::InMemory => Box::new(InMemoryStorage::new(new_table.clone())),
            StorageConfig::Disk { path } => Box::new(DiskStorage::new(new_table.clone(), &path)),
        };

        let old_storage = self.storage.insert(table_name.to_owned(), storage);
        if old_storage.is_some() {
            // TODO: What to do in this case?
            return Err(DatabaseError::TableAlreadyExists(table_name.clone()));
        }
        return Ok(())
    }

    pub fn store(&mut self, cmd: StoreCommand) -> Result<usize, DatabaseError> {
        let schema = self.schema_for(&cmd.table_name)?.clone();
        let storage = self.mut_storage_for(&cmd.table_name)?;
        
        let column_mapping = schema.project_from_schema_required(&cmd.columns)?;

        let mut inserts = Vec::new();
        for row in cmd.what {
            schema.validate_input(&row, &column_mapping)?;
            // Build the new StoredRow
            inserts.push(row);
        }
        let stored = inserts.len();

        // Store the rows
        storage.store(inserts, &column_mapping);
        Ok(stored)
    }

    pub fn get(&self, cmd: GetCommand) -> Result<Vec<StoredRow>, DatabaseError> {
        let schema = self.schema_for(&cmd.table_name)?;
        let storage = self.storage_for(&cmd.table_name)?;

        // Validate and project columns
        let column_mapping = schema.project_to_schema_optional(&cmd.columns)?;

        // TODO: Some mechanism of reporting / logging internal assertions
        assert!(column_mapping.len() == cmd.columns.len(), "Column mapping should match the number of columns requested");

        // Validate filter columns
        // FIXME: Cloning
        let filter_columns: Vec<String> = cmd.filters.iter().map(|f| match f {
            Filter::Equal { column, .. } => column.clone(),
            Filter::GreaterThan { column, .. } => column.clone(),
            Filter::LessThan { column, .. } => column.clone(),
        }).collect();
        // TODO: Mapping of filters to column IDs is unused. Internally this will use string mapping.
        schema.project_to_schema_optional(&filter_columns)?;
    
        // Filter and map rows
        let mut results = Vec::new();
        for (_, row) in storage.scan() {
            if self.filter_row(&schema, &row, &cmd.filters)? {
                let mut selected_row = Vec::new();
                for proj_col in &column_mapping {
                    // FIXME: Cloning
                    selected_row.push(row.get_column(proj_col.clone()));
                }
                let projected = StoredRow::of_columns(&selected_row);
                results.push(projected);
            }
        }
        Ok(results)
    }

    pub fn delete(&mut self, cmd: DeleteCommand) -> Result<usize, DatabaseError> {
        let schema = self.schema_for(&cmd.table_name)?;

        // Validate filter columns
        let filter_columns: Vec<String> = cmd.filters.iter().map(|f| match f {
            Filter::Equal { column, .. } => column.clone(),
            Filter::GreaterThan { column, .. } => column.clone(),
            Filter::LessThan { column, .. } => column.clone(),
        }).collect();
        schema.project_to_schema_optional(&filter_columns)?;

        // Filter rows to remove
        let to_remove: Vec<RowId> = self.storage_for(&cmd.table_name)?
            .scan()
            .filter(|(_, row)| self.filter_row(&schema, &row, &cmd.filters).unwrap_or(false))
            .map(|(row_id, _)| row_id)
            .collect();

        // Execute removal
        let removed = to_remove.len();
        // FIXME: Mutable borrow, again - borrow checker, storage.as_mut() doesn't work
        self.mut_storage_for(&cmd.table_name)?.delete_rows(to_remove);
        Ok(removed)
    }

    pub fn schema_for(&self, table_name: &str) -> Result<&TableSchema, DatabaseError> {
        self.schemas
            .get(table_name)
            .ok_or_else(|| DatabaseError::TableNotFound(table_name.to_string()))
    }

    fn storage_for(&self, table_name: &str) -> Result<&Box<dyn Storage>, DatabaseError> {
        self.storage
            .get(table_name)
            .ok_or_else(|| DatabaseError::TableNotFound(table_name.to_string()))
    }

    fn mut_storage_for(&mut self, table_name: &str) -> Result<&mut Box<dyn Storage>, DatabaseError> {
        self.storage
            .get_mut(table_name)
            .ok_or_else(|| DatabaseError::TableNotFound(table_name.to_string()))
    }

    // TODO: This probably should go somewhere else
    pub fn canonical_column(&self, schema: &ColumnSchema, data: &[u8]) -> Result<ColumnValue, DatabaseError> {
        match schema.dtype {
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

    // TODO: This probably should go somewhere else. Like a client-side util?
    pub fn get_column_value(&self, schema: &TableSchema, row: &StoredRow, col_idx: usize) -> Result<ColumnValue, DatabaseError> {
        let col_scheme = &schema.columns[col_idx];
        self.canonical_column(col_scheme, row.get_column(col_idx))
    }

    // TODO: This probably should go somewhere else
    fn filter_row(&self, schema: &TableSchema, row: &RowContent, filters: &[Filter]) -> Result<bool, DatabaseError> {
        for filter in filters {
            let (column, value) = match filter {
                Filter::Equal { column, value } => (column, value),
                Filter::GreaterThan { column, value } => (column, value),
                Filter::LessThan { column, value } => (column, value),
            };
            let (col_idx, col_scheme) = schema.require_column(column)?;
            let col_value = self.canonical_column(&col_scheme, row.get_column(col_idx))?;
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

    // TODO: This probably should go somewhere else
    fn convert_filter_value(&self, value: &[u8], dtype: &DataType) -> ColumnValue {
        match dtype {
            DataType::U32 => ColumnValue::U32(u32::from_le_bytes(value.try_into().unwrap())),
            DataType::F64 => ColumnValue::F64(f64::from_le_bytes(value.try_into().unwrap())),
            DataType::UTF8 { .. } => ColumnValue::String(String::from_utf8(value.to_vec()).unwrap()),
            DataType::VARBINARY { .. } => ColumnValue::Bytes(value.to_vec()),
            DataType::BUFFER { .. } => ColumnValue::Bytes(value.to_vec()),
        }
    }
}

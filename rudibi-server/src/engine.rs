use std::collections::HashMap;

use crate::dtype::*;
use crate::query::{Bool, Value};
use crate::storage::{DiskStorage, InMemoryStorage, RowId, ScanItem, Storage};

#[derive(Debug, PartialEq)]
pub enum DbError {
    TableNotFound(String),
    TableAlreadyExists(String),
    EmptyTableSchema,
    ColumnNotFound(String),
    InvalidColumnCount { expected: usize, got: usize },
    RowSizeExceeded { got: usize, max: usize },
    RowSizeTooSmall { got: usize, min: usize },
    ColumnSizeOutOfBounds { column: String, got: usize, min: usize, max: usize },

    InputError(String),
    QueryError(TypeError),

    UnsupportedOperation(String),
    DatabaseIntegrityError(String)
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub dtype: DataType,
}

impl Column {
    pub fn new(name: &str, dtype: DataType) -> Column {
        Column { name: name.to_string(), dtype }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: HashMap<String, (usize, Column)>,
    pub column_layout: Vec<Column>,
    pub min_row_size: usize,
    pub max_row_size: usize,
}

impl Table {

    pub fn new(name: &str, schema: Vec<Column>) -> Table {
        Table {
            name: name.to_string(),
            min_row_size: schema.iter().map(|c| c.dtype.min_size()).sum(),
            max_row_size: schema.iter().map(|c| c.dtype.max_size()).sum(),
            columns: schema.iter().enumerate().map(|(i, c)| (c.name.clone(), (i, c.clone()))).collect(),
            column_layout: schema,
        }
    }

    // Projecting columns in select clauses, filters, etc.
    // Seen as projecting input columns to schema
    pub fn project_to_schema_optional(&self, columns: &[&str]) -> Result<Vec<usize>, DbError> {
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
    pub fn project_from_schema_required(&self, columns: &[&str]) -> Result<Vec<usize>, DbError> {
        if columns.len() != self.column_layout.len() {
            // FIXME: Better error here. Missing required column.
            return Err(DbError::InvalidColumnCount { expected: self.column_layout.len(), got: columns.len() });
        }
        // FIXME: O(n^2) check
        let mut indices = Vec::with_capacity(self.column_layout.len());
        for col in &self.column_layout {
            // FIXME: Better error here. Missing required column.
            let source_idx = columns.iter().position(|c| c == &col.name)
                .ok_or_else(|| DbError::ColumnNotFound(col.name.clone()))?;
            indices.push(source_idx);
        }
        Ok(indices)
    }

    fn require_column<'schema>(&'schema self, name: &'_ str) -> Result<(usize, &'schema Column), DbError> {
        self.columns.get(name)
            .map(|(i, col)| (i.clone(), col))
            .ok_or_else(|| DbError::ColumnNotFound(name.to_string()))
    }

    fn validate_input(&self, row: &Row, column_mapping: &Vec<usize>) -> Result<(), DbError> {
        // Validate the number of columns
        let input_offsets = row.offsets.len();
        let input_columns = input_offsets - 1;

        // Probably not needed here
        // TODO: allow partial inserts for optional columns
        if input_columns != column_mapping.len(){
            return Err(DbError::InvalidColumnCount { expected: self.column_layout.len(), got: input_columns }) ;
        }
        
        // Validate the row size
        let input_size = row.data.len();
        if input_size > self.max_row_size {
            return Err(DbError::RowSizeExceeded { got: input_size, max: self.max_row_size });
        }
        if input_size < self.min_row_size {
            return Err(DbError::RowSizeTooSmall { got: input_size, min: self.min_row_size });
        }

        // Validate each column in schema for size in input
        for (idx, col) in self.column_layout.iter().enumerate() {
            let input_col_idx = column_mapping[idx];
            let input_col = row.get_column(input_col_idx);
            let input_col_size = input_col.len();
            let col_min = col.dtype.min_size();
            let col_max = col.dtype.max_size();
            if input_col_size < col_min || input_col_size > col_max {
                return Err(DbError::ColumnSizeOutOfBounds { column: col.name.clone(), got: input_col_size, min: col_min, max: col_max });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub data: Vec<u8>,        // Contiguous buffer holding all column data
    pub offsets: Vec<usize>,  // Start offsets for each column, plus end of last column
}

impl Row {
    
    pub fn of_columns(columns: &[&[u8]]) -> Row {
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
        Row { data, offsets }
    }

    pub fn get_column(&self, col_idx: usize) -> &[u8] {
        let start = self.offsets[col_idx];
        let end = self.offsets[col_idx + 1];
        return &self.data[start..end];
    }
}

#[derive(Debug)]
pub enum Filter {
    Equal { column: String, value: Vec<u8> },
    GreaterThan { column: String, value: Vec<u8> },
    LessThan { column: String, value: Vec<u8> },
}

#[derive(Clone)]
pub enum StorageCfg {
    InMemory,
    Disk { path: String },
}

pub struct Database {
    schemas: HashMap<String, Table>,
    storage: HashMap<String, Box<dyn Storage>>
}

pub struct FilterContext<'schema, 'row> {
    schema: &'schema Table,
    item: &'row ScanItem<'row>,
}

impl<'schema, 'row, 'ctx> FilterContext<'schema, 'row> where 
    'ctx: 'schema + 'row {
    fn execute_binop(&self, left: &'ctx Value<'ctx>, right: &'ctx Value<'ctx>, op: fn(&ColumnValue<'row>, &ColumnValue<'row>) -> Result<bool, TypeError>) -> Result<bool, DbError> {
        op(&self.resolve_value(&left)?, &self.resolve_value(&right)?).map_err(|err| DbError::QueryError(err))
    }

    fn resolve_value(&self, val: &'ctx Value<'ctx>) -> Result<ColumnValue<'row>, DbError> {
        match val {
            Value::ColumnRef(column_name) => {
                let (col_idx, col) = self.schema.require_column(&column_name)?;
                let col_value = self.item.row_content.get_column(col_idx.clone());
                canonical_column(&col.dtype, col_value)
                    .map_err(|_| DbError::DatabaseIntegrityError(
                        format!("Column {} at RowId={} in {} cannot be represented as data type {:?}", &column_name, &self.item.row_id, &self.schema.name, &col.dtype))
                    )
            },
            Value::Const(column_value) => Ok(*column_value),
        }
    }
}

fn filter_row_new(schema: &Table, item: &ScanItem, filter: &Bool) -> Result<bool, DbError> {
    let ctx = FilterContext { schema, item };
    let res = match filter {
        Bool::True => true,
        Bool::False => false,
        
        Bool::Eq(left, right) => ctx.execute_binop(left, right, ColumnValue::eq)?,
        Bool::Neq(left, right) => ctx.execute_binop(left, right, ColumnValue::neq)?,
        Bool::Gt(left, right) => ctx.execute_binop(left, right, ColumnValue::gt)?,
        Bool::Gte(left, right) => ctx.execute_binop(left, right, ColumnValue::gte)?,
        Bool::Lt(left, right) => ctx.execute_binop(left, right, ColumnValue::lt)?,
        Bool::Lte(left, right) => ctx.execute_binop(left, right, ColumnValue::lte)?,
        Bool::And(left, right) => filter_row_new(schema, item, left)? & filter_row_new(schema, item, right)?,
        Bool::Or(left, right) => filter_row_new(schema, item, left)? | filter_row_new(schema, item, right)?,
        Bool::Xor(left, right) => filter_row_new(schema, item, left)? ^ filter_row_new(schema, item, right)?,
        Bool::Not(inner) => !filter_row_new(schema, item, inner)?,
    };
    Ok(res)
}

impl Database {
    pub fn new() -> Database {
        Database {
            schemas: HashMap::new(),
            storage: HashMap::new(),
        }
    }

    pub fn new_table(&mut self, new_table: &Table, storage_cfg: StorageCfg) -> Result<(), DbError> {
        let table_name = &new_table.name;
        if let Some(_) = self.schemas.get(table_name) {
            return Err(DbError::TableAlreadyExists(table_name.clone()));
        }

        if new_table.column_layout.is_empty() {
            return Err(DbError::EmptyTableSchema);
        }

        self.schemas.insert(table_name.to_owned(), new_table.clone());

        let storage: Box<dyn Storage> = match storage_cfg {
            StorageCfg::InMemory => Box::new(InMemoryStorage::new(new_table.clone())),
            StorageCfg::Disk { path } => Box::new(DiskStorage::new(new_table.clone(), &path)),
        };

        let old_storage = self.storage.insert(table_name.to_owned(), storage);
        if old_storage.is_some() {
            // TODO: What to do in this case?
            return Err(DbError::TableAlreadyExists(table_name.clone()));
        }
        return Ok(())
    }

    pub fn insert(&mut self, table_name: &str, columns: &[&str], what: &[Row]) -> Result<usize, DbError> {
        let schema = self.schema_for(&table_name)?;
        let column_mapping = schema.project_from_schema_required(columns)?;

        for row in what.iter().cloned() {
            schema.validate_input(&row, &column_mapping)?;
        }

        let storage = self.mut_storage_for(&table_name)?;
        storage.store(&what, &column_mapping);
        
        // Maybe return it from storage?
        let stored = what.len();
        Ok(stored)
    }

    pub fn select_old(&self, table_name: &str, columns: &[&str], filters: &[Filter]) -> Result<Vec<Row>, DbError> {
        let schema = self.schema_for(table_name)?;
        let storage = self.storage_for(table_name)?;

        // Validate and project columns
        let column_mapping = schema.project_to_schema_optional(columns)?;

        // TODO: Some mechanism of reporting / logging internal assertions
        assert!(column_mapping.len() == columns.len(), "Column mapping should match the number of columns requested");

        let filter_columns = Self::extract_filtered_columns(filters);
        // TODO: Mapping of filters to column IDs is unused. Internally this will use string mapping.
        // Validate filter columns
        schema.project_to_schema_optional(&filter_columns)?;
    
        // Filter and map rows
        let mut results = Vec::new();
        for item in storage.scan() {
            if self.filter_row(&schema, &item, &filters)? {
                let mut selected_row = Vec::new();
                for proj_col in &column_mapping {
                    // FIXME: Cloning
                    selected_row.push(item.row_content.get_column(proj_col.clone()));
                }
                let projected = Row::of_columns(&selected_row);
                results.push(projected);
            }
        }
        Ok(results)
    }

    pub fn select_new(&self, values: &[Value], table: &str, filter: &Bool) -> Result<Vec<Row>, DbError> {
        let schema = self.schema_for(&table)?;
        let storage = self.storage_for(&table)?;

        // Validate and project columns
        let mut result_columns = Vec::with_capacity(values.len());
        for val in values {
            if let Value::ColumnRef(col_name) = val {
                result_columns.push(col_name.clone());
            } else {
                return Err(DbError::UnsupportedOperation(format!("Selecting values other than column references not supported {:?}", val)));
            }
        }

        let column_mapping = schema.project_to_schema_optional(&result_columns)?;

        // TODO: Some mechanism of reporting / logging internal assertions
        assert!(column_mapping.len() == result_columns.len(), "Column mapping should match the number of columns requested");

        let filter_columns = crate::query::parse_filter_columns(&filter);
        // TODO: Mapping of filters to column IDs is unused. Internally this will use string mapping.
        // Validate filter columns
        schema.project_to_schema_optional(&filter_columns)?;
    
        // Filter and map rows
        let mut results = Vec::new();
        for item in storage.scan() {
            if filter_row_new(&schema, &item, &filter)? {
                let mut selected_row = Vec::new();
                for proj_col in &column_mapping {
                    // FIXME: Cloning
                    selected_row.push(item.row_content.get_column(proj_col.clone()));
                }
                let projected = Row::of_columns(&selected_row);
                results.push(projected);
            }
        }
        Ok(results)
    }

    pub fn delete(&mut self, table_name: &str, filters: &[Filter]) -> Result<usize, DbError> {
        let schema = self.schema_for(table_name)?;

        // Validate filter columns
        let filter_columns = Self::extract_filtered_columns(filters);
        schema.project_to_schema_optional(&filter_columns)?;

        // Filter rows to remove
        let mut to_remove: Vec<RowId> = Vec::new();
        for item in self.storage_for(table_name)?.scan() {
            if self.filter_row(&schema, &item, &filters)? { to_remove.push(item.row_id); }
        }

        // Execute removal
        let removed = to_remove.len();
        // FIXME: Mutable borrow, again - borrow checker, storage.as_mut() doesn't work
        self.mut_storage_for(table_name)?.delete_rows(to_remove);
        Ok(removed)
    }

    fn extract_filtered_columns(filters: &[Filter]) -> Vec<&str> {
        filters.iter().map(|f| match f {
            Filter::Equal { column, .. } => column.as_str(),
            Filter::GreaterThan { column, .. } => column.as_str(),
            Filter::LessThan { column, .. } => column.as_str(),
        }).collect()
    }

    pub fn schema_for(&self, table_name: &str) -> Result<&Table, DbError> {
        self.schemas
            .get(table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_string()))
    }

    fn storage_for(&self, table_name: &str) -> Result<&Box<dyn Storage>, DbError> {
        self.storage
            .get(table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_string()))
    }

    fn mut_storage_for(&mut self, table_name: &str) -> Result<&mut Box<dyn Storage>, DbError> {
        self.storage
            .get_mut(table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_string()))
    }

    // TODO: This probably should go somewhere else
    fn filter_row(&self, schema: &Table, item: &ScanItem, filters: &[Filter]) -> Result<bool, DbError> {
        for filter in filters {
            let (column, value) = match filter {
                Filter::Equal { column, value } => (column, value),
                Filter::GreaterThan { column, value } => (column, value),
                Filter::LessThan { column, value } => (column, value),
            };
            let (col_idx, col_scheme) = schema.require_column(column)?;
            
            // TODO: add implicit casting
            let col_value = canonical_column(&col_scheme.dtype, item.row_content.get_column(col_idx))
                .map_err(|_| DbError::DatabaseIntegrityError(
                    format!("Column {} at RowId={} in {} cannot be represented as data type {:?}", column, item.row_id, &schema.name, &col_scheme.dtype))
                )?;
            let filter_val = canonical_column(&col_scheme.dtype, value)
                .map_err(|_| DbError::InputError(format!("Cannot convert value of filter {:?} to {:?}", filter, &col_scheme.dtype)))?;
    
            let passes = match filter {
                Filter::Equal { .. } => match (col_value, filter_val) {
                    (ColumnValue::U32(col_val), ColumnValue::U32(filter_val)) => col_val == filter_val,
                    (ColumnValue::F64(col_val), ColumnValue::F64(filter_val)) => col_val == filter_val,
                    (ColumnValue::UTF8(col_val), ColumnValue::UTF8(filter_val)) => col_val == filter_val,
                    _ => return Err(DbError::UnsupportedOperation(format!(
                        "Equal filter not supported for data type {:?}", col_scheme.dtype
                    ))),
                },
                Filter::GreaterThan { .. } => match (col_value, filter_val) {
                    (ColumnValue::U32(col_val), ColumnValue::U32(filter_val)) => col_val > filter_val,
                    (ColumnValue::F64(col_val), ColumnValue::F64(filter_val)) => col_val > filter_val,
                    _ => return Err(DbError::UnsupportedOperation(format!(
                        "GreaterThan filter not supported for data type {:?}", col_scheme.dtype
                    ))),
                },
                Filter::LessThan { .. } => match (col_value, filter_val) {
                    (ColumnValue::U32(col_val), ColumnValue::U32(filter_val)) => col_val < filter_val,
                    (ColumnValue::F64(col_val), ColumnValue::F64(filter_val)) => col_val < filter_val,
                    _ => return Err(DbError::UnsupportedOperation(format!(
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
}


// Data types available in the database
// The functionality of value comparisons and casts should go here

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
pub enum ColumnValue {
    U32(u32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug)]
pub enum TypeError {
    ConversionError
}

pub fn convert_filter_value(value: &[u8], dtype: &DataType) -> ColumnValue {
    match dtype {
        DataType::U32 => ColumnValue::U32(u32::from_le_bytes(value.try_into().unwrap())),
        DataType::F64 => ColumnValue::F64(f64::from_le_bytes(value.try_into().unwrap())),
        DataType::UTF8 { .. } => ColumnValue::String(String::from_utf8(value.to_vec()).unwrap()),
        DataType::VARBINARY { .. } => ColumnValue::Bytes(value.to_vec()),
        DataType::BUFFER { .. } => ColumnValue::Bytes(value.to_vec()),
    }
}

pub fn canonical_column(dtype: &DataType, data: &[u8]) -> Result<ColumnValue, TypeError> {
    match dtype {
        DataType::U32 => { Ok(ColumnValue::U32(u32::from_le_bytes(data.try_into().map_err(|_| TypeError::ConversionError)?))) }
        DataType::F64 => { Ok(ColumnValue::F64(f64::from_le_bytes(data.try_into().map_err(|_| TypeError::ConversionError)?))) }
        DataType::UTF8 { .. } => Ok(ColumnValue::String(
            String::from_utf8(data.to_vec()).map_err(|_| TypeError::ConversionError)?,
        )),
        DataType::VARBINARY { .. } => Ok(ColumnValue::Bytes(data.to_vec())),
        DataType::BUFFER { length } => {
            if data.len() != *length {
                return Err(TypeError::ConversionError);
            }
            Ok(ColumnValue::Bytes(data.to_vec()))
        }
    }
}
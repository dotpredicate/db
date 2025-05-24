
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
pub enum TypeError {
    ConversionError,
    InvalidArgType(String, ColumnValue, ColumnValue)
}

// TODO: Use pointers here!
#[derive(Debug, Clone)]
pub enum ColumnValue {
    U32(u32),
    F64(f64),
    UTF8(String),
    Bytes(Vec<u8>),
}

impl ColumnValue {
    pub fn ne(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 != r0,
            (Self::F64(l0), Self::F64(r0)) => l0 != r0,
            (Self::UTF8(l0), Self::UTF8(r0)) => l0 != r0,
            // FIXME: Remove value clones here, describe just the types
            _ => return Err(TypeError::InvalidArgType("ne".to_string(), self.clone(), other.clone())),
        };
        Ok(res)
    }

    pub fn eq(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 == r0,
            (Self::F64(l0), Self::F64(r0)) => l0 == r0,
            (Self::UTF8(l0), Self::UTF8(r0)) => l0 == r0,
            // FIXME: Remove value clones here, describe just the types
            _ => return Err(TypeError::InvalidArgType("eq".to_string(), self.clone(), other.clone())),
        };
        Ok(res)
    }

    pub fn gt(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 > r0,
            (Self::F64(l0), Self::F64(r0)) => l0 > r0,
            // FIXME: Remove value clones here, describe just the types
            _ => return Err(TypeError::InvalidArgType("gt".to_string(), self.clone(), other.clone())),
        };
        Ok(res)
    }

    pub fn gte(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 >= r0,
            (Self::F64(l0), Self::F64(r0)) => l0 >= r0,
            // FIXME: Remove value clones here, describe just the types
            _ => return Err(TypeError::InvalidArgType("gte".to_string(), self.clone(), other.clone())),
        };
        Ok(res)
    }

    pub fn lt(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 < r0,
            (Self::F64(l0), Self::F64(r0)) => l0 < r0,
            // FIXME: Remove value clones here, describe just the types
            _ => return Err(TypeError::InvalidArgType("lt".to_string(), self.clone(), other.clone())),
        };
        Ok(res)
    }

    pub fn lte(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 <= r0,
            (Self::F64(l0), Self::F64(r0)) => l0 <= r0,
            // FIXME: Remove value clones here, describe just the types
            _ => return Err(TypeError::InvalidArgType("lte".to_string(), self.clone(), other.clone())),
        };
        Ok(res)
    }
}


// Panicking implementation of `eq`
// Itended for use in tests
impl PartialEq for ColumnValue {
    fn eq(&self, other: &Self) -> bool { ColumnValue::eq(self, other).unwrap() }
}

pub fn convert_filter_value(value: &[u8], dtype: &DataType) -> Result<ColumnValue, TypeError> {
    let result = match dtype {
        DataType::U32 => ColumnValue::U32(u32::from_le_bytes(value.try_into().map_err(|_| TypeError::ConversionError)?)),
        DataType::F64 => ColumnValue::F64(f64::from_le_bytes(value.try_into().map_err(|_| TypeError::ConversionError)?)),
        DataType::UTF8 { .. } => ColumnValue::UTF8(String::from_utf8(value.to_vec()).map_err(|_| TypeError::ConversionError)?),
        DataType::VARBINARY { .. } => ColumnValue::Bytes(value.to_vec()),
        DataType::BUFFER { .. } => ColumnValue::Bytes(value.to_vec()),
    };
    Ok(result)
}

pub fn canonical_column(dtype: &DataType, data: &[u8]) -> Result<ColumnValue, TypeError> {
    match dtype {
        DataType::U32 => { Ok(ColumnValue::U32(u32::from_le_bytes(data.try_into().map_err(|_| TypeError::ConversionError)?))) }
        DataType::F64 => { Ok(ColumnValue::F64(f64::from_le_bytes(data.try_into().map_err(|_| TypeError::ConversionError)?))) }
        DataType::UTF8 { .. } => Ok(ColumnValue::UTF8(
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

// Data types available in the database
// The functionality of value comparisons and casts should go here

use std::str;

#[derive(Debug, Clone, PartialEq)]
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
    InvalidArgType(String, DataType, DataType)
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnValue<'a> {
    U32(u32),
    F64(f64),
    UTF8(&'a str),
    Bytes(&'a [u8]),
}

impl<'a> Into<DataType> for &ColumnValue<'a> {
    fn into(self) -> DataType {
        match self {
            ColumnValue::U32(_) => DataType::U32,
            ColumnValue::F64(_) => DataType::F64,
            ColumnValue::UTF8(val) => DataType::UTF8 { max_bytes: val.len() },
            ColumnValue::Bytes(val) => DataType::BUFFER { length: val.len() },
        }
    }
}

impl<'cmp> ColumnValue<'cmp> {

    #[inline(always)]
    pub fn eq(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 == r0,
            (Self::F64(l0), Self::F64(r0)) => l0 == r0,
            (Self::UTF8(l0), Self::UTF8(r0)) => l0 == r0,
            _ => return Err(TypeError::InvalidArgType("eq".to_string(), self.into(), other.into())),
        };
        Ok(res)
    }

    #[inline(always)]
    pub fn neq(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 != r0,
            (Self::F64(l0), Self::F64(r0)) => l0 != r0,
            (Self::UTF8(l0), Self::UTF8(r0)) => l0 != r0,
            _ => return Err(TypeError::InvalidArgType("ne".to_string(), self.into(), other.into())),
        };
        Ok(res)
    }

    #[inline(always)]
    pub fn gt(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 > r0,
            (Self::F64(l0), Self::F64(r0)) => l0 > r0,
            _ => return Err(TypeError::InvalidArgType("gt".to_string(), self.into(), other.into())),
        };
        Ok(res)
    }

    #[inline(always)]
    pub fn gte(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 >= r0,
            (Self::F64(l0), Self::F64(r0)) => l0 >= r0,
            _ => return Err(TypeError::InvalidArgType("gte".to_string(), self.into(), other.into())),
        };
        Ok(res)
    }

    #[inline(always)]
    pub fn lt(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 < r0,
            (Self::F64(l0), Self::F64(r0)) => l0 < r0,
            _ => return Err(TypeError::InvalidArgType("lt".to_string(), self.into(), other.into())),
        };
        Ok(res)
    }

    #[inline(always)]
    pub fn lte(&self, other: &Self) -> Result<bool, TypeError> {
        let res = match (self, other) {
            (Self::U32(l0), Self::U32(r0)) => l0 <= r0,
            (Self::F64(l0), Self::F64(r0)) => l0 <= r0,
            _ => return Err(TypeError::InvalidArgType("lte".to_string(), self.into(), other.into())),
        };
        Ok(res)
    }
}

// Panicking implementation of `eq`
// Itended for use in tests
#[cfg(test)]
impl<'a> PartialEq for ColumnValue<'a> {
    fn eq(&self, other: &Self) -> bool { ColumnValue::eq(self, other).unwrap() }
}

// TODO: These byte conversions should be moved to `serial`
#[inline(always)]
pub fn canonical_column<'a>(dtype: &'_ DataType, data: &'a [u8]) -> Result<ColumnValue<'a>, TypeError> {
    match dtype {
        DataType::U32 => { Ok(ColumnValue::U32(u32::from_le_bytes(data.try_into().map_err(|_| TypeError::ConversionError)?))) }
        DataType::F64 => { Ok(ColumnValue::F64(f64::from_le_bytes(data.try_into().map_err(|_| TypeError::ConversionError)?))) }
        DataType::UTF8 { .. } => Ok(ColumnValue::UTF8(str::from_utf8(data).map_err(|_| TypeError::ConversionError)?)),
        DataType::VARBINARY { .. } => Ok(ColumnValue::Bytes(&data)),
        DataType::BUFFER { length } => {
            if data.len() != *length {
                return Err(TypeError::ConversionError);
            }
            Ok(ColumnValue::Bytes(&data))
        }
    }
}
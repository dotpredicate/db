
use crate::dtype::ColumnValue;

#[derive(Debug)]
pub enum Value<'a> {
    // Primitive value types
    ColumnRef(&'a str),
    Const(ColumnValue<'a>),

    // BinOps
    // Add(Box<Value<'a>>, Box<Value<'a>>),
    // Sub(Box<Value<'a>>, Box<Value<'a>>),
    // Mul(Box<Value<'a>>, Box<Value<'a>>),
    // Div(Box<Value<'a>>, Box<Value<'a>>)
}

// impl ops::Add<Value> for Value {
//     type Output = Self;
//     fn add(self, rhs: Value) -> Self::Output { Self::Add(Box::new(self), Box::new(rhs)) }
// }

// impl ops::Sub<Value> for Value {
//     type Output = Self;
//     fn sub(self, rhs: Value) -> Self::Output { Self::Sub(Box::new(self), Box::new(rhs)) }
// }

// impl ops::Mul<Value> for Value {
//     type Output = Self;
//     fn mul(self, rhs: Value) -> Self::Output { Self::Mul(Box::new(self), Box::new(rhs)) }
// }

// impl ops::Div<Value> for Value {
//     type Output = Self;
//     fn div(self, rhs: Value) -> Self::Output { Self::Div(Box::new(self), Box::new(rhs)) }
// }

pub enum Bool<'a> {
    True,
    False,

    Eq(Value<'a>, Value<'a>),
    Neq(Value<'a>, Value<'a>),
    Gt(Value<'a>, Value<'a>),
    Gte(Value<'a>, Value<'a>),
    Lt(Value<'a>, Value<'a>),
    Lte(Value<'a>, Value<'a>),

    And(Box<Bool<'a>>, Box<Bool<'a>>),
    Or(Box<Bool<'a>>, Box<Bool<'a>>),
    Xor(Box<Bool<'a>>, Box<Bool<'a>>),
    Not(Box<Bool<'a>>),
}

impl<'a> Bool<'a> {
    pub fn or(self, other: Bool<'a>) -> Bool<'a> {
        Bool::Or(Box::new(self), Box::new(other))
    }
    
    pub fn and(self, other: Bool<'a>) -> Bool<'a> {
        Bool::And(Box::new(self), Box::new(other))
    }
}

fn collect_value_columns<'a>(value: &'a Value) -> Vec<&'a str> {
    match value {
        Value::ColumnRef(col) => vec![col],
        Value::Const(_) => vec![],
        // Value::Add(left, right) |
        // Value::Sub(left, right) |
        // Value::Mul(left, right) |
        // Value::Div(left, right) => {
        //     let mut left_cols = collect_value_columns(left);
        //     left_cols.extend(collect_value_columns(right));
        //     left_cols
        // }
    }
}

pub fn collect_filter_columns<'a>(bool_expr: &'a Bool) -> Vec<&'a str> {
    match bool_expr {
        Bool::True | Bool::False => vec![],
        Bool::Eq(left, right) |
        Bool::Neq(left, right) |
        Bool::Gt(left, right) |
        Bool::Gte(left, right) |
        Bool::Lt(left, right) |
        Bool::Lte(left, right) => {
            let mut cols = collect_value_columns(left);
            cols.extend(collect_value_columns(right));
            cols
        },
        Bool::And(left, right) |
        Bool::Or(left, right) |
        Bool::Xor(left, right) => {
            let mut left_cols = collect_filter_columns(left);
            left_cols.extend(collect_filter_columns(right));
            left_cols
        },
        Bool::Not(expr) => collect_filter_columns(expr),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_collect_columns() {
        let query = Bool::And(
            Box::new(Bool::Eq(
                Value::ColumnRef("age"),
                Value::Const(ColumnValue::U32(20)),
            )),
            Box::new(Bool::Gt(
                Value::ColumnRef("salary"),
                Value::Const(ColumnValue::U32(1000))
            )),
        );

        let columns = collect_filter_columns(&query);
        assert_eq!(columns, vec!["age", "salary"]);
    }

}
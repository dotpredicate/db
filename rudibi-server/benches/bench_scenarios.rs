
use rudibi_server::dtype::{DataType, ColumnValue::*};
use rudibi_server::query::{Value::*, Bool::*};
use rudibi_server::serial::Serializable;
use rudibi_server::engine::*;

use divan::Bencher;

pub fn batch_store_u32(bencher: Bencher, n: u32, storage: fn() -> StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[&i.serialized()]))
            .collect();
        (db, rows)
    }).bench_values(|(mut db, rows)| {
        db.insert("TestTable", &["id"], &rows).unwrap();
    });
}

pub fn select_half_filter_lt(bencher: divan::Bencher, n: u32, storage: fn() -> StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[i.serialized()]))
            .collect();
        db.insert("TestTable", &["id"], &rows).unwrap();
        return (db, (n/2));
    }).bench_values(|(db, max)| {
        db.select(&[ColumnRef("id")], "TestTable", &Lt(ColumnRef("id"), Const(U32(max)))).unwrap();
    });
}

pub fn select_all(bencher: divan::Bencher, n: u32, storage: fn() -> StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[i.serialized()]))
            .collect();
        db.insert("TestTable", &["id"], &rows).unwrap();
        return db;
    }).bench_values(|db| {
        db.select(&[ColumnRef("id")], "TestTable", &True).unwrap();
    });
}


pub fn delete_all(bencher: divan::Bencher, n: u32, storage: fn() -> StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[i.serialized()]))
            .collect();
        db.insert("TestTable", &["id"], &rows).unwrap();
        return db;
    }).bench_values(|mut db| {
        db.delete("TestTable", &True).unwrap();
    });
}

pub fn delete_first_half(bencher: divan::Bencher, n: u32, storage: fn() -> StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[&i.serialized()]))
            .collect();
        db.insert("TestTable", &["id"], &rows).unwrap();
        return (db, (n/2));
    }).bench_values(|(mut db, max)| {
        db.delete("TestTable", &Lt(ColumnRef("id"), Const(U32(max)))).unwrap();
    });
}
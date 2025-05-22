use rudibi_server::engine::*;

use divan::Bencher;

pub fn batch_store_u32(bencher: Bencher, n: u32, storage: StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage.clone()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[&i.to_le_bytes()]))
            .collect();
        (db, rows)
    }).bench_values(|(mut db, rows)| {
        db.insert(Insert::new("TestTable", &["id"], rows)).unwrap();
    });
}

pub fn select_half_filter_lt(bencher: divan::Bencher, n: u32, storage: StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage.clone()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[&i.to_le_bytes()]))
            .collect();
        db.insert(Insert::new("TestTable", &["id"], rows)).unwrap();
        return (db, (n/2).to_le_bytes().to_vec());
    }).bench_values(|(db, max)| {
        db.select(Select::new("TestTable", &["id"], vec![Filter::LessThan { column: "id".into(), value: max }])).unwrap();
    });
}

pub fn select_all(bencher: divan::Bencher, n: u32, storage: StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage.clone()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[&i.to_le_bytes()]))
            .collect();
        db.insert(Insert::new("TestTable", &["id"], rows)).unwrap();
        return db;
    }).bench_values(|db| {
        db.select(Select::new("TestTable", &["id"], vec![])).unwrap();
    });
}


pub fn delete_all(bencher: divan::Bencher, n: u32, storage: StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage.clone()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[&i.to_le_bytes()]))
            .collect();
        db.insert(Insert::new("TestTable", &["id"], rows)).unwrap();
        return db;
    }).bench_values(|mut db| {
        db.delete(Delete::new("TestTable", vec![])).unwrap();
    });
}

pub fn delete_first_half(bencher: divan::Bencher, n: u32, storage: StorageCfg) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&Table::new("TestTable", vec![Column::new("id", DataType::U32)]), storage.clone()).unwrap();

        let rows: Vec<Row> = (0..n)
            .map(|i| Row::of_columns(&[&i.to_le_bytes()]))
            .collect();
        db.insert(Insert::new("TestTable", &["id"], rows)).unwrap();
        return (db, (n/2).to_le_bytes().to_vec());
    }).bench_values(|(mut db, max)| {
        db.delete(Delete::new("TestTable", vec![Filter::LessThan { column: "id".into(), value: max }])).unwrap();
    });
}
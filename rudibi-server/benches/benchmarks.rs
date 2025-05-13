
use rudibi_server::engine::*;
use divan;

fn main() {
    // Run registered benchmarks.
    divan::main();
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn batch_store_u32(bencher: divan::Bencher, n: u32) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&TableSchema::new("TestTable", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();

        let rows: Vec<StoredRow> = (0..n)
            .map(|i| StoredRow::of_columns(&[&i.to_le_bytes()]))
            .collect();
        (db, rows)
    }).bench_values(|(mut db, rows)| {
        db.store(StoreCommand::new("TestTable", &["id"], rows)).unwrap();
    });
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_half_filter_lt(bencher: divan::Bencher, n: u32) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&TableSchema::new("TestTable", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();

        let rows: Vec<StoredRow> = (0..n)
            .map(|i| StoredRow::of_columns(&[&i.to_le_bytes()]))
            .collect();
        db.store(StoreCommand::new("TestTable", &["id"], rows)).unwrap();
        return (db, (n/2).to_le_bytes().to_vec());
    }).bench_values(|(db, max)| {
        db.get(GetCommand::new("TestTable", &["id"], vec![Filter::LessThan { column: "id".into(), value: max }])).unwrap();
    });
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_all(bencher: divan::Bencher, n: u32) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&TableSchema::new("TestTable", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();

        let rows: Vec<StoredRow> = (0..n)
            .map(|i| StoredRow::of_columns(&[&i.to_le_bytes()]))
            .collect();
        db.store(StoreCommand::new("TestTable", &["id"], rows)).unwrap();
        return db;
    }).bench_values(|db| {
        db.get(GetCommand::new("TestTable", &["id"], vec![])).unwrap();
    });

}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn delete_all(bencher: divan::Bencher, n: u32) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&TableSchema::new("TestTable", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();

        let rows: Vec<StoredRow> = (0..n)
            .map(|i| StoredRow::of_columns(&[&i.to_le_bytes()]))
            .collect();
        db.store(StoreCommand::new("TestTable", &["id"], rows)).unwrap();
        return db;
    }).bench_values(|mut db| {
        db.delete(DeleteCommand::new("TestTable", vec![])).unwrap();
    });
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    // Too slow for +100K
    // Runs for ~400ms for 50k
    // Runs for ~1.5s for 100K
    args = [1, 10, 100, 1_000, 10_000, 50_000]
)]
fn delete_first_half(bencher: divan::Bencher, n: u32) {
    bencher.with_inputs(|| { 
        let mut db = Database::new();
        db.new_table(&TableSchema::new("TestTable", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();

        let rows: Vec<StoredRow> = (0..n)
            .map(|i| StoredRow::of_columns(&[&i.to_le_bytes()]))
            .collect();
        db.store(StoreCommand::new("TestTable", &["id"], rows)).unwrap();
        return (db, (n/2).to_le_bytes().to_vec());
    }).bench_values(|(mut db, max)| {
        db.delete(DeleteCommand::new("TestTable", vec![Filter::LessThan { column: "id".into(), value: max }])).unwrap();
    });
}
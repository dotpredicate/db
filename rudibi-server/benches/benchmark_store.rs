
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
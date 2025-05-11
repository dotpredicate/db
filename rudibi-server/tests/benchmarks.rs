
#[cfg(test)]
mod tests {

    use rudibi_server::engine::*;
    
    #[test]
    fn batch_store_1_000_000_rows() {
        let mut db = Database::new();
        db.new_table(&TableSchema::new("TestTable", vec![ColumnSchema::new("id", DataType::U32)])).unwrap();

        let rows: Vec<StoredRow> = (0..1_000_000u32)
            .map(|i| StoredRow::of_columns(&[&i.to_le_bytes()]))
            .collect();

        let start = std::time::Instant::now();
        db.store(StoreCommand::new("TestTable", &["id"], rows)).unwrap();
        let duration = start.elapsed();

        println!("Time to store 1000 rows: {:?}", duration);
        assert!(duration.as_millis() < 200); // Rough performance check
    }
}
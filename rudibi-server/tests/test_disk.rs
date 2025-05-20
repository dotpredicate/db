
mod util;

use rudibi_server::engine::*;
use rudibi_server::storage::{DiskStorage, RowContent, Storage};

#[test]
fn test_single_column() {
    let mut storage = DiskStorage::new(TableSchema::new("Table", vec![ColumnSchema::new("id", DataType::U32)]), &util::random_temp_file());
    let rows = vec![StoredRow::of_columns(&[&1u32.to_le_bytes()])];
    storage.store(rows, &vec![0]);

    let read: Vec<RowContent> = storage.scan().map(|x| x.1).collect();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].get_column(0), &1u32.to_le_bytes());
}

#[test]
fn test_multiple_columns() {
    let mut storage = DiskStorage::new(util::fruits_schema(), &util::random_temp_file());
    let rows = vec![
        StoredRow::of_columns(&[&1u32.to_le_bytes(), &[1, 2, 3, 4]]),
        StoredRow::of_columns(&[&2u32.to_le_bytes(), &[5, 6, 7, 8]]),
    ];
    storage.store(rows, &vec![0, 1]);

    let read_rows: Vec<RowContent> = storage.scan().map(|x| x.1).collect();
    assert_eq!(read_rows.len(), 2);
    assert_eq!(read_rows[0].get_column(0), &1u32.to_le_bytes());
    assert_eq!(read_rows[0].get_column(1), &[1, 2, 3, 4]);
    assert_eq!(read_rows[1].get_column(0), &2u32.to_le_bytes());
    assert_eq!(read_rows[1].get_column(1), &[5, 6, 7, 8]);

}

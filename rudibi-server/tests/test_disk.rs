
use rudibi_server::dtype::DataType;
use rudibi_server::rows;
use rudibi_server::engine::*;
use rudibi_server::storage::{DiskStorage, RowContent, Storage};
use rudibi_server::testlib;

#[test]
fn test_single_column() {
    let mut storage = DiskStorage::new(Table::new("Table", vec![Column::new("id", DataType::U32)]), &testlib::random_temp_file());
    let rows = rows![[1u32]];
    storage.store(rows, &vec![0]);

    let read: Vec<RowContent> = storage.scan().map(|i| i.row_content).collect();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].get_column(0), &1u32.to_le_bytes());
}

#[test]
fn test_multiple_columns() {
    let mut storage = DiskStorage::new(testlib::fruits_schema(), &testlib::random_temp_file());
    let rows = rows![
        [1u32, [1, 2, 3, 4]],
        [2u32, [5, 6, 7, 8]]
    ];
    storage.store(rows, &vec![0, 1]);

    let read_rows: Vec<RowContent> = storage.scan().map(|i| i.row_content).collect();
    assert_eq!(read_rows.len(), 2);
    assert_eq!(read_rows[0].get_column(0), &1u32.to_le_bytes());
    assert_eq!(read_rows[0].get_column(1), &[1, 2, 3, 4]);
    assert_eq!(read_rows[1].get_column(0), &2u32.to_le_bytes());
    assert_eq!(read_rows[1].get_column(1), &[5, 6, 7, 8]);

}

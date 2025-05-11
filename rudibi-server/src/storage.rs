use crate::engine::{StoredRow, TableSchema};
use std::path::PathBuf;

// Not flexible and too small, but OK for now
pub type RowId = usize;

// Rust requires a concrete implementation in return types or something.
// This is a workaround.
pub struct TableIterator {
    iter: Box<dyn Iterator<Item = (RowId, StoredRow)>>,
}

impl Iterator for TableIterator {
    type Item = (RowId, StoredRow);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait Storage {
    fn new(schema: TableSchema) -> Self where Self: Sized;
    fn store(&mut self, rows: Vec<StoredRow>);
    fn scan(&self) -> TableIterator;
    fn delete_rows(&mut self, row_ids: Vec<RowId>);
}

pub struct InMemoryStorage {
    _schema: TableSchema, // FIXME: Use schema to save data in a more structured way
    content: Vec<StoredRow>,
}

impl Storage for InMemoryStorage {
    fn new(schema: TableSchema) -> Self {
        InMemoryStorage {
            _schema: schema,
            content: Vec::new(),
        }
    }

    fn store(&mut self, rows: Vec<StoredRow>) {
        self.content.extend(rows);
    }

    fn scan(&self) -> TableIterator {
        let cloned = self.content.clone(); // FIXME: Cloning EVERYTHING.
        TableIterator { iter: Box::new(cloned.into_iter().enumerate()) }
    }

    fn delete_rows(&mut self, mut row_ids: Vec<RowId>) {
        // Sorting in reverse order to avoid index shifting issues
        row_ids.sort_by(|a, b| b.cmp(a));
        for idx in row_ids {
            self.content.remove(idx);
        }
    }
}

pub struct DiskStorage {
    _schema: TableSchema,
    _file: PathBuf,
}

// TODO: Implement disk storage
impl Storage for DiskStorage {
    fn new(_schema: TableSchema) -> Self {
        unimplemented!()
    }

    fn store(&mut self, _rows: Vec<StoredRow>) {
        // Implement file writing later
        unimplemented!()
    }

    fn scan(&self) -> TableIterator {
        // Implement file scanning later
        // unimplemented!()
        TableIterator { iter: Box::new(Vec::new().into_iter()) }
    }

    fn delete_rows(&mut self, _row_ids: Vec<RowId>) {
        // Implement file deletion later
        unimplemented!()
    }
}
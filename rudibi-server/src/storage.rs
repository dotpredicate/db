use crate::engine::{StoredRow, TableSchema};
use std::path::PathBuf;

// Not flexible and too small, but OK for now
pub type RowId = usize;
type ScanItem<'a> = (RowId, RowContent<'a>);


pub struct RowContent<'a> {
    pub data: &'a [u8],
    pub offsets: &'a [usize],
}

impl RowContent<'_> {

    pub fn get_column(&self, col_idx: usize) -> &[u8] {
        let start = self.offsets[col_idx];
        let end = self.offsets[col_idx + 1];
        return &self.data[start..end];
    }
}

// Rust requires a concrete implementation in return types for traits or something.
// This is a workaround.
pub struct TableIterator<'a> {
    iter: Box<dyn Iterator<Item = ScanItem<'a>> + 'a>,
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = ScanItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait Storage {
    fn new(schema: TableSchema) -> Self where Self: Sized;
    fn store(&mut self, rows: Vec<StoredRow>, column_mapping: &Vec<usize>);
    fn scan(&self) -> TableIterator;
    fn delete_rows(&mut self, row_ids: Vec<RowId>);
}


pub struct InMemoryStorage {
    offsets_per_row: usize,
    _schema: TableSchema, // FIXME: Use schema to save data in a more structured way
    data: Vec<u8>,
    relative_column_offsets: Vec<usize>,
    row_data_starts: Vec<usize>,
}

impl Storage for InMemoryStorage {
    fn new(schema: TableSchema) -> Self {
        InMemoryStorage {
            offsets_per_row: schema.columns.len() + 1,
            _schema: schema,
            data: Vec::new(),
            relative_column_offsets: Vec::new(),
            row_data_starts: Vec::new(),
        }
    }

    fn store(&mut self, rows: Vec<StoredRow>, column_mapping: &Vec<usize>) {
        self.row_data_starts.reserve(rows.len());
        self.relative_column_offsets.reserve(rows.len() * self.offsets_per_row);
        for row in rows {
            let mut next_offset = 0;
            self.relative_column_offsets.push(next_offset);
                
            let row_start = self.data.len();
            self.row_data_starts.push(row_start);

            for i in column_mapping {
                let col = row.get_column(*i);
                self.data.extend_from_slice(col);
                next_offset += col.len();
                self.relative_column_offsets.push(next_offset);
            }
        }

    }

    fn delete_rows(&mut self, mut row_ids: Vec<RowId>) {
        // Sorting in reverse order to avoid index shifting issues
        row_ids.sort_by(|a, b| b.cmp(a));
        for row_id in row_ids {
            if row_id < self.row_data_starts.len() {
                let start = self.row_data_starts[row_id];
                let end = if row_id + 1 < self.row_data_starts.len() {
                    self.row_data_starts[row_id + 1]
                } else {
                    // Case for the last row
                    self.data.len()
                };
                self.data.drain(start..end);
                let deleted_length = end - start;
                self.row_data_starts.remove(row_id);
                // Shift row starts
                // TODO: SLOW
                for i in row_id..self.row_data_starts.len() {
                    if self.row_data_starts[i] > start {
                        self.row_data_starts[i] -= deleted_length;
                    }
                }

                let offset_start = row_id * self.offsets_per_row;
                let offset_end = (row_id + 1) * self.offsets_per_row;
                self.relative_column_offsets.drain(offset_start..offset_end);
            }
        }
    }

    fn scan(&self) -> TableIterator {
        TableIterator {
            iter: Box::new(
                (0..self.row_data_starts.len()).map(move |row_id| {
                    let row_content = self.get_row_content(row_id);
                    (row_id, row_content.unwrap())
                })
            )
        }
    }
}

impl InMemoryStorage {

    fn get_row_content(&self, row_id: RowId) -> Option<RowContent> {
        if row_id < self.row_data_starts.len() {
            let start = self.row_data_starts[row_id];
            let end = if row_id + 1 < self.row_data_starts.len() {
                self.row_data_starts[row_id + 1]
            } else {
                // Case for the last row
                self.data.len()
            };
            let data = &self.data[start..end];
            let offsets_start = row_id * self.offsets_per_row;
            let offsets_end = (row_id + 1) * self.offsets_per_row;
            let offsets = &self.relative_column_offsets[offsets_start..offsets_end];
            Some(RowContent { data, offsets })
        } else {
            None
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

    fn store(&mut self, _rows: Vec<StoredRow>, _column_mapping: &Vec<usize>) {
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
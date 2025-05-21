use crate::engine::{StoredRow, TableSchema};

// Not flexible and too small, but OK for now
pub type RowId = usize;
type ScanItem<'a> = (RowId, RowContent<'a>);


#[derive(Debug)]
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
type RowIter<'a> = Box<dyn Iterator<Item = ScanItem<'a>> + 'a>;

pub struct TableIterator<'a> {
    iter: RowIter<'a>,
}

impl<'a> TableIterator<'a> {
    pub fn new(iter: RowIter<'a>) -> Self {
        TableIterator { iter }
    }
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = ScanItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait Storage {
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
        TableIterator::new(Box::new(
            (0..self.row_data_starts.len()).map(move |row_id| {
                let row_content = self.get_row_content(row_id);
                (row_id, row_content.unwrap())
            })
        ))
    }
}

impl InMemoryStorage {

    pub fn new(schema: TableSchema) -> Self {
        InMemoryStorage {
            offsets_per_row: schema.columns.len() + 1,
            _schema: schema,
            data: Vec::new(),
            relative_column_offsets: Vec::new(),
            row_data_starts: Vec::new(),
        }
    }

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


use std::io::{Write, BufWriter, Read, BufReader};
use std::fs::File;
pub struct DiskStorage {
    schema: TableSchema,
    file_path: String,
    file: BufWriter<File>,
}

type MagicType = [u8; 4];
const HEADER_MAGIC: &MagicType = b"RDBI";

impl DiskStorage {

    pub fn new(schema: TableSchema, path: &str) -> Self {
        let file = File::create(path).expect("Failed to create file");
        let mut writer = BufWriter::new(file);

        writer.write_all(HEADER_MAGIC).expect("Failed to write magic number");
        writer.write_all(&(schema.columns.len() + 1 as usize).to_le_bytes()).expect("Failed to write offsets per row");

        DiskStorage {
            schema,
            file_path: path.to_string(),
            file: writer,
        }
    }
}

impl Drop for DiskStorage {
    fn drop(&mut self) {
        self.file.flush().expect("Failed to flush file");
    }
}

// TODO: Implement disk storage
impl Storage for DiskStorage {
    
    fn store(&mut self, rows: Vec<StoredRow>, column_mapping: &Vec<usize>) {
        // println!("DiskStorage::store - start - storing {} rows", rows.len());
        // TODO: Storage error handling
        // TODO: This is probably not optimal
        for row in rows {
            // println!("\nRow: {:?}", row);
            // println!("Column mapping: {:?}", column_mapping);
            
            // Column offsets
            // FIXME: This is bad.
            let mut last_offset: usize = 0;
            self.file.write(&last_offset.to_le_bytes()).expect("Failed to write initial column offset");
            for next_col in column_mapping {
                let sz = row.offsets[*next_col + 1] - row.offsets[*next_col];
                // println!("Last offset: {last_offset}, size: {sz}");
                last_offset += sz;
                self.file
                    .write(&last_offset.to_le_bytes())
                    .expect("Failed to write offset");
            }
            
            // Row content length
            self.file
                .write_all(&row.data.len().to_le_bytes())
                .expect("Failed to write content length");

            // Row content
            for next_col in column_mapping {
                let col = row.get_column(*next_col);
                // println!("Column {next_col}: {:?}", col);
                self.file
                    .write_all(col)
                    .expect("Failed to write column");
            }
        }
        self.file.flush().expect("Failed to flush file");
        // println!("\nDiskStorage::store - finished\n");
    }

    fn scan(&self) -> TableIterator {
        // TODO: Use mmap instead
        let mut reader = BufReader::new(File::open(&self.file_path).expect("Failed to open file"));
        let mut row_num: RowId = 0;
        let mut magic_buf = MagicType::default();
        reader.read_exact(&mut magic_buf).expect("Failed to read magic number");
        assert_eq!(&magic_buf, HEADER_MAGIC);
        let mut offsets_per_row_buf = usize::to_le_bytes(0);
        reader.read_exact(&mut offsets_per_row_buf).expect("Failed to read offsets per row");

        let num_offsets = usize::from_le_bytes(offsets_per_row_buf);
        // println!("Number of offsets per row: {num_offsets}");

        TableIterator::new(Box::new(std::iter::from_fn(move || {
            // println!("\nReading row {row_num}...");

            // Read row column offsets
            let mut offsets_buf = vec![0u8; num_offsets * size_of::<usize>()];
            if reader.read_exact(&mut offsets_buf).is_err_and(|err| err.kind() == std::io::ErrorKind::UnexpectedEof) {
                // println!("End of file - no more rows");
                return None;
            }
            let offsets: Vec<usize> = offsets_buf.chunks(size_of::<usize>())
                .map(|chunk| usize::from_le_bytes(chunk.try_into().unwrap()))
                .collect();
            // println!("Offsets: {:?}", offsets);

            // Read content length
            let mut len_buf = usize::to_le_bytes(0);
            reader.read_exact(&mut len_buf).expect("Failed to read content length");
            let content_len = usize::from_le_bytes(len_buf);

            // Read content
            let mut content = vec![0u8; content_len];
            reader.read_exact(&mut content).expect("Failed to read content");
            // println!("Content: {:?}", content);

            // Create scan item
            // FIXME: Dark Rust magic
            let content_box = content.into_boxed_slice();
            let offsets_box = offsets.into_boxed_slice();
            let row_content = RowContent {
                data: Box::leak(content_box),
                offsets: Box::leak(offsets_box),
            };
            // print!("Row content: {row_content:?}\n");
            let row_id = row_num.clone();
            row_num += 1;
            Some((row_id, row_content))
        })))
    }

    fn delete_rows(&mut self, _row_ids: Vec<RowId>) {
        // Implement file deletion later
        unimplemented!()
    }
}


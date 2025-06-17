use rudibi_server::engine::{Column, Database, Row, StorageCfg, Table};
use rudibi_server::serial::Serializable;
use rudibi_server::dtype::{ColumnValue::*, DataType};
use rudibi_server::query::{Bool::*, Value::*};
use rudibi_server::testlib;

use std::hint::black_box;
use std::fmt::{format, Debug};
use std::time::Duration;

#[derive(Debug)]
pub enum Backend { Memory, Disk }

pub struct BenchResult {
    fastest: Duration,
    slowest: Duration,
    median: Duration,
    mean: Duration,
}

const COLUMNS: usize = 5;
const HEADER_ROW: [&str; COLUMNS] = ["arg", "mean", "median", "fastest", "slowest"];
const MAX_DURATION_LENGTH: usize = 11;

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    let result = if secs > 99.999 {
        String::from(">99.999 s")
    } else if secs >= 1.0 {
        format!("{:.3} s", secs)
    } else if secs >= 0.001 {
        format!("{:.3} ms", secs * 1000.0)
    } else if secs >= 0.000001 {
        format!("{:.3} µs", secs * 1_000_000.0)
    } else {
        format!("{:.1} ns", secs * 1_000_000_000.0)
    };
    assert!(result.len() <= MAX_DURATION_LENGTH, "{result}-{}", result.len());
    result
}

struct TablePrinter {
    lengths: [usize; COLUMNS],
    args: Vec<String>,
    idx: usize,
}


impl TablePrinter {

    pub fn of<Arg: Debug> (args: &[Arg]) -> Self 
    {
        let formatted_args: Vec<String> = args.iter().map(|arg| format!("{:?}", arg)).collect();
        let max_arg_len = formatted_args.iter().map(|f| f.len()).max().unwrap();
        let max_value_lengths: [usize; COLUMNS] = [max_arg_len, MAX_DURATION_LENGTH, MAX_DURATION_LENGTH, MAX_DURATION_LENGTH, MAX_DURATION_LENGTH];
        let mut max_column_lengths: [usize; COLUMNS] = [0; COLUMNS];
        for i in 0..COLUMNS {
            max_column_lengths[i] = std::cmp::max(max_value_lengths[i], HEADER_ROW[i].len());
        }

        Self { 
            args: formatted_args,
            lengths: max_column_lengths,
            idx: 0
        }
    }

    pub fn print_header(&self) {
        self.print_row(&HEADER_ROW);
        let divider = std::iter::repeat_n(String::from("-"), self.lengths.iter().cloned().reduce(|a, b| a + b).unwrap() + 3*COLUMNS + 1).reduce(|a, b| a + &b).unwrap();
        println!("{divider}");
        
    }

    pub fn print_result(&mut self, m: BenchResult) {
        assert!(self.idx < self.args.len());
        let row = [self.args[self.idx].as_str(), &format_duration(m.mean), &format_duration(m.median), &format_duration(m.fastest), &format_duration(m.slowest)];
        self.print_row(&row);
        self.idx += 1;
    }

    fn print_row(&self, cells: &[&str; COLUMNS]) {
        println!(
            "| {:<w0$} | {:>w1$} | {:>w2$} | {:>w3$} | {:>w4$} |",
            cells[0], cells[1], cells[2], cells[3], cells[4],
            w0 = self.lengths[0],
            w1 = self.lengths[1],
            w2 = self.lengths[2],
            w3 = self.lengths[3],
            w4 = self.lengths[4],
        );
    }
}

pub fn run_bench<T: Copy + Debug, U, R> (
    bench_name: &str, samples: usize,
    args: &[T], backend: Backend, schema: Table,
    setup: fn(&mut Database, T) -> U,
    test: fn(&mut Database, U) -> R, 
) {
    assert!(samples > 0);
    assert!(args.len() > 0);
    println!("{bench_name} ({backend:?}, {samples} samples)");
    let mut printer = TablePrinter::of(args);
    printer.print_header();
    for arg in args.iter().cloned() {
        let mut measurements = Vec::with_capacity(samples);
        for _ in 0..samples {
            let mut db = Database::new();
            let storage = match backend {
                Backend::Memory => StorageCfg::InMemory,
                Backend::Disk => StorageCfg::Disk { path: testlib::random_temp_file() },
            };
            db.new_table(&schema, storage.clone()).unwrap();
            let test_arg = setup(&mut db, arg);
            let start = std::time::SystemTime::now();
            black_box(test(black_box(&mut db), black_box(test_arg)));
            let time = start.elapsed().unwrap();
            if let StorageCfg::Disk { path } = storage { std::fs::remove_file(path).unwrap() }
            measurements.push(time);
        }
        measurements.sort();
        let fastest = *measurements.first().unwrap();
        let slowest = *measurements.last().unwrap();
        let middle = measurements.len() / 2;
        let median = match measurements.len() % 2 == 0 {
            true => measurements[middle],
            false => (measurements[middle-1] + measurements[middle]) / 2
        };
        let mean = measurements.iter().cloned().reduce(|a, b| a + b).unwrap() / measurements.len() as u32;
        let result = BenchResult {
            fastest,
            slowest,
            median,
            mean,
        };
        printer.print_result(result);
    }
    println!();
}

pub mod scenarios {
    use super::*;

    pub fn batch_store_u32(backend: Backend) {
        run_bench(
            "batch_store_u32", 50,
            &[1, 10, 100, 1_000, 10_000, 100_000, 1_000_000],
            backend,
            Table::new("TestTable", vec![Column::new("id", DataType::U32)]),
            |_db, n| {
                return (0..n)
                    .map(|i| Row::of_columns(&[&i.serialized()]))
                    .collect::<Vec<Row>>();
            },
            |db, rows| { db.insert("TestTable", &["id"], &rows).unwrap() }
        )
    }

    pub fn select_half_filter_lt(backend: Backend) {
        run_bench(
            "select_half_filter_lt", 50, 
            &[1, 10, 100, 1_000, 10_000, 100_000, 1_000_000],
            backend,
            Table::new("TestTable", vec![Column::new("id", DataType::U32)]),
            |db, n| {
                let rows: Vec<Row> = (0..n)
                    .map(|i| Row::of_columns(&[i.serialized()]))
                    .collect();
                db.insert("TestTable", &["id"], &rows).unwrap();
                return n/2;
            },
            |db, max| { db.select(&[ColumnRef("id")], "TestTable", &Lt(ColumnRef("id"), Const(U32(max)))).unwrap() }
        );
    }

    pub fn select_all(backend: Backend) {
        run_bench(
            "select_all", 50,
            &[1, 10, 100, 1_000, 10_000, 100_000, 1_000_000],
            backend,
            Table::new("TestTable", vec![Column::new("id", DataType::U32)]),
            |db, n| {
                let rows: Vec<Row> = (0..n)
                    .map(|i| Row::of_columns(&[i.serialized()]))
                    .collect();
                db.insert("TestTable", &["id"], &rows).unwrap();
            },
            |db, _| { db.select(&[ColumnRef("id")], "TestTable", &True).unwrap() }
        );
    }

    pub fn delete_all(dataset_sizes: &[u32], backend: Backend) {
        run_bench(
            "delete_all", 50,
            dataset_sizes,
            backend,
            Table::new("TestTable", vec![Column::new("id", DataType::U32)]),
            |db, n| {
                let rows: Vec<Row> = (0..n)
                    .map(|n| Row::of_columns(&[u32::serialized(&n)]))
                    .collect();
                db.insert("TestTable", &["id"], &rows).unwrap();
                return ();
            },
            |db, _| { db.delete("TestTable", &True).unwrap() }

        );
    }

    pub fn delete_first_half(dataset_sizes: &[u32], backend: Backend) {
        run_bench(
            "delete_first_half", 50,
            dataset_sizes,
            backend,
            Table::new("TestTable", vec![Column::new("id", DataType::U32)]),
            |db, n| {
                let rows: Vec<Row> = (0..n)
                    .map(|n| Row::of_columns(&[u32::serialized(&n)]))
                    .collect();
                db.insert("TestTable", &["id"], &rows).unwrap();
                return n/2;
            },
            |db, n| { db.delete("TestTable", &Lt(ColumnRef("id"), Const(U32(n)))).unwrap() }
        );
    }
}
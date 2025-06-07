use rudibi_server::engine::{Database, StorageCfg, Table};
use rudibi_server::testlib;

use std::hint::black_box;
use std::fmt::Debug;
use std::time::Duration;

#[derive(Debug)]
pub enum Backend { Memory, Disk }

#[derive(Debug)]
pub struct BenchResult<T> {
    arg: T,
    fastest: Duration,
    slowest: Duration,
    median: Duration,
    mean: Duration,
    _samples: usize,
}

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 1.0 {
        format!("{:.1} s", secs)
    } else if secs >= 0.001 {
        format!("{:.3} ms", secs * 1000.0)
    } else if secs >= 0.000001 {
        format!("{:.3} µs", secs * 1_000_000.0)
    } else {
        format!("{:.1} ns", secs * 1_000_000_000.0)
    }
}
pub fn print_row(row: &Vec<String>, lengths: &Vec<usize>) {
    println!(
        "| {:<w0$} | {:>w1$} | {:>w2$} | {:>w3$} | {:>w4$} |",
        row[0], row[1], row[2], row[3], row[4],
        w0 = lengths[0],
        w1 = lengths[1],
        w2 = lengths[2],
        w3 = lengths[3],
        w4 = lengths[4],
    );
}

pub fn run_bench<T: Copy + Debug, U> (
    bench_name: &str, samples: usize,
    args: &[T], backend: Backend, schema: Table,
    setup: fn(&mut Database, T) -> U,
    test: fn(&mut Database, U) -> (), 
) {
    assert!(samples > 0);
    let mut results: Vec<BenchResult<T>> = Vec::with_capacity(samples);
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
        results.push(BenchResult {
            arg,
            fastest,
            slowest,
            median,
            mean,
            _samples: samples,
        });
    }
    println!("{bench_name} ({backend:?}, {samples} samples)");

    const COLUMNS: usize = 5;
    let mut table: Vec<Vec<String>> = Vec::with_capacity(results.len() + 1);
    let header_row = vec!["arg", "fastest", "slowest", "median", "mean"].iter().map(|s| s.to_string()).collect();
    table.push(header_row);
    for result in &results {
        let mut row = Vec::with_capacity(COLUMNS);
        row.push(format!("{:?}", result.arg));
        row.push(format_duration(result.fastest));
        row.push(format_duration(result.slowest));
        row.push(format_duration(result.median));
        row.push(format_duration(result.mean));
        assert_eq!(row.len(), COLUMNS);
        table.push(row);
    }

    let lengths: Vec<usize> = (0..COLUMNS).map(|col| {
        table.iter().map(|row| row[col].len()).max().unwrap()
    }).collect();
    assert_eq!(lengths.len(), COLUMNS);
    print_row(&table[0], &lengths);
    let divider = std::iter::repeat_n(String::from("-"), lengths.iter().cloned().reduce(|a, b| a + b).unwrap() + 3*COLUMNS + 1).reduce(|a, b| a + &b).unwrap();
    println!("{divider}");

    for row in table.iter().skip(1) {
        print_row(row, &lengths);
    }
}


use rudibi_server::{dtype::DataType, engine::*};
use rudibi_server::serial::Serializable;
use rudibi_server::query::Bool::*;
use rudibi_server::query::Value::*;
use rudibi_server::dtype::ColumnValue::*;
mod bench_scenarios;
mod benchlib;

use divan;

use crate::benchlib::{run_bench, Backend};

fn main() {
    // divan::main();
    delete_first_half(Backend::Memory);
}

fn in_mem_provider() -> StorageCfg { StorageCfg::InMemory }

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn batch_store_u32(bencher: divan::Bencher, n: u32) {
    bench_scenarios::batch_store_u32(bencher, n, in_mem_provider);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_half_filter_lt(bencher: divan::Bencher, n: u32) {
    bench_scenarios::select_half_filter_lt(bencher, n, in_mem_provider);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_all(bencher: divan::Bencher, n: u32) {
    bench_scenarios::select_all(bencher, n, in_mem_provider);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn delete_all(bencher: divan::Bencher, n: u32) {
    bench_scenarios::delete_all(bencher, n, in_mem_provider);
}

fn delete_first_half(backend: benchlib::Backend) {
    run_bench(
        "delete_first_half", 50,
        &[1, 10, 100, 1_000, 10_000, 50_000],
        backend,
        Table::new("TestTable", vec![Column::new("id", DataType::U32)]),
        |db, n| {
            let rows: Vec<Row> = (0..n)
                .map(|n| Row::of_columns(&[u32::serialized(&n)]))
                .collect();
            db.insert("TestTable", &["id"], &rows).unwrap();
            return n/2;
        },
        |db, n| {
            db.delete("TestTable", &Lt(ColumnRef("id"), Const(U32(n)))).unwrap();
        }
    );
}
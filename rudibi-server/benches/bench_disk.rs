
use rudibi_server::engine::*;
use rudibi_server::testlib;
mod bench_scenarios;

use divan;

fn main() {
    // Run registered benchmarks
    divan::main();
}

fn disk_provider() -> StorageCfg { StorageCfg::Disk { path: testlib::random_temp_file() } }

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn batch_store_u32(bencher: divan::Bencher, n: u32) {
    bench_scenarios::batch_store_u32(bencher, n, disk_provider);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_half_filter_lt(bencher: divan::Bencher, n: u32) {
    bench_scenarios::select_half_filter_lt(bencher, n, disk_provider);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_half_filter_lt_new(bencher: divan::Bencher, n: u32) {
    bench_scenarios::select_half_filter_lt_new(bencher, n, disk_provider);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_all(bencher: divan::Bencher, n: u32) {
    bench_scenarios::select_all(bencher, n, disk_provider);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_all_new(bencher: divan::Bencher, n: u32) {
    bench_scenarios::select_all_new(bencher, n, disk_provider);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000] 
)]
fn delete_all(bencher: divan::Bencher, n: u32) {
    bench_scenarios::delete_all(bencher, n, disk_provider);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000]
)]
fn delete_first_half(bencher: divan::Bencher, n: u32) {
    bench_scenarios::delete_first_half(bencher, n, disk_provider);
}
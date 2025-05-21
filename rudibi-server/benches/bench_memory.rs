
use rudibi_server::{engine::*, storage};
mod bench_scenarios;

use divan;

fn main() {
    // Run registered benchmarks
    divan::main();
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn batch_store_u32(bencher: divan::Bencher, n: u32) {
    bench_scenarios::batch_store_u32(bencher, n, StorageConfig::InMemory);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_half_filter_lt(bencher: divan::Bencher, n: u32) {
    bench_scenarios::select_half_filter_lt(bencher, n, StorageConfig::InMemory);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn select_all(bencher: divan::Bencher, n: u32) {
    bench_scenarios::select_all(bencher, n, StorageConfig::InMemory);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn delete_all(bencher: divan::Bencher, n: u32) {
    bench_scenarios::delete_all(bencher, n, StorageConfig::InMemory);
}

#[divan::bench(
    sample_count = 10,
    sample_size = 5,
    args = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
)]
fn delete_first_half(bencher: divan::Bencher, n: u32) {
    bench_scenarios::delete_all(bencher, n, StorageConfig::InMemory);
}
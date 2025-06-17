
mod benchlib;
use benchlib::{Backend, scenarios};

fn main() {
    scenarios::batch_store_u32(Backend::Disk);
    scenarios::select_all(Backend::Disk);
    scenarios::select_half_filter_lt(Backend::Disk);
    scenarios::delete_all(&[1, 10, 100, 1_000, 10_000, 100_000], Backend::Disk);
    scenarios::delete_first_half(&[1, 10, 100, 1_000, 10_000, 100_000], Backend::Disk);
}
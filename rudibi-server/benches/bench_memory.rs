
mod benchlib;
use benchlib::{Backend, scenarios};

fn main() {
    scenarios::batch_store_u32(Backend::Memory);
    scenarios::select_all(Backend::Memory);
    scenarios::select_half_filter_lt(Backend::Memory);
    scenarios::delete_all(&[1, 10, 100, 1_000, 10_000, 100_000, 1_000_000], Backend::Memory);
    scenarios::delete_first_half(&[1, 10, 100, 1_000, 5_000, 10_000, 20_000], Backend::Memory);
}
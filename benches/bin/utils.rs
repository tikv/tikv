extern crate rand;

use std::sync::atomic::{ATOMIC_U64_INIT, AtomicU64, Ordering};
use std::time::Duration;
use tikv::coprocessor::codec::table::{encode_index_seek_key, encode_row_key};
use tikv::util::codec::number::NumberEncoder;

use rand::Rng;

#[inline]
pub fn next_ts() -> u64 {
    static CURRENT: AtomicU64 = ATOMIC_U64_INIT;
    CURRENT.fetch_add(1, Ordering::SeqCst)
}

/// Generate `count` row keys that all with the specified `table_id`.
///
/// Row id will start with `start_id` and increment
pub fn generate_row_keys(table_id: i64, start_id: i64, count: usize) -> Vec<Vec<u8>> {
    let mut result = Vec::with_capacity(count);
    for i in (start_id)..(start_id + count as i64) {
        let mut handle = Vec::with_capacity(8);
        handle.encode_i64(i as i64).unwrap();
        let key = encode_row_key(table_id, &handle);
        result.push(key);
    }
    result
}


/// Generate `count` unique index keys that all with the specified `table_id`, `index_id`,
/// and `value_len`.
pub fn generate_unique_index_keys(
    table_id: i64,
    index_id: i64,
    value_len: usize,
    count: usize,
) -> Vec<Vec<u8>> {
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        result.push(encode_index_seek_key(
            table_id,
            index_id,
            &vec![0u8; value_len],
        ));
    }
    result
}

pub fn shuffle<T>(data: &mut [T]) {
    let mut rng = rand::thread_rng();
    for i in 0..(data.len()) {
        let j = rng.gen_range(i, data.len());
        data.swap(i, j);
    }
}

/// Run `job` for `iterations` times, and return a Vec containing nanoseconds of each turn `job`
/// returns.
///
/// `job` must return a `Duration` type. Please calculate time cost of what you want to bench
/// manually ant return it.
pub fn record_time<F>(mut job: F, iterations: u32) -> Vec<u64>
where
    F: FnMut() -> Duration,
{
    (0..iterations)
        .map(|_| {
            let time = job();
            time.as_secs() * 1_000_000_000 + (time.subsec_nanos() as u64)
        })
        .collect()
}

pub fn average(data: &[u64]) -> u64 {
    data.iter().sum::<u64>() / (data.len() as u64)
}

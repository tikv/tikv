// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{ATOMIC_U64_INIT, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use test::black_box;
use tikv::coprocessor::codec::table::{encode_index_seek_key, encode_row_key};
use tikv::util::codec::number::NumberEncoder;

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
    if value_len < 8 {
        panic!("Value len should be at least 8.");
    }
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let mut value = Vec::with_capacity(value_len);
        value.resize(value_len - 8, 0);
        value.encode_u64(i as u64).unwrap();
        result.push(encode_index_seek_key(table_id, index_id, &value));
    }
    result
}

/// Convert duration to nanoseconds
pub fn to_total_nanos(duration: &Duration) -> u64 {
    duration.as_secs() * 1_000_000_000 + u64::from(duration.subsec_nanos())
}

/// Run `job` for `iterations` times, and return the average time cost as nanoseconds.
/// Attention that if `job` returns no value, it will be optimized out by the compiler.
pub fn do_bench<F, T>(mut job: F, iterations: u32) -> f64
where
    F: FnMut() -> T,
{
    let t = Instant::now();
    for _ in 0..iterations {
        // Avoid being optimized out by compiler
        black_box(job());
    }
    to_total_nanos(&t.elapsed()) as f64 / f64::from(iterations)
}

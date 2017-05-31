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

use rand::{Rng, thread_rng};
use test::{bench, fmt_bench_samples};
use super::assert_storage::AssertionStorage;

fn gen_rand_str(len: usize) -> Vec<u8> {
    let mut rand_str = vec![0; len];
    thread_rng().fill_bytes(&mut rand_str);
    rand_str
}

#[test]
fn bench_txn_put_exclusive_1() {
    let (sts, cts) = (0, 1);
    let bs = bench::benchmark(|b| {
        b.iter(|| {
            let _ = gen_rand_str(30);
            let _ = gen_rand_str(256);
            let (_, _) = (sts + 1, cts + 1);
        });
    });
    println!("{}", fmt_bench_samples(&bs));
}

#[test]
fn bench_txn_put() {
    let (_cluster, storage) = AssertionStorage::new_raft_storage_with_store_count(1, "");
    let (sts, cts) = (0, 1);
    let bs = bench::benchmark(|b| {
        b.iter(|| {
            let key = gen_rand_str(30);
            let value = gen_rand_str(256);
            let (sts, cts) = (sts + 1, cts + 1);
            storage.put_ok(&key, &value, sts, cts);
        });
    });
    println!("{}", fmt_bench_samples(&bs));
}

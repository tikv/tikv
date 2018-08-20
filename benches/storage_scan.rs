// Copyright 2018 PingCAP, Inc.
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

#![feature(repeat_generic_slice)]

/// This suite contains benchmarks for several different configurations generated
/// dynamically. Thus it has `harness = false`.
extern crate criterion;

extern crate clap;

extern crate bench_util;
extern crate test_storage;
extern crate test_util;
extern crate tikv;

use clap::{App, Arg};
use criterion::{black_box, Bencher, Criterion};

use bench_util::*;
use test_storage::SyncStorage;
use test_util::*;
use tikv::storage::engine::RocksEngine;
use tikv::storage::{Key, Mutation};

/// Benchmark forward scan performance of many DELETE versions.
fn bench_forward_scan_mvcc_deleted(b: &mut Bencher) {
    let store = SyncStorage::default();
    let mut ts_generator = 1..;

    let mut kvs = KvGenerator::new(32, 1000);

    for (k, v) in kvs.take(10000) {
        let mut ts = ts_generator.next().unwrap();
        store
            .prewrite(
                new_no_cache_context(),
                vec![Mutation::Put((Key::from_raw(&k), v))],
                k.clone(),
                ts,
            )
            .unwrap();
        store
            .commit(
                new_no_cache_context(),
                vec![Key::from_raw(&k)],
                ts,
                ts_generator.next().unwrap(),
            )
            .unwrap();

        ts = ts_generator.next().unwrap();
        store
            .prewrite(
                new_no_cache_context(),
                vec![Mutation::Delete(Key::from_raw(&k))],
                k.clone(),
                ts,
            )
            .unwrap();
        store
            .commit(
                new_no_cache_context(),
                vec![Key::from_raw(&k)],
                ts,
                ts_generator.next().unwrap(),
            )
            .unwrap();
    }

    // We should got nothing when scanning any keys.

    kvs = KvGenerator::new(100, 1000);
    let ts = ts_generator.next().unwrap();
    b.iter(|| {
        let (k, _) = kvs.next().unwrap();
        assert!(
            store
                .scan(new_no_cache_context(), Key::from_raw(&k), 1, false, ts)
                .unwrap()
                .is_empty()
        )
    })
}

#[derive(Clone)]
struct ScanConfig {
    number_of_keys: usize,
    number_of_versions: usize,
    key_len: usize,
    value_len: usize,
}

impl ::std::fmt::Debug for ScanConfig {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(
            f,
            "keys = {}, versions = {}, key_len = {}, value_len = {}",
            self.number_of_keys, self.number_of_versions, self.key_len, self.value_len
        )
    }
}

fn prepare_scan_data(store: &SyncStorage<RocksEngine>, config: &ScanConfig) -> u64 {
    let kvs: Vec<_> = KvGenerator::new_by_seed(0xFEE1DEAD, config.key_len, config.value_len)
        .take(config.number_of_keys)
        .collect();
    let mut ts_generator = 1..;
    for _ in 0..config.number_of_versions {
        let ts = ts_generator.next().unwrap();
        let mutations: Vec<_> = kvs
            .iter()
            .map(|(k, v)| Mutation::Put((Key::from_raw(k), v.clone())))
            .collect();
        store
            .prewrite(new_no_cache_context(), mutations, kvs[0].0.clone(), ts)
            .unwrap();
        let keys: Vec<_> = kvs.iter().map(|(k, _)| Key::from_raw(k)).collect();
        store
            .commit(
                new_no_cache_context(),
                keys,
                ts,
                ts_generator.next().unwrap(),
            )
            .unwrap();
    }
    ts_generator.next().unwrap()
}

fn bench_forward_scan(b: &mut Bencher, input: &ScanConfig) {
    let store = SyncStorage::default();
    let max_ts = prepare_scan_data(&store, input);
    b.iter(|| {
        let kvs = store
            .scan(
                new_no_cache_context(),
                black_box(Key::from_raw(&[])),
                input.number_of_keys + 1,
                false,
                max_ts,
            )
            .unwrap();
        assert_eq!(kvs.len(), input.number_of_keys);
    });
}

fn bench_backward_scan(b: &mut Bencher, input: &ScanConfig) {
    let store = SyncStorage::default();
    let max_ts = prepare_scan_data(&store, input);
    let start_key = Key::from_raw(&[0xFF].repeat(input.key_len + 1));
    b.iter(|| {
        let kvs = store
            .reverse_scan(
                new_no_cache_context(),
                black_box(start_key.clone()),
                input.number_of_keys + 1,
                false,
                max_ts,
            )
            .unwrap();
        assert_eq!(kvs.len(), input.number_of_keys);
    });
}

fn main() {
    let matches = App::new("storage_scan_benchmark")
        .arg(
            Arg::with_name("full")
                .long("full")
                .help("Run full benchmarks"),
        )
        .get_matches();

    let mut criterion = Criterion::default().sample_size(10);

    let mut inputs = vec![];

    let number_of_keys_coll;
    let number_of_versions_coll;
    let key_len_coll;
    let value_len_coll;

    if matches.is_present("full") {
        number_of_keys_coll = vec![100, 10000, 1000000];
        number_of_versions_coll = vec![1, 2, 5, 10, 20, 50];
        key_len_coll = vec![32, 100, 200];
        value_len_coll = vec![5, 100, 200];
    } else {
        number_of_keys_coll = vec![5000];
        number_of_versions_coll = vec![1, 2, 20];
        key_len_coll = vec![32];
        value_len_coll = vec![5, 100];
    }

    for number_of_keys in &number_of_keys_coll {
        for number_of_versions in &number_of_versions_coll {
            for key_len in &key_len_coll {
                for value_len in &value_len_coll {
                    inputs.push(ScanConfig {
                        number_of_keys: *number_of_keys,
                        number_of_versions: *number_of_versions,
                        key_len: *key_len,
                        value_len: *value_len,
                    });
                }
            }
        }
    }
    criterion.bench_function_over_inputs("forward_scan", bench_forward_scan, inputs.clone());
    criterion.bench_function_over_inputs("backward_scan", bench_backward_scan, inputs);
    criterion.bench_function("forward_scan_mvcc_deleted", bench_forward_scan_mvcc_deleted);

    criterion.final_summary();
}

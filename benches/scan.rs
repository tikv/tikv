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

extern crate criterion;

//extern crate bench_util;
extern crate test_storage;
extern crate test_util;
extern crate tikv;

use criterion::{black_box, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use callgrind::CallgrindClientRequest;

//use bench_util::*;
use test_storage::*;
use test_util::*;
use tikv::storage::{Key, Mutation, RocksEngine, Engine, CursorBuilder, CFStatistics};

//
///// Benchmark forward scan performance of many DELETE versions.
//fn bench_forward_scan_mvcc_deleted(b: &mut Bencher) {
//    let store = SyncTestStorageBuilder::new().build().unwrap();
//    let mut ts_generator = 1..;
//
//    let mut kvs = KvGenerator::new(32, 1000);
//
//    for (k, v) in kvs.take(10000) {
//        let mut ts = ts_generator.next().unwrap();
//        store
//            .prewrite(
//                new_no_cache_context(),
//                vec![Mutation::Put((Key::from_raw(&k), v))],
//                k.clone(),
//                ts,
//            )
//            .unwrap();
//        store
//            .commit(
//                new_no_cache_context(),
//                vec![Key::from_raw(&k)],
//                ts,
//                ts_generator.next().unwrap(),
//            )
//            .unwrap();
//
//        ts = ts_generator.next().unwrap();
//        store
//            .prewrite(
//                new_no_cache_context(),
//                vec![Mutation::Delete(Key::from_raw(&k))],
//                k.clone(),
//                ts,
//            )
//            .unwrap();
//        store
//            .commit(
//                new_no_cache_context(),
//                vec![Key::from_raw(&k)],
//                ts,
//                ts_generator.next().unwrap(),
//            )
//            .unwrap();
//    }
//
//    // We should got nothing when scanning any keys.
//
//    kvs = KvGenerator::new(100, 1000);
//    let ts = ts_generator.next().unwrap();
//    b.iter(|| {
//        let (k, _) = kvs.next().unwrap();
//        assert!(
//            store
//                .scan(
//                    new_no_cache_context(),
//                    Key::from_raw(&k),
//                    None,
//                    1,
//                    false,
//                    ts
//                )
//                .unwrap()
//                .is_empty()
//        )
//    })
//}

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

fn prepare_scan_data(store: &SyncTestStorage<RocksEngine>, config: &ScanConfig) -> u64 {
    let kvs: Vec<_> = KvGenerator::with_seed(config.key_len, config.value_len, 0xFEE1DEAD)
        .generate(config.number_of_keys);
    let mut ts_generator = 1..;
    for _ in 0..config.number_of_versions {
        let ts = ts_generator.next().unwrap();
        let mutations: Vec<_> = kvs
            .iter()
            .map(|(k, v)| Mutation::Put((Key::from_raw(k), v.clone())))
            .collect();
        store
            .prewrite(Context::default(), mutations, kvs[0].0.clone(), ts)
            .unwrap();
        let keys: Vec<_> = kvs.iter().map(|(k, _)| Key::from_raw(k)).collect();
        store
            .commit(
                Context::default(),
                keys,
                ts,
                ts_generator.next().unwrap(),
            )
            .unwrap();
    }
    ts_generator.next().unwrap()
}

fn bench_forward_scan(b: &mut Bencher, input: &ScanConfig) {
    let store = SyncTestStorageBuilder::new().build().unwrap();
    let max_ts = prepare_scan_data(&store, input);
    let engine = store.get_engine();
    let db = engine.get_rocksdb();

    db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);

    b.iter(|| {
        CallgrindClientRequest::StartInstrumentation.now();
        let kvs = store
            .scan(
                Context::default(),
                black_box(Key::from_raw(&[])),
                None,
                input.number_of_keys + 1,
                false,
                max_ts,
            )
            .unwrap();
        assert_eq!(kvs.len(), input.number_of_keys);
        CallgrindClientRequest::StopInstrumentation.now();
    });
}

fn bench_naive_forward_scan_0(b: &mut Bencher, input: &ScanConfig) {
    use engine::rocks::{SeekKey, ReadOptions};

    let store = SyncTestStorageBuilder::new().build().unwrap();
    prepare_scan_data(&store, input);
    let engine = store.get_engine();
    let db = engine.get_rocksdb();

    db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);

    b.iter(|| {
        CallgrindClientRequest::StartInstrumentation.now();
        let cf_write = db.cf_handle("write").unwrap();
        let snapshot = db.snapshot();
        let mut opt = ReadOptions::new();
        opt.fill_cache(false);
        opt.set_total_order_seek(true);
        let mut iter = snapshot.iter_cf(cf_write, opt);
        assert!(iter.seek(SeekKey::Start));
        let mut n = 0;
        loop {
            if !iter.next() {
                break;
            }
            black_box(iter.key());
            n += 1;
        }
        assert_eq!(n, input.number_of_keys - 1);
        CallgrindClientRequest::StopInstrumentation.now();
    })
}

fn bench_naive_forward_scan_1(b: &mut Bencher, input: &ScanConfig) {
    use engine::rocks::{SeekKey, ReadOptions};

    let store = SyncTestStorageBuilder::new().build().unwrap();
    prepare_scan_data(&store, input);
    let engine = store.get_engine();
    let db = engine.get_rocksdb();

    db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);

    b.iter(|| {
        CallgrindClientRequest::StartInstrumentation.now();
        let cf_write = db.cf_handle("write").unwrap();
        let snapshot = db.snapshot();
        let mut opt = ReadOptions::new();
        opt.fill_cache(false);
        opt.set_total_order_seek(true);
        let mut iter = snapshot.iter_cf(cf_write, opt);
        assert!(iter.seek(SeekKey::Start));
        let mut n = 0;
        loop {
            if !iter.next() {
                break;
            }
            black_box(iter.key());
            black_box(iter.value());
            n += 1;
        }
        assert_eq!(n, input.number_of_keys - 1);
        CallgrindClientRequest::StopInstrumentation.now();
    })
}

fn bench_naive_forward_scan_2(b: &mut Bencher, input: &ScanConfig) {
    use engine::rocks::{SeekKey, ReadOptions};

    let store = SyncTestStorageBuilder::new().build().unwrap();
    prepare_scan_data(&store, input);
    let engine = store.get_engine();
    let db = engine.get_rocksdb();

    db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);

    b.iter(|| {
        CallgrindClientRequest::StartInstrumentation.now();
        let cf_write = db.cf_handle("write").unwrap();
        let snapshot = db.snapshot();
        let mut opt = ReadOptions::new();
        opt.fill_cache(false);
        opt.set_total_order_seek(true);
        let mut out = Vec::new();
        let mut iter = snapshot.iter_cf(cf_write, opt);
        assert!(iter.seek(SeekKey::Start));
        let mut n = 0;
        loop {
            if !iter.next() {
                break;
            }
            out.push((iter.key().to_vec(), iter.value().to_vec()));
            n += 1;
        }
        black_box(&mut out);
        assert_eq!(n, input.number_of_keys - 1);
        CallgrindClientRequest::StopInstrumentation.now();
    })
}

fn bench_naive_forward_scan_3(b: &mut Bencher, input: &ScanConfig) {
    let store = SyncTestStorageBuilder::new().build().unwrap();
    prepare_scan_data(&store, input);
    let engine = store.get_engine();
    let db = engine.get_rocksdb();

    db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);

    b.iter(|| {
        CallgrindClientRequest::StartInstrumentation.now();
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = CursorBuilder::new(&snapshot, "write")
            .fill_cache(true)
            .prefix_seek(false)
            .build()
            .unwrap();

        let mut out = vec![];
        let mut stats = CFStatistics::default();
        cursor.seek_to_first(&mut stats);
        let mut n = 0;
        loop {
            if !cursor.next(&mut stats) {
                break;
            }
            out.push((cursor.key(&mut stats).to_vec(), cursor.value(&mut stats).to_vec()));
            n += 1;
        }
        black_box(&mut out);
        assert_eq!(n, input.number_of_keys - 1);
        CallgrindClientRequest::StopInstrumentation.now();
    })
}

fn bench_backward_scan(b: &mut Bencher, input: &ScanConfig) {
    let store = SyncTestStorageBuilder::new().build().unwrap();
    let max_ts = prepare_scan_data(&store, input);
    let start_key = Key::from_raw(&[0xFF].repeat(input.key_len + 1));
    b.iter(|| {
        CallgrindClientRequest::StartInstrumentation.now();
        let kvs = store
            .reverse_scan(
                Context::default(),
                black_box(start_key.clone()),
                None,
                input.number_of_keys + 1,
                false,
                max_ts,
            )
            .unwrap();
        assert_eq!(kvs.len(), input.number_of_keys);
        CallgrindClientRequest::StopInstrumentation.now();
    });
}

#[cfg_attr(feature = "cargo-clippy", allow(useless_let_if_seq))]
fn main() {
    let mut criterion = Criterion::default().sample_size(10).configure_from_args();

    let mut inputs = vec![];

    let number_of_keys_coll;
    let number_of_versions_coll;
    let key_len_coll;
    let value_len_coll;

    number_of_keys_coll = vec![100000];
    number_of_versions_coll = vec![1];
    key_len_coll = vec![19];
    value_len_coll = vec![50];

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
    criterion.bench_function_over_inputs("naive_forward_scan_0", bench_naive_forward_scan_0, inputs.clone());
    criterion.bench_function_over_inputs("naive_forward_scan_1", bench_naive_forward_scan_1, inputs.clone());
    criterion.bench_function_over_inputs("naive_forward_scan_2", bench_naive_forward_scan_2, inputs.clone());
    criterion.bench_function_over_inputs("naive_forward_scan_3", bench_naive_forward_scan_3, inputs.clone());
//    criterion.bench_function_over_inputs("backward_scan", bench_backward_scan, inputs);

//    if bench_util::use_full_payload() {
//        // Only run this bench in bench mode because its preparation costs time.
//        criterion.bench_function("forward_scan_mvcc_deleted", bench_forward_scan_mvcc_deleted);
//    }

    criterion.final_summary();
}

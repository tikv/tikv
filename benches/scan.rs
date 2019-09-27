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

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

extern crate criterion;

extern crate test_storage;
extern crate test_util;
extern crate tikv;

use callgrind::CallgrindClientRequest;
use criterion::{black_box, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use std::sync::Arc;

use engine::{CF_LOCK, CF_WRITE, DB};
use test_storage::*;
use test_util::*;
use tikv::storage::mvcc::{RangeForwardScanner, ScannerConfig, ScannerBuilder};
use tikv::storage::{CFStatistics, CursorBuilder, Engine, Key, Mutation, RocksEngine};

//region Config

#[derive(Clone)]
struct ScanConfig {
    number_of_keys: usize,
    number_of_versions: usize,
    key_len: usize,
    value_len: usize,
    compacted: bool,
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
            .commit(Context::default(), keys, ts, ts_generator.next().unwrap())
            .unwrap();
    }
    ts_generator.next().unwrap()
}

impl ScanConfig {
    pub fn into_input(self) -> Input {
        let store = SyncTestStorageBuilder::new().build().unwrap();
        let max_ts = prepare_scan_data(&store, &self);
        let engine = store.get_engine();
        let db = engine.get_rocksdb();
        if self.compacted {
            db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
            db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
            db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);
        }
        Input {
            config: self,
            store: Arc::new(store),
            engine,
            db,
            max_ts,
        }
    }
}

#[derive(Clone)]
struct Input {
    config: ScanConfig,
    store: Arc<SyncTestStorage<RocksEngine>>,
    engine: RocksEngine,
    db: Arc<DB>,
    max_ts: u64,
}

impl ::std::fmt::Debug for Input {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(
            f,
            "keys={}/versions={}/key_len={}/value_len={}/compact={:?}",
            self.config.number_of_keys,
            self.config.number_of_versions,
            self.config.key_len,
            self.config.value_len,
            self.config.compacted
        )
    }
}

//endregion

fn bench_mvcc_forward_scan(b: &mut Bencher, input: &Input) {
    use crate::tikv::storage::Scanner;

    b.iter(|| {
        CallgrindClientRequest::start();
        let snapshot = input.engine.snapshot(&Context::default()).unwrap();
        let mut scanner = ScannerBuilder::new(snapshot, input.max_ts, false)
            .build().unwrap();
        let mut n = 0;
        loop {
            let kv = scanner.next().unwrap();
            if kv.is_none() {
                break;
            }
            black_box(kv);
            n += 1;
        }
        assert_eq!(n, input.config.number_of_keys);
        CallgrindClientRequest::stop(None);
    });
}

fn bench_range_forward_scanner_1(b: &mut Bencher, input: &Input) {
    let mut keys_buf = BufferVec::with_capacity(1, 1 * 100);
    let mut values_buf = BufferVec::with_capacity(1, 1 * 200);

    b.iter(|| {
        CallgrindClientRequest::start();
        let snapshot = input.engine.snapshot(&Context::default()).unwrap();
        let mut cfg = ScannerConfig::new(snapshot, input.max_ts, false);
        let lock_cursor = cfg.create_cf_cursor(CF_LOCK).unwrap();
        let write_cursor = cfg.create_cf_cursor(CF_WRITE).unwrap();
        let mut scanner = RangeForwardScanner::new(cfg, lock_cursor, write_cursor).unwrap();
        scanner.scan_first_lock().unwrap();
        let mut all_n = 0;

        loop {
            keys_buf.clear();
            values_buf.clear();
            let n = scanner.next(1, &mut keys_buf, &mut values_buf).unwrap();
            black_box(&keys_buf);
            black_box(&values_buf);
            all_n += n;
            if n < 1 {
                break;
            }
        }
        assert_eq!(all_n, input.config.number_of_keys);
        CallgrindClientRequest::stop(None);
    })
}

fn bench_range_forward_scanner_1024(b: &mut Bencher, input: &Input) {
    let mut keys_buf = BufferVec::with_capacity(1024, 1024 * 100);
    let mut values_buf = BufferVec::with_capacity(1024, 1024 * 200);

    b.iter(|| {
        CallgrindClientRequest::start();

        keys_buf.clear();
        values_buf.clear();

        let snapshot = input.engine.snapshot(&Context::default()).unwrap();
        let mut cfg = ScannerConfig::new(snapshot, input.max_ts, false);
        let lock_cursor = cfg.create_cf_cursor(CF_LOCK).unwrap();
        let write_cursor = cfg.create_cf_cursor(CF_WRITE).unwrap();
        let mut scanner = RangeForwardScanner::new(cfg, lock_cursor, write_cursor).unwrap();
        scanner.scan_first_lock().unwrap();
        let mut all_n = 0;
        loop {
            let n = scanner.next(1024, &mut keys_buf, &mut values_buf).unwrap();
            black_box(&keys_buf);
            black_box(&values_buf);
            all_n += n;
            if n < 1024 {
                break;
            }
        }
        assert_eq!(all_n, input.config.number_of_keys);
        CallgrindClientRequest::stop(None);
    })
}

fn bench_iter_forward(b: &mut Bencher, input: &Input) {
    use engine::rocks::{ReadOptions, SeekKey};

    b.iter(|| {
        CallgrindClientRequest::start();
        let cf_write = input.db.cf_handle("write").unwrap();
        let cf_lock = input.db.cf_handle("lock").unwrap();

        let snapshot = input.db.snapshot();
        let mut opt = ReadOptions::new();
        opt.fill_cache(true);
        opt.set_total_order_seek(true);
        let mut iter_lock = snapshot.iter_cf(cf_lock, opt);
        assert!(!iter_lock.seek(SeekKey::Start));

        let mut opt = ReadOptions::new();
        opt.fill_cache(true);
        opt.set_total_order_seek(true);
        let mut iter = snapshot.iter_cf(cf_write, opt);
        assert!(iter.seek(SeekKey::Key(b"")));
        let mut n = 0;
        loop {
            if !iter.next() {
                break;
            }
            black_box(iter.key());
            n += 1;
        }
        assert_eq!(
            n,
            input.config.number_of_keys * input.config.number_of_versions - 1
        );
        CallgrindClientRequest::stop(None);
    })
}

fn bench_iter_forward_3_cf(b: &mut Bencher, input: &Input) {
    use engine::rocks::{ReadOptions, SeekKey};

    b.iter(|| {
        CallgrindClientRequest::start();
        let cf_write = input.db.cf_handle("write").unwrap();
        let cf_lock = input.db.cf_handle("lock").unwrap();
        let cf_default = input.db.cf_handle("default").unwrap();

        let snapshot = input.db.snapshot();
        let mut opt = ReadOptions::new();
        opt.fill_cache(true);
        opt.set_total_order_seek(true);
        let mut iter_lock = snapshot.iter_cf(cf_lock, opt);
        assert!(!iter_lock.seek(SeekKey::Start));

        let mut opt = ReadOptions::new();
        opt.fill_cache(true);
        opt.set_total_order_seek(true);
        let mut iter_lock = snapshot.iter_cf(cf_default, opt);
        iter_lock.seek(SeekKey::Start);

        let mut opt = ReadOptions::new();
        opt.fill_cache(true);
        opt.set_total_order_seek(true);
        let mut iter = snapshot.iter_cf(cf_write, opt);
        assert!(iter.seek(SeekKey::Key(b"")));
        let mut n = 0;
        loop {
            if !iter.next() {
                break;
            }
            black_box(iter.key());
            n += 1;
        }
        assert_eq!(
            n,
            input.config.number_of_keys * input.config.number_of_versions - 1
        );
        CallgrindClientRequest::stop(None);
    })
}

fn bench_iter_forward_with_value(b: &mut Bencher, input: &Input) {
    use engine::rocks::{ReadOptions, SeekKey};

    b.iter(|| {
        CallgrindClientRequest::start();
        let cf_write = input.db.cf_handle("write").unwrap();
        let cf_lock = input.db.cf_handle("lock").unwrap();

        let snapshot = input.db.snapshot();
        let mut opt = ReadOptions::new();
        opt.fill_cache(true);
        opt.set_total_order_seek(true);
        let mut iter_lock = snapshot.iter_cf(cf_lock, opt);
        assert!(!iter_lock.seek(SeekKey::Start));

        let mut opt = ReadOptions::new();
        opt.fill_cache(true);
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
        assert_eq!(
            n,
            input.config.number_of_keys * input.config.number_of_versions - 1
        );
        CallgrindClientRequest::stop(None);
    })
}

fn bench_iter_forward_alloc_vec_for_kv(b: &mut Bencher, input: &Input) {
    use engine::rocks::{ReadOptions, SeekKey};

    b.iter(|| {
        CallgrindClientRequest::start();
        let cf_write = input.db.cf_handle("write").unwrap();
        let cf_lock = input.db.cf_handle("lock").unwrap();
        let snapshot = input.db.snapshot();
        let mut opt = ReadOptions::new();
        opt.fill_cache(true);
        opt.set_total_order_seek(true);
        opt.set_iterate_lower_bound(vec![]);
        let mut out = Vec::new();
        let mut iter_lock = snapshot.iter_cf(cf_lock, opt);
        assert!(!iter_lock.seek(SeekKey::Start));

        let mut opt = ReadOptions::new();
        opt.fill_cache(true);
        opt.set_total_order_seek(true);
        opt.set_iterate_lower_bound(vec![]);
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
        assert_eq!(
            n,
            input.config.number_of_keys * input.config.number_of_versions - 1
        );
        CallgrindClientRequest::stop(None);
    })
}

fn bench_cursor_forward(b: &mut Bencher, input: &Input) {
    b.iter(|| {
        CallgrindClientRequest::start();
        let snapshot = input.engine.snapshot(&Context::default()).unwrap();
        let mut stats = CFStatistics::default();
        let mut cursor_lock = CursorBuilder::new(&snapshot, "lock")
            .fill_cache(true)
            .prefix_seek(false)
            .build()
            .unwrap();
        assert!(!cursor_lock.seek_to_first(&mut stats));
        let mut cursor = CursorBuilder::new(&snapshot, "write")
            .fill_cache(true)
            .prefix_seek(false)
            .build()
            .unwrap();

        cursor.seek_to_first(&mut stats);
        let mut n = 0;
        loop {
            if !cursor.next(&mut stats) {
                break;
            }
            black_box(cursor.key(&mut stats));
            black_box(cursor.value(&mut stats));
            n += 1;
        }
        assert_eq!(
            n,
            input.config.number_of_keys * input.config.number_of_versions - 1
        );
        CallgrindClientRequest::stop(None);
    })
}

fn bench_cursor_forward_alloc_vec_for_kv(b: &mut Bencher, input: &Input) {
    b.iter(|| {
        CallgrindClientRequest::start();
        let snapshot = input.engine.snapshot(&Context::default()).unwrap();
        let mut stats = CFStatistics::default();
        let mut cursor_lock = CursorBuilder::new(&snapshot, "lock")
            .fill_cache(true)
            .prefix_seek(false)
            .build()
            .unwrap();
        assert!(!cursor_lock.seek_to_first(&mut stats));
        let mut cursor = CursorBuilder::new(&snapshot, "write")
            .fill_cache(true)
            .prefix_seek(false)
            .build()
            .unwrap();

        let mut out = vec![];
        cursor.seek_to_first(&mut stats);
        let mut n = 0;
        loop {
            if !cursor.next(&mut stats) {
                break;
            }
            out.push((
                cursor.key(&mut stats).to_vec(),
                cursor.value(&mut stats).to_vec(),
            ));
            n += 1;
        }
        black_box(&mut out);
        assert_eq!(
            n,
            input.config.number_of_keys * input.config.number_of_versions - 1
        );
        CallgrindClientRequest::stop(None);
    })
}

fn is_key_eq(key_a: &[u8], key_b: &[u8]) -> bool {
    let key_len = key_a.len();
    if key_len != key_b.len() {
        return false;
    }
    if key_len >= 24 {
        // fast path
        unsafe {
            let left = std::ptr::read_unaligned(key_a.as_ptr().add(key_len - 8) as *const u64);
            let right = std::ptr::read_unaligned(key_b.as_ptr().add(key_len - 8) as *const u64);
            if left != right {
                return false;
            }
            let left = std::ptr::read_unaligned(key_a.as_ptr().add(key_len - 16) as *const u64);
            let right = std::ptr::read_unaligned(key_b.as_ptr().add(key_len - 16) as *const u64);
            if left != right {
                return false;
            }
            let left = std::ptr::read_unaligned(key_a.as_ptr().add(key_len - 24) as *const u64);
            let right = std::ptr::read_unaligned(key_b.as_ptr().add(key_len - 24) as *const u64);
            if left != right {
                return false;
            }
            key_a[..key_len - 24] == key_b[..key_len - 24]
        }
    } else {
        key_a == key_b
    }
}

use tikv_util::buffer_vec::BufferVec;
use tikv_util::codec::bytes::encode_bytes;

fn bench_eq_key(b: &mut Bencher) {
    let mut key_a = encode_bytes(b"tXXXXXXXX_rXXYYXXYY");
    key_a.extend(b"11111111");
    let key_b = encode_bytes(b"tXXXXXXXX_rXXYYXXYY");
    b.iter(|| {
        assert!(Key::is_user_key_eq(
            black_box(&key_a),
            black_box(key_b.as_slice())
        ));
    });
}

fn bench_eq_naive(b: &mut Bencher) {
    let mut key_a = encode_bytes(b"tXXXXXXXX_rXXYYXXYY");
    key_a.extend(b"11111111");
    let key_b = encode_bytes(b"tXXXXXXXX_rXXYYXXYY");
    b.iter(|| {
        let key = black_box(&key_a);
        assert!((&key[..key.len() - 8] == black_box(key_b.as_slice())));
    });
}

fn bench_eq_new(b: &mut Bencher) {
    let mut key_a = encode_bytes(b"tXXXXXXXX_rXXYYXXYY");
    key_a.extend(b"11111111");
    let key_b = encode_bytes(b"tXXXXXXXX_rXXYYXXYY");
    b.iter(|| {
        let key = black_box(&key_a);
        assert!(is_key_eq(
            &key[..key.len() - 8],
            black_box(key_b.as_slice())
        ));
    });
}

fn bench_ne_key(b: &mut Bencher) {
    let mut key_a = encode_bytes(b"tXXXXXXXX_rXXYYXXYY");
    key_a.extend(b"11111111");
    let key_b = encode_bytes(b"tXXXXXXXX_rXXYYXXZZ");
    b.iter(|| {
        assert!(!Key::is_user_key_eq(
            black_box(&key_a),
            black_box(key_b.as_slice())
        ));
    });
}

fn bench_ne_naive(b: &mut Bencher) {
    let mut key_a = encode_bytes(b"tXXXXXXXX_rXXYYXXYY");
    key_a.extend(b"11111111");
    let key_b = encode_bytes(b"tXXXXXXXX_rXXYYXXZZ");
    b.iter(|| {
        let key = black_box(&key_a);
        assert!(!(&key[..key.len() - 8] == black_box(key_b.as_slice())));
    });
}

fn bench_ne_new(b: &mut Bencher) {
    let mut key_a = encode_bytes(b"tXXXXXXXX_rXXYYXXYY");
    key_a.extend(b"11111111");
    let key_b = encode_bytes(b"tXXXXXXXX_rXXYYXXZZ");
    b.iter(|| {
        let key = black_box(&key_a);
        assert!(!is_key_eq(
            &key[..key.len() - 8],
            black_box(key_b.as_slice())
        ));
    });
}

fn main() {
    let mut criterion = Criterion::default().sample_size(10).configure_from_args();

    let mut inputs = vec![];
    inputs.push(
        ScanConfig {
            number_of_keys: 100000,
            number_of_versions: 1,
            key_len: 19,
            value_len: 32,
            compacted: true,
        }
        .into_input(),
    );
    //    inputs.push(
    //        ScanConfig {
    //            number_of_keys: 100000,
    //            number_of_versions: 1,
    //            key_len: 19,
    //            value_len: 32,
    //            compacted: false,
    //        }
    //        .into_input(),
    //    );
    //    inputs.push(
    //        ScanConfig {
    //            number_of_keys: 10000,
    //            number_of_versions: 10,
    //            key_len: 19,
    //            value_len: 32,
    //            compacted: true,
    //        }
    //        .into_input(),
    //    );
    //    // TPC-H SF=1 item table.
    //    inputs.push(
    //        ScanConfig {
    //            number_of_keys: 300000,
    //            number_of_versions: 1,
    //            key_len: 19,
    //            value_len: 172,
    //            compacted: true,
    //        }
    //        .into_input(),
    //    );

    criterion.bench_function_over_inputs(
        "mvcc_forward_scan",
        bench_mvcc_forward_scan,
        inputs.clone(),
    );
    criterion.bench_function_over_inputs(
        "range_forward_scanner_1",
        bench_range_forward_scanner_1,
        inputs.clone(),
    );
    criterion.bench_function_over_inputs(
        "range_forward_scanner_1024",
        bench_range_forward_scanner_1024,
        inputs.clone(),
    );
//    criterion.bench_function_over_inputs("iter_forward", bench_iter_forward, inputs.clone());
//    criterion.bench_function_over_inputs(
//        "iter_forward_3_cf",
//        bench_iter_forward_3_cf,
//        inputs.clone(),
//    );
    criterion.bench_function_over_inputs(
        "iter_forward_with_value",
        bench_iter_forward_with_value,
        inputs.clone(),
    );
//    criterion.bench_function_over_inputs(
//        "iter_forward_alloc_vec_for_kv",
//        bench_iter_forward_alloc_vec_for_kv,
//        inputs.clone(),
//    );
    criterion.bench_function_over_inputs(
        "cursor_forward",
        bench_cursor_forward,
        inputs.clone(),
    );
//    criterion.bench_function_over_inputs(
//        "cursor_forward_alloc_vec_for_kv",
//        bench_cursor_forward_alloc_vec_for_kv,
//        inputs.clone(),
//    );

    //    criterion.bench_function("eq_key", bench_eq_key);
    //    criterion.bench_function("eq_naive", bench_eq_naive);
    //    criterion.bench_function("eq_new", bench_eq_new);
    //    criterion.bench_function("ne_key", bench_ne_key);
    //    criterion.bench_function("ne_naive", bench_ne_naive);
    //    criterion.bench_function("ne_new", bench_ne_new);

    criterion.final_summary();
}

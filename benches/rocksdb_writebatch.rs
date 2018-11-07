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

/// This suite contains benchmarks for several different configurations generated
/// dynamically. Thus it has `harness = false`.
extern crate criterion;

extern crate rocksdb;
extern crate tempdir;

use criterion::{black_box, Bencher, Criterion};
use rocksdb::{Writable, WriteBatch, DB};
use tempdir::TempDir;

fn writebatch(db: &DB, round: usize, batch_keys: usize) {
    let v = b"operators are syntactic sugar for calls to methods of built-in traits";
    for r in 0..round {
        let batch = WriteBatch::new();
        for i in 0..batch_keys {
            let k = format!("key_round{}_key{}", r, i);
            batch.put(black_box(k.as_bytes()), black_box(v)).unwrap();
        }
        db.write(batch).unwrap()
    }
}

#[cfg_attr(feature = "cargo-clippy", allow(trivially_copy_pass_by_ref))]
fn bench_writebatch(b: &mut Bencher, batch_keys: &usize) {
    let path = TempDir::new("/tmp/rocksdb_write_batch_bench").unwrap();
    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
    let key_count = 1 << 13;
    let round = key_count / batch_keys;
    b.iter(|| {
        writebatch(&db, round, *batch_keys);
    });
}

fn fill_writebatch(wb: &WriteBatch, target_size: usize) {
    let (k, v) = (b"this is the key", b"this is the value");
    loop {
        wb.put(k, v).unwrap();
        if wb.data_size() >= target_size {
            break;
        }
    }
}

fn bench_writebatch_without_capacity(b: &mut Bencher) {
    b.iter(|| {
        let wb = WriteBatch::new();
        fill_writebatch(&wb, 4096);
    });
}

fn bench_writebatch_with_capacity(b: &mut Bencher) {
    b.iter(|| {
        let wb = WriteBatch::with_capacity(4096);
        fill_writebatch(&wb, 4096);
    });
}

fn main() {
    let mut criterion = Criterion::default().sample_size(10).configure_from_args();

    criterion.bench_function("without_capacity", bench_writebatch_without_capacity);
    criterion.bench_function("with_capacity", bench_writebatch_with_capacity);

    let batch_sizes = if criterion.is_test_mode() {
        vec![32]
    } else {
        vec![1, 2, 3, 8, 16, 32, 64, 128, 256, 512, 1024]
    };
    criterion.bench_function_over_inputs("writebatch", bench_writebatch, batch_sizes);

    criterion.final_summary();
}

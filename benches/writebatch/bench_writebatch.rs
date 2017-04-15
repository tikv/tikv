// Copyright 2016 PingCAP, Inc.
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

use std::time::*;
use test::Bencher;
use tempdir::TempDir;
use rocksdb::{DB, WriteBatch, Writable};

fn bench_writebatch_impl(round: usize, batch_keys: usize) {
    let path = TempDir::new("/tmp/rocksdb_write_batch_bench").unwrap();
    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
    let v = b"operators are syntactic sugar for calls to methods of built-in traits";
    for r in 0..round {
        let batch = WriteBatch::new();
        for i in 0..batch_keys {
            let k = format!("key_round{}_key{}", r, i);
            batch.put(k.as_bytes(), v);
        }
        db.write(batch).unwrap()
    }
}

#[bench]
fn bench_batch_1(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 1;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

#[bench]
fn bench_batch_2(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 2;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

#[bench]
fn bench_batch_4(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 4;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

#[bench]
fn bench_batch_8(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 8;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

#[bench]
fn bench_batch_16(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 16;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

#[bench]
fn bench_batch_32(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 32;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

#[bench]
fn bench_batch_64(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 64;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

#[bench]
fn bench_batch_128(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 128;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

#[bench]
fn bench_batch_256(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 256;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

#[bench]
fn bench_batch_512(b: &mut Bencher) {
    let key_count = 1<<13;
    let batch_keys = 512;
    let round = key_count / batch_keys;
    b.iter(|| {
        bench_writebatch_impl(round, batch_keys);
    });
}

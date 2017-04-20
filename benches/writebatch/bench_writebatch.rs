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

use test::Bencher;
use tempdir::TempDir;
use rocksdb::{DB, WriteBatch, Writable};

fn writebatch(db: &DB, round: usize, batch_keys: usize) {
    let v = b"operators are syntactic sugar for calls to methods of built-in traits";
    for r in 0..round {
        let batch = WriteBatch::new();
        for i in 0..batch_keys {
            let k = format!("key_round{}_key{}", r, i);
            batch.put(k.as_bytes(), v).unwrap();
        }
        db.write(batch).unwrap()
    }
}

fn bench_writebatch_impl(b: &mut Bencher, batch_keys: usize) {
    let path = TempDir::new("/tmp/rocksdb_write_batch_bench").unwrap();
    let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
    let key_count = 1 << 13;
    let round = key_count / batch_keys;
    b.iter(|| {
        writebatch(&db, round, batch_keys);
    });
}

#[bench]
fn bench_writebatch_1(b: &mut Bencher) {
    bench_writebatch_impl(b, 1);
}

#[bench]
fn bench_writebatch_2(b: &mut Bencher) {
    bench_writebatch_impl(b, 2);
}

#[bench]
fn bench_writebatch_4(b: &mut Bencher) {
    bench_writebatch_impl(b, 4);
}

#[bench]
fn bench_writebatch_8(b: &mut Bencher) {
    bench_writebatch_impl(b, 8);
}

#[bench]
fn bench_writebatch_16(b: &mut Bencher) {
    bench_writebatch_impl(b, 16);
}

#[bench]
fn bench_writebatch_32(b: &mut Bencher) {
    bench_writebatch_impl(b, 32);
}

#[bench]
fn bench_writebatch_64(b: &mut Bencher) {
    bench_writebatch_impl(b, 64);
}

#[bench]
fn bench_writebatch_128(b: &mut Bencher) {
    bench_writebatch_impl(b, 128);
}

#[bench]
fn bench_writebatch_256(b: &mut Bencher) {
    bench_writebatch_impl(b, 256);
}

#[bench]
fn bench_writebatch_512(b: &mut Bencher) {
    bench_writebatch_impl(b, 512);
}

#[bench]
fn bench_writebatch_1024(b: &mut Bencher) {
    bench_writebatch_impl(b, 1024);
}

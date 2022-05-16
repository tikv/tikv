// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_rocks::{raw::DB, Compat, RocksWriteBatch};
use engine_traits::{Mutable, WriteBatch, WriteBatchExt};
use tempfile::Builder;
use test::Bencher;

fn writebatch(db: &Arc<DB>, round: usize, batch_keys: usize) {
    let v = b"operators are syntactic sugar for calls to methods of built-in traits";
    for r in 0..round {
        let mut batch = db.c().write_batch();
        for i in 0..batch_keys {
            let k = format!("key_round{}_key{}", r, i);
            batch.put(k.as_bytes(), v).unwrap();
        }
        batch.write().unwrap()
    }
}

fn bench_writebatch_impl(b: &mut Bencher, batch_keys: usize) {
    let path = Builder::new()
        .prefix("/tmp/rocksdb_write_batch_bench")
        .tempdir()
        .unwrap();
    let db = Arc::new(DB::open_default(path.path().to_str().unwrap()).unwrap());
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

fn fill_writebatch(wb: &mut RocksWriteBatch, target_size: usize) {
    let (k, v) = (b"this is the key", b"this is the value");
    loop {
        wb.put(k, v).unwrap();
        if wb.data_size() >= target_size {
            break;
        }
    }
}

#[bench]
fn bench_writebatch_without_capacity(b: &mut Bencher) {
    let path = Builder::new()
        .prefix("/tmp/rocksdb_write_batch_bench")
        .tempdir()
        .unwrap();
    let db = Arc::new(DB::open_default(path.path().to_str().unwrap()).unwrap());
    b.iter(|| {
        let mut wb = db.c().write_batch();
        fill_writebatch(&mut wb, 4096);
    });
}

#[bench]
fn bench_writebatch_with_capacity(b: &mut Bencher) {
    let path = Builder::new()
        .prefix("/tmp/rocksdb_write_batch_bench")
        .tempdir()
        .unwrap();
    let db = Arc::new(DB::open_default(path.path().to_str().unwrap()).unwrap());
    b.iter(|| {
        let mut wb = db.c().write_batch_with_cap(4096);
        fill_writebatch(&mut wb, 4096);
    });
}

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use test::{black_box, Bencher};
use tikv::storage::{Statistics, Store};

fn bench_impl(b: &mut Bencher, value_size: usize) {
    let s = super::prepare_table_data(30000, value_size);
    let key = super::make_key(15000);
    let store = super::new_snapshot_store(&s);
    b.iter(|| {
        let mut stats = Statistics::default();
        let v = black_box(&store).get(black_box(&key), &mut stats);
        black_box(v.unwrap().unwrap());
    });
}

/// Point get single key in a 30000 key space (value size = 64).
#[bench]
fn bench_short(b: &mut Bencher) {
    bench_impl(b, 64);
}

/// Point get single key in a 30000 key space (value size = 1024).
#[bench]
fn bench_long(b: &mut Bencher) {
    bench_impl(b, 1024);
}

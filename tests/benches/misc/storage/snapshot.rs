// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::Context;
use test::{black_box, Bencher};
use tikv::storage::Engine;

#[bench]
fn bench_rocksdb_snapshot(b: &mut Bencher) {
    let s = super::prepare_table_data(1000, 64);
    let engine = s.get_engine();
    b.iter(|| {
        let snapshot = black_box(&engine).snapshot(&Context::default()).unwrap();
        black_box(snapshot);
    });
}

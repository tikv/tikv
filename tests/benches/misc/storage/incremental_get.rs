// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use test::{black_box, Bencher};
use tikv::storage::{Statistics, Store};

/// Point get 500 adjacent keys in a 30000 key space (value size = 64).
#[bench]
fn bench_get_near(b: &mut Bencher) {
    let s = super::prepare_table_data(30000, 64);
    let keys = super::prepare_get_keys(500, 1);
    let store = super::new_snapshot_store(&s);
    b.iter(|| {
        let store = black_box(&store);
        let mut stats = Statistics::default();
        for key in black_box(&keys) {
            black_box(store.get(key, &mut stats).unwrap());
        }
    });
}

/// Point get (incremental) 500 adjacent keys in a 30000 key space (value size = 64).
#[bench]
fn bench_incremental_get_near(b: &mut Bencher) {
    let s = super::prepare_table_data(30000, 64);
    let keys = super::prepare_get_keys(500, 1);
    let mut store = super::new_snapshot_store(&s);
    b.iter(|| {
        let store = black_box(&mut store);
        for key in black_box(&keys) {
            black_box(store.incremental_get(key).unwrap());
        }
    })
}

/// Point get 500 non-adjacent keys in a 30000 key space (value size = 64).
#[bench]
fn bench_get_far(b: &mut Bencher) {
    let s = super::prepare_table_data(30000, 64);
    let keys = super::prepare_get_keys(500, 32);
    let store = super::new_snapshot_store(&s);
    b.iter(|| {
        let store = black_box(&store);
        let mut stats = Statistics::default();
        for key in black_box(&keys) {
            black_box(store.get(key, &mut stats).unwrap());
        }
    });
}

/// Point get (incremental) 500 non-adjacent keys in a 30000 key space (value size = 64).
#[bench]
fn bench_incremental_get_far(b: &mut Bencher) {
    let s = super::prepare_table_data(30000, 64);
    let keys = super::prepare_get_keys(500, 32);
    let mut store = super::new_snapshot_store(&s);
    b.iter(|| {
        let store = black_box(&mut store);
        for key in black_box(&keys) {
            black_box(store.incremental_get(key).unwrap());
        }
    })
}

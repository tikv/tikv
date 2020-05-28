// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::Context;
use test::{black_box, Bencher};
use test_storage::SyncTestStorageBuilder;
use test_util::*;
use tikv::storage::{Scanner, Store};
use txn_types::{Key, Mutation};

fn bench_scan_asc_impl(b: &mut Bencher, value_size: usize) {
    let s = super::prepare_table_data(30000, value_size);
    let lower = super::make_key(14000);
    let upper = super::make_key(14100);
    let store = super::new_snapshot_store(&s);
    b.iter(|| {
        let lower = test::black_box(&lower).clone();
        let upper = test::black_box(&upper).clone();
        let mut scanner = store
            .scanner(false, false, false, Some(lower), Some(upper))
            .unwrap();
        let mut n = 0;
        loop {
            let ret = scanner.next().unwrap();
            if ret.is_none() {
                break;
            }
            let kv = ret.unwrap();
            black_box(kv);
            n += 1;
        }
        assert_eq!(n, 100);
    })
}

/// Ascending scan 100 keys single key in a 30000 key space (value size = 64).
#[bench]
fn bench_scan_asc_short(b: &mut Bencher) {
    bench_scan_asc_impl(b, 64);
}

/// Ascending scan 100 keys single key in a 30000 key space (value size = 1024).
#[bench]
fn bench_scan_asc_long(b: &mut Bencher) {
    bench_scan_asc_impl(b, 1024);
}

/// In mvcc kv is not actually deleted, which may cause performance issue
/// when doing scan.
#[ignore]
#[bench]
fn bench_tombstone_scan(b: &mut Bencher) {
    let store = SyncTestStorageBuilder::new().build().unwrap();
    let mut ts_generator = 1..;

    let mut kvs = KvGenerator::new(100, 1000);

    for (k, v) in kvs.take(100_000) {
        let mut ts = ts_generator.next().unwrap();
        store
            .prewrite(
                Context::default(),
                vec![Mutation::Put((Key::from_raw(&k), v))],
                k.clone(),
                ts,
            )
            .expect("");
        store
            .commit(
                Context::default(),
                vec![Key::from_raw(&k)],
                ts,
                ts_generator.next().unwrap(),
            )
            .expect("");

        ts = ts_generator.next().unwrap();
        store
            .prewrite(
                Context::default(),
                vec![Mutation::Delete(Key::from_raw(&k))],
                k.clone(),
                ts,
            )
            .expect("");
        store
            .commit(
                Context::default(),
                vec![Key::from_raw(&k)],
                ts,
                ts_generator.next().unwrap(),
            )
            .expect("");
    }

    kvs = KvGenerator::new(100, 1000);
    b.iter(|| {
        let (k, _) = kvs.next().unwrap();
        assert!(store
            .scan(
                Context::default(),
                Key::from_raw(&k),
                None,
                1,
                false,
                ts_generator.next().unwrap(),
            )
            .unwrap()
            .is_empty())
    })
}

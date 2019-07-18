// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use test::Bencher;

use kvproto::kvrpcpb::Context;

use test_storage::SyncTestStorageBuilder;
use test_util::*;
use tikv::storage::{Key, Mutation};

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
                ts_generator.next().unwrap()
            )
            .unwrap()
            .is_empty())
    })
}

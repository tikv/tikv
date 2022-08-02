// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::Context;
use test_storage::{SyncTestStorageApiV1, SyncTestStorageBuilderApiV1};
use tidb_query_datatype::codec::table;
use tikv::storage::{kv::RocksEngine, mvcc::SnapshotReader, Engine};
use txn_types::{Key, Mutation};

fn prepare_mvcc_data(key: &Key, n: u64) -> SyncTestStorageApiV1<RocksEngine> {
    let store = SyncTestStorageBuilderApiV1::default().build().unwrap();
    for ts in 1..=n {
        let mutation = Mutation::make_put(key.clone(), b"value".to_vec());
        store
            .prewrite(
                Context::default(),
                vec![mutation],
                key.clone().into_encoded(),
                ts,
            )
            .unwrap();
        store
            .commit(Context::default(), vec![key.clone()], ts, ts + 1)
            .unwrap();
    }
    let engine = store.get_engine();
    let db = engine.get_rocksdb().get_sync_db();
    db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);
    store
}

fn bench_get_txn_commit_record(b: &mut test::Bencher, n: u64) {
    let key = Key::from_raw(&table::encode_row_key(1, 0));
    let store = prepare_mvcc_data(&key, n);
    b.iter(|| {
        let mut mvcc_reader = SnapshotReader::new(
            1.into(),
            store.get_engine().snapshot(Default::default()).unwrap(),
            true,
        );
        mvcc_reader
            .get_txn_commit_record(&key)
            .unwrap()
            .unwrap_single_record();
    });
}

#[bench]
fn bench_get_txn_commit_record_100(c: &mut test::Bencher) {
    bench_get_txn_commit_record(c, 100);
}

#[bench]
fn bench_get_txn_commit_record_5(c: &mut test::Bencher) {
    bench_get_txn_commit_record(c, 5);
}

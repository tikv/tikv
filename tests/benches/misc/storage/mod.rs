// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod incremental_get;
mod key;
mod point_get;
mod scan;
mod snapshot;

use engine_rocks::RocksSnapshot;
use kvproto::kvrpcpb::{Context, IsolationLevel};
use std::sync::Arc;
use test_storage::{SyncTestStorage, SyncTestStorageBuilder};
use tidb_query_datatype::codec::table;
use tikv::storage::{Engine, RocksEngine, SnapshotStore};
use txn_types::{Key, Mutation};

const TABLE_ID: i64 = 5;

fn prepare_table_data(keys: usize, value_size: usize) -> SyncTestStorage<RocksEngine> {
    let store = SyncTestStorageBuilder::new().build().unwrap();
    let mut mutations = Vec::new();
    let mut k = Vec::new();
    for i in 0..keys {
        let user_key = table::encode_row_key(TABLE_ID, i as i64);
        let user_value = vec![b'x'; value_size];
        let key = Key::from_raw(&user_key);
        let mutation = Mutation::Put((key.clone(), user_value));
        mutations.push(mutation);
        k.push(key);
    }

    let pk = table::encode_row_key(TABLE_ID, 0);

    store
        .prewrite(Context::default(), mutations, pk, 1)
        .unwrap();
    store.commit(Context::default(), k, 1, 2).unwrap();

    let engine = store.get_engine();
    let db = engine.get_rocksdb();
    db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);

    return store;
}

fn make_key(handle: i64) -> Key {
    Key::from_raw(&table::encode_row_key(TABLE_ID, handle))
}

fn prepare_get_keys(keys: usize, step: usize) -> Vec<Key> {
    let mut get_keys = Vec::new();
    let mut ki = 0;
    for _ in 0..keys {
        get_keys.push(make_key(ki));
        ki += step as i64;
    }
    get_keys
}

fn new_snapshot_store(storage: &SyncTestStorage<RocksEngine>) -> SnapshotStore<Arc<RocksSnapshot>> {
    let engine = storage.get_engine();
    let snapshot = engine.snapshot(&Context::default()).unwrap();
    let store = SnapshotStore::new(
        snapshot,
        10.into(),
        IsolationLevel::Si,
        true,
        Default::default(),
        false,
    );
    store
}

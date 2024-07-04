// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crossbeam::epoch;
use engine_rocks::{util::new_engine, RocksEngine};
use engine_traits::{
    CacheRange, CfName, Iterable, Iterator, Peekable, SyncMutable, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use proptest::prelude::*;
use tikv_util::config::{ReadableSize, VersionTrack};

use super::engine::SkiplistHandle;
use crate::{
    decode_key, engine::SkiplistEngine, keys::encode_key, memory_controller::MemoryController,
    InternalBytes, RangeCacheEngineConfig,
};

#[derive(Clone)]
enum Operation {
    Put(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
    Delete(Vec<u8>),
    Scan(Vec<u8>, usize),
    DeleteRange(Vec<u8>, Vec<u8>),
}

impl std::fmt::Debug for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Put(k, v) => write!(
                f,
                "Put('{}', '{}')",
                hex::encode_upper(k),
                hex::encode_upper(v)
            ),
            Operation::Get(k) => write!(f, "Get('{}')", hex::encode_upper(k)),
            Operation::Delete(k) => write!(f, "Delete('{}')", hex::encode_upper(k)),
            Operation::Scan(k, l) => write!(f, "Scan('{}', '{}')", hex::encode_upper(k), l),
            Operation::DeleteRange(k1, k2) => write!(
                f,
                "DeleteRange('{}', '{}')",
                hex::encode_upper(k1),
                hex::encode_upper(k2)
            ),
        }
    }
}

fn gen_operations(value_size: usize) -> impl Strategy<Value = (CfName, Vec<Operation>)> {
    let key_size: usize = 16;
    let ops = prop::collection::vec(
        prop_oneof![
            (
                // The length of a valid key must be greater than 0.
                prop::collection::vec(prop::num::u8::ANY, 1..key_size),
                prop::collection::vec(prop::num::u8::ANY, 0..value_size)
            )
                .prop_map(|(k, v)| Operation::Put(k, v)),
            prop::collection::vec(prop::num::u8::ANY, 0..key_size).prop_map(|k| Operation::Get(k)),
            prop::collection::vec(prop::num::u8::ANY, 0..key_size)
                .prop_map(|k| Operation::Delete(k)),
            (
                prop::collection::vec(prop::num::u8::ANY, 0..key_size),
                0..10usize
            )
                .prop_map(|(k, v)| Operation::Scan(k, v)),
            (
                prop::collection::vec(prop::num::u8::ANY, 0..key_size),
                prop::collection::vec(prop::num::u8::ANY, 0..key_size)
            )
                .prop_map(|(k1, k2)| Operation::DeleteRange(k1, k2)),
        ],
        0..100,
    );
    let cf = prop_oneof![Just(CF_DEFAULT), Just(CF_LOCK), Just(CF_WRITE)];
    (cf, ops)
}

fn scan_rocksdb(
    db: &RocksEngine,
    cf: CfName,
    start: &[u8],
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut iter = db.iterator(cf).unwrap();
    iter.seek(start).unwrap();
    let mut num = 0;
    let mut res = vec![];
    while num < limit {
        if iter.valid().unwrap() {
            let k = iter.key().to_vec();
            let v = iter.value().to_vec();

            res.push((k, v));
            num += 1;
        } else {
            break;
        }
        iter.next().unwrap();
    }
    res
}

fn scan_skiplist(
    handle: SkiplistHandle,
    start: &InternalBytes,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut iter = handle.iterator();
    let guard = &epoch::pin();
    iter.seek(start, guard);
    let mut num = 0;
    let mut res = vec![];
    while num < limit {
        if iter.valid() {
            let k = decode_key(iter.key().as_slice()).user_key.to_vec();
            let v = iter.value().as_slice().to_vec();

            res.push((k, v));
            num += 1;
        } else {
            break;
        }
        iter.next(guard);
    }
    res
}

fn test_rocksdb_skiplist_basic_operations(cf: CfName, operations: Vec<Operation>) {
    let skiplist = SkiplistEngine::default();

    let path_rocks = tempfile::tempdir().unwrap();
    let db_rocks = new_engine(
        path_rocks.path().to_str().unwrap(),
        &[CF_DEFAULT, CF_LOCK, CF_WRITE],
    )
    .unwrap();

    let mut cfg = RangeCacheEngineConfig::default();
    cfg.soft_limit_threshold = Some(ReadableSize::gb(1));
    cfg.hard_limit_threshold = Some(ReadableSize::gb(2));
    let controller = Arc::new(MemoryController::new(
        Arc::new(VersionTrack::new(cfg)),
        skiplist.clone(),
    ));

    let skiplist_args = |k: Vec<u8>, v: Option<Vec<u8>>| {
        let mut key = encode_key(&k, 0, crate::ValueType::Value);
        key.set_memory_controller(controller.clone());
        let value = v.map(|v| {
            let mut value = InternalBytes::from_vec(v);
            value.set_memory_controller(controller.clone());
            value
        });
        let guard = epoch::pin();
        let handle = skiplist.cf_handle(cf);
        (handle, key, value, guard)
    };

    for op in operations {
        match op {
            Operation::Put(k, v) => {
                db_rocks.put_cf(cf, &k, &v).unwrap();

                let (handle, key, value, guard) = skiplist_args(k, Some(v));
                handle.insert(key, value.unwrap(), &guard)
            }
            Operation::Get(k) => {
                let res_rocks = db_rocks.get_value_cf(cf, &k).unwrap();
                let (handle, key, _value, guard) = skiplist_args(k, None);
                let res_skiplist = handle.get(&key, &guard);
                assert_eq!(
                    res_rocks.as_deref(),
                    res_skiplist.map(|e| e.value().as_slice())
                );
            }
            Operation::Delete(k) => {
                db_rocks.delete_cf(cf, &k).unwrap();

                let (handle, key, _value, guard) = skiplist_args(k, None);
                handle.remove(&key, &guard)
            }
            Operation::Scan(k, limit) => {
                let res_rocks = scan_rocksdb(&db_rocks, cf, &k, limit);
                let (handle, key, _value, _guard) = skiplist_args(k, None);
                let res_titan = scan_skiplist(handle, &key, limit);
                assert_eq!(res_rocks, res_titan);
            }
            Operation::DeleteRange(k1, k2) => {
                if k1 <= k2 {
                    db_rocks.delete_range_cf(cf, &k1, &k2).unwrap();
                    let range = CacheRange::new(k1.clone(), k2.clone());
                    skiplist.delete_range_cf(cf, &range);
                } else {
                    db_rocks.delete_range_cf(cf, &k2, &k1).unwrap();
                    let range = CacheRange::new(k2.clone(), k1.clone());
                    skiplist.delete_range_cf(cf, &range);
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    #[test]
    fn test_rocksdb_skiplist_basic_ops((cf, operations) in gen_operations(1000)) {
        test_rocksdb_skiplist_basic_operations(cf, operations);
    }
}

// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crossbeam::epoch;
use engine_rocks::{util::new_engine, RocksEngine};
use engine_traits::{
    CacheRegion, CfName, Iterable, Iterator, Peekable, SyncMutable, CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use proptest::prelude::*;
use tikv_util::config::{ReadableSize, VersionTrack};
use txn_types::{Key, TimeStamp};

use super::engine::SkiplistHandle;
use crate::{
    decode_key, engine::SkiplistEngine, keys::encode_key, memory_controller::MemoryController,
    InMemoryEngineConfig, InternalBytes,
};

// This fixed mvcc suffix is used for CF_WRITE and CF_DEFAULT in prop test.
const MVCC_SUFFIX: u64 = 10;

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
                log_wrappers::Value::key(k),
                log_wrappers::Value::key(v)
            ),
            Operation::Get(k) => write!(f, "Get('{}')", log_wrappers::Value::key(k)),
            Operation::Delete(k) => write!(f, "Delete('{}')", log_wrappers::Value::key(k)),
            Operation::Scan(k, l) => write!(f, "Scan('{}', '{}')", log_wrappers::Value::key(k), l),
            Operation::DeleteRange(k1, k2) => write!(
                f,
                "DeleteRange('{}', '{}')",
                log_wrappers::Value::key(k1),
                log_wrappers::Value::key(k2)
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

    let mut cfg = InMemoryEngineConfig::default();
    cfg.evict_threshold = Some(ReadableSize::gb(1));
    cfg.capacity = Some(ReadableSize::gb(2));
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

    // Delete range in SkiplistEngine considers MVCC suffix for CF_DEFAULT and
    // CF_WRITE, so we append the suffix for them.
    for op in operations {
        match op {
            Operation::Put(mut k, v) => {
                if cf != CF_LOCK {
                    k = Key::from_raw(&k)
                        .append_ts(TimeStamp::new(MVCC_SUFFIX))
                        .into_encoded();
                }
                db_rocks.put_cf(cf, &k, &v).unwrap();

                let (handle, key, value, guard) = skiplist_args(k, Some(v));
                handle.insert(key, value.unwrap(), &guard)
            }
            Operation::Get(mut k) => {
                if cf != CF_LOCK {
                    k = Key::from_raw(&k)
                        .append_ts(TimeStamp::new(MVCC_SUFFIX))
                        .into_encoded();
                }
                let res_rocks = db_rocks.get_value_cf(cf, &k).unwrap();
                let (handle, key, _value, guard) = skiplist_args(k, None);
                let res_skiplist = handle.get(&key, &guard);
                assert_eq!(
                    res_rocks.as_deref(),
                    res_skiplist.map(|e| e.value().as_slice())
                );
            }
            Operation::Delete(mut k) => {
                if cf != CF_LOCK {
                    k = Key::from_raw(&k)
                        .append_ts(TimeStamp::new(MVCC_SUFFIX))
                        .into_encoded();
                }
                db_rocks.delete_cf(cf, &k).unwrap();

                let (handle, key, _value, guard) = skiplist_args(k, None);
                handle.remove(&key, &guard)
            }
            Operation::Scan(mut k, limit) => {
                if cf != CF_LOCK {
                    k = Key::from_raw(&k)
                        .append_ts(TimeStamp::new(MVCC_SUFFIX))
                        .into_encoded();
                }
                let res_rocks = scan_rocksdb(&db_rocks, cf, &k, limit);
                let (handle, key, _value, _guard) = skiplist_args(k, None);
                let res_titan = scan_skiplist(handle, &key, limit);
                assert_eq!(res_rocks, res_titan);
            }
            Operation::DeleteRange(mut k1, mut k2) => {
                if k1 > k2 {
                    (k1, k2) = (k2, k1);
                }

                let range = CacheRegion::new(1, 0, k1.clone(), k2.clone());
                skiplist.delete_range_cf(cf, &range);

                if cf != CF_LOCK {
                    k1 = Key::from_raw(&k1)
                        .append_ts(TimeStamp::new(MVCC_SUFFIX))
                        .into_encoded();
                    k2 = Key::from_raw(&k2)
                        .append_ts(TimeStamp::new(MVCC_SUFFIX))
                        .into_encoded();
                }
                db_rocks.delete_range_cf(cf, &k1, &k2).unwrap();
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

#[test]
fn test_case1() {
    let de = |data| hex::decode(data).unwrap();
    let cf = CF_WRITE;
    let operations = [
        Operation::Put(de("E2"), de("38CC98E09D9CB1D1")),
        Operation::DeleteRange(de(""), de("E2")),
        Operation::Scan(de("2C0F698A"), 3),
    ];
    test_rocksdb_skiplist_basic_operations(cf, operations.to_vec());
}

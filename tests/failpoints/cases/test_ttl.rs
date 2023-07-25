// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::Iterator as StdIterator, sync::mpsc::channel, time::Duration};

use api_version::{test_kv_format_impl, ApiV1Ttl, KvFormat, RawValue};
use engine_rocks::{raw::CompactOptions, util::get_cf_handle};
use engine_traits::{IterOptions, MiscExt, Peekable, SyncMutable, CF_DEFAULT};
use futures::executor::block_on;
use kvproto::{kvrpcpb, kvrpcpb::Context};
use tikv::{
    config::DbConfig,
    server::{
        gc_worker::{GcTask, TestGcRunner},
        ttl::check_ttl_and_compact_files,
    },
    storage::{
        kv::{SnapContext, TestEngineBuilder},
        lock_manager::MockLockManager,
        raw::encoded::RawEncodeSnapshot,
        test_util::{expect_ok_callback, expect_value},
        Engine, Iterator, Snapshot, Statistics, TestStorageBuilder,
    },
};
use txn_types::Key;

#[test]
fn test_ttl_checker() {
    test_kv_format_impl!(test_ttl_checker_impl<ApiV1Ttl ApiV2>);
}

fn test_ttl_checker_impl<F: KvFormat>() {
    fail::cfg("ttl_current_ts", "return(100)").unwrap();
    let mut cfg = DbConfig::default();
    cfg.defaultcf.disable_auto_compactions = true;
    let dir = tempfile::TempDir::new().unwrap();
    let builder = TestEngineBuilder::new()
        .path(dir.path())
        .api_version(F::TAG);
    let engine = builder.build_with_cfg(&cfg).unwrap();
    let kvdb = engine.get_rocksdb();

    // Make all entries earlier than safe point.
    // TTL expired entries can only be collected when commit_ts < safe_point.
    let commit_ts = 100;
    let mut gc_runner = TestGcRunner::new(200);

    let mut do_compact = |start_key: &[u8], end_key: &[u8], mut expect_keys: usize| {
        gc_runner.prepare_gc(&kvdb);
        check_ttl_and_compact_files(&kvdb, start_key, end_key, false);

        if F::TAG == kvrpcpb::ApiVersion::V2 {
            while let Ok(Some(task)) = gc_runner.gc_receiver.recv_timeout(Duration::from_secs(3)) {
                match task {
                    GcTask::RawGcKeys { keys, .. } => {
                        expect_keys = expect_keys.checked_sub(keys.len()).unwrap();

                        // Delete keys by `delete_cf` for simplicity.
                        // In real cases, all old MVCC versions of `key` should be deleted.
                        // See `GcRunner::raw_gc_keys`.
                        for key in keys {
                            let db_key =
                                keys::data_key(key.append_ts(commit_ts.into()).as_encoded());
                            kvdb.delete_cf(CF_DEFAULT, &db_key).unwrap();
                        }

                        if expect_keys == 0 {
                            break;
                        }
                    }
                    _ => unreachable!(),
                }
            }
            assert_eq!(expect_keys, 0);
        }

        gc_runner.post_gc();
    };

    let cases: Vec<
        Vec<(&[u8] /* key */, Option<u64> /* expire_ts */)>, /* a batch, will be written to
                                                              * individual sst file by
                                                              * `flush_cf` */
    > = vec![
        vec![(b"r\0key0", Some(10)), (b"r\0key1", Some(110))],
        vec![(b"r\0key2", Some(120)), (b"r\0key3", Some(20))],
        vec![(b"r\0key4", None)],
        vec![(b"r\0key5", Some(10))],
    ];
    let keys = cases
        .into_iter()
        .flat_map(|batch| {
            let keys = batch
                .into_iter()
                .map(|(key, expire_ts)| {
                    let key = make_raw_key::<F>(key, Some(commit_ts));
                    let value = RawValue {
                        user_value: vec![0; 10],
                        expire_ts,
                        is_delete: false,
                    };
                    kvdb.put_cf(CF_DEFAULT, &key, &F::encode_raw_value_owned(value))
                        .unwrap();
                    key
                })
                .collect::<Vec<_>>();
            kvdb.flush_cf(CF_DEFAULT, true).unwrap();
            keys
        })
        .collect::<Vec<_>>();

    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[0]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[1]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[2]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[3]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[4]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[5]).unwrap().is_some());

    do_compact(b"zr\0key1", b"zr\0key25", 2); // cover key0 ~ key3
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[0]).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[1]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[2]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[3]).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[4]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[5]).unwrap().is_some());

    do_compact(b"zr\0key2", b"zr\0key6", 2); // cover key2 ~ key5
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[0]).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[1]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[2]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[3]).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[4]).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, &keys[5]).unwrap().is_none());
}

#[test]
fn test_ttl_compaction_filter() {
    test_ttl_compaction_filter_impl::<ApiV1Ttl>();
}

fn test_ttl_compaction_filter_impl<F: KvFormat>() {
    fail::cfg("ttl_current_ts", "return(100)").unwrap();
    let mut cfg = DbConfig::default();
    cfg.writecf.disable_auto_compactions = true;
    let dir = tempfile::TempDir::new().unwrap();
    let builder = TestEngineBuilder::new()
        .path(dir.path())
        .api_version(F::TAG);
    let engine = builder.build_with_cfg(&cfg).unwrap();
    let kvdb = engine.get_rocksdb();

    let key1 = b"zr\0key1";
    let value1 = RawValue {
        user_value: vec![0; 10],
        expire_ts: Some(10),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key1, &F::encode_raw_value_owned(value1))
        .unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();

    let db = kvdb.as_inner();
    let handle = get_cf_handle(db, CF_DEFAULT).unwrap();
    db.compact_range_cf_opt(handle, &CompactOptions::new(), None, None);

    assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());

    let key2 = b"zr\0key2";
    let value2 = RawValue {
        user_value: vec![0; 10],
        expire_ts: Some(120),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key2, &F::encode_raw_value_owned(value2))
        .unwrap();
    let key3 = b"zr\0key3";
    let value3 = RawValue {
        user_value: vec![0; 10],
        expire_ts: Some(20),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key3, &F::encode_raw_value_owned(value3))
        .unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();

    let key4 = b"zr\0key4";
    let value4 = RawValue {
        user_value: vec![0; 10],
        expire_ts: None,
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key4, &F::encode_raw_value_owned(value4))
        .unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();

    db.compact_range_cf_opt(handle, &CompactOptions::new(), None, None);
    assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
}

#[test]
fn test_ttl_snapshot() {
    test_kv_format_impl!(test_ttl_snapshot_impl<ApiV1Ttl ApiV2>);
}

fn test_ttl_snapshot_impl<F: KvFormat>() {
    fail::cfg("ttl_current_ts", "return(100)").unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let mut engine = TestEngineBuilder::new()
        .path(dir.path())
        .api_version(F::TAG)
        .build()
        .unwrap();
    let kvdb = engine.get_rocksdb();

    let key1 = b"r\0key1";
    let value1 = RawValue {
        user_value: b"value1".to_vec(),
        expire_ts: Some(90),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key1, &F::encode_raw_value_owned(value1))
        .unwrap();
    let value10 = RawValue {
        user_value: b"value1".to_vec(),
        expire_ts: Some(110),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key1, &F::encode_raw_value_owned(value10))
        .unwrap();

    let key2 = b"r\0key2";
    let value2 = RawValue {
        user_value: b"value2".to_vec(),
        expire_ts: Some(90),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key2, &F::encode_raw_value_owned(value2))
        .unwrap();
    let value20 = RawValue {
        user_value: b"value2".to_vec(),
        expire_ts: Some(90),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key2, &F::encode_raw_value_owned(value20))
        .unwrap();

    let key3 = b"r\0key3";
    let value3 = RawValue {
        user_value: b"value3".to_vec(),
        expire_ts: None,
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key3, &F::encode_raw_value_owned(value3))
        .unwrap();

    let snapshot = engine.snapshot(SnapContext::default()).unwrap();
    let ttl_snapshot = RawEncodeSnapshot::<_, F>::from_snapshot(snapshot);
    assert_eq!(
        ttl_snapshot
            .get(&Key::from_encoded_slice(b"r\0key1"))
            .unwrap(),
        Some(b"value1".to_vec())
    );
    assert_eq!(
        ttl_snapshot
            .get(&Key::from_encoded_slice(b"r\0key2"))
            .unwrap(),
        None
    );
    assert_eq!(
        ttl_snapshot
            .get(&Key::from_encoded_slice(b"r\0key3"))
            .unwrap(),
        Some(b"value3".to_vec())
    );
    let mut stats = Statistics::default();
    assert_eq!(
        ttl_snapshot
            .get_key_ttl_cf(CF_DEFAULT, &Key::from_encoded_slice(b"r\0key1"), &mut stats)
            .unwrap(),
        Some(10)
    );
    assert_eq!(
        ttl_snapshot
            .get_key_ttl_cf(CF_DEFAULT, &Key::from_encoded_slice(b"r\0key2"), &mut stats)
            .unwrap(),
        None
    );
    assert_eq!(
        ttl_snapshot
            .get_key_ttl_cf(CF_DEFAULT, &Key::from_encoded_slice(b"r\0key3"), &mut stats)
            .unwrap(),
        Some(0)
    );
}

#[test]
fn test_ttl_iterator() {
    test_kv_format_impl!(test_ttl_iterator_impl<ApiV1Ttl ApiV2>);
}

fn test_ttl_iterator_impl<F: KvFormat>() {
    fail::cfg("ttl_current_ts", "return(100)").unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let mut engine = TestEngineBuilder::new()
        .path(dir.path())
        .api_version(F::TAG)
        .build()
        .unwrap();
    let kvdb = engine.get_rocksdb();

    let key1 = b"r\0key1";
    let value1 = RawValue {
        user_value: b"value1".to_vec(),
        expire_ts: Some(90),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key1, &F::encode_raw_value_owned(value1))
        .unwrap();
    let value10 = RawValue {
        user_value: b"value1".to_vec(),
        expire_ts: Some(110),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key1, &F::encode_raw_value_owned(value10))
        .unwrap();

    let key2 = b"r\0key2";
    let value2 = RawValue {
        user_value: b"value2".to_vec(),
        expire_ts: Some(110),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key2, &F::encode_raw_value_owned(value2))
        .unwrap();
    let value20 = RawValue {
        user_value: b"value2".to_vec(),
        expire_ts: Some(90),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key2, &F::encode_raw_value_owned(value20))
        .unwrap();

    let key3 = b"r\0key3";
    let value3 = RawValue {
        user_value: b"value3".to_vec(),
        expire_ts: None,
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key3, &F::encode_raw_value_owned(value3))
        .unwrap();

    let key4 = b"r\0key4";
    let value4 = RawValue {
        user_value: b"value4".to_vec(),
        expire_ts: Some(10),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key4, &F::encode_raw_value_owned(value4))
        .unwrap();

    let key5 = b"r\0key5";
    let value5 = RawValue {
        user_value: b"value5".to_vec(),
        expire_ts: None,
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key5, &F::encode_raw_value_owned(value5))
        .unwrap();
    let value50 = RawValue {
        user_value: b"value5".to_vec(),
        expire_ts: Some(90),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key5, &F::encode_raw_value_owned(value50))
        .unwrap();

    let snapshot = engine.snapshot(SnapContext::default()).unwrap();
    let ttl_snapshot = RawEncodeSnapshot::<_, F>::from_snapshot(snapshot);
    let mut iter = ttl_snapshot
        .iter(CF_DEFAULT, IterOptions::new(None, None, false))
        .unwrap();
    iter.seek_to_first().unwrap();
    assert_eq!(iter.key(), b"r\0key1");
    assert_eq!(iter.value(), b"value1");
    assert_eq!(iter.next().unwrap(), true);
    assert_eq!(iter.key(), b"r\0key3");
    assert_eq!(iter.value(), b"value3");
    assert_eq!(iter.next().unwrap(), false);

    iter.seek_to_last().unwrap();
    assert_eq!(iter.key(), b"r\0key3");
    assert_eq!(iter.value(), b"value3");
    assert_eq!(iter.prev().unwrap(), true);
    assert_eq!(iter.key(), b"r\0key1");
    assert_eq!(iter.value(), b"value1");
    assert_eq!(iter.prev().unwrap(), false);

    iter.seek(&Key::from_encoded_slice(b"r\0key2")).unwrap();
    assert_eq!(iter.valid().unwrap(), true);
    assert_eq!(iter.key(), b"r\0key3");
    assert_eq!(iter.value(), b"value3");
    iter.seek(&Key::from_encoded_slice(b"r\0key4")).unwrap();
    assert_eq!(iter.valid().unwrap(), false);

    iter.seek_for_prev(&Key::from_encoded_slice(b"r\0key2"))
        .unwrap();
    assert_eq!(iter.valid().unwrap(), true);
    assert_eq!(iter.key(), b"r\0key1");
    assert_eq!(iter.value(), b"value1");
    iter.seek_for_prev(&Key::from_encoded_slice(b"r\0key1"))
        .unwrap();
    assert_eq!(iter.valid().unwrap(), true);
    assert_eq!(iter.key(), b"r\0key1");
    assert_eq!(iter.value(), b"value1");
}

#[test]
fn test_stoarge_raw_batch_put_ttl() {
    test_kv_format_impl!(test_stoarge_raw_batch_put_ttl_impl<ApiV1Ttl ApiV2>);
}

fn test_stoarge_raw_batch_put_ttl_impl<F: KvFormat>() {
    fail::cfg("ttl_current_ts", "return(100)").unwrap();

    let storage = TestStorageBuilder::<_, _, F>::new(MockLockManager::new())
        .build()
        .unwrap();
    let (tx, rx) = channel();
    let ctx = Context {
        api_version: F::CLIENT_TAG,
        ..Default::default()
    };

    let test_data = vec![
        (b"r\0a".to_vec(), b"aa".to_vec(), 10),
        (b"r\0b".to_vec(), b"bb".to_vec(), 20),
        (b"r\0c".to_vec(), b"cc".to_vec(), 30),
        (b"r\0d".to_vec(), b"dd".to_vec(), 0),
        (b"r\0e".to_vec(), b"ee".to_vec(), 40),
    ];

    let kvpairs = test_data
        .clone()
        .into_iter()
        .map(|(key, value, _)| (key, value))
        .collect();
    let ttls = test_data
        .clone()
        .into_iter()
        .map(|(_, _, ttl)| ttl)
        .collect();
    // Write key-value pairs in a batch
    storage
        .raw_batch_put(
            ctx.clone(),
            "".to_string(),
            kvpairs,
            ttls,
            expect_ok_callback(tx, 0),
        )
        .unwrap();
    rx.recv().unwrap();

    // Verify pairs one by one
    for (key, val, _) in &test_data {
        expect_value(
            val.to_vec(),
            block_on(storage.raw_get(ctx.clone(), "".to_string(), key.to_vec())).unwrap(),
        );
    }
    // Verify ttl one by one
    for (key, _, ttl) in test_data {
        let res = block_on(storage.raw_get_key_ttl(ctx.clone(), "".to_string(), key)).unwrap();
        assert_eq!(res, Some(ttl));
    }
}

fn make_raw_key<F: KvFormat>(key: &[u8], ts: Option<u64>) -> Vec<u8> {
    let encode_key = F::encode_raw_key(key, ts.map(Into::into));
    let res = keys::data_key(encode_key.as_encoded());
    res
}

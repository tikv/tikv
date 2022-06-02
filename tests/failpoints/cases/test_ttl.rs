// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::channel;

use api_version::{test_kv_format_impl, ApiV1Ttl, KvFormat, RawValue};
use engine_rocks::{raw::CompactOptions, util::get_cf_handle};
use engine_traits::{IterOptions, MiscExt, Peekable, SyncMutable, CF_DEFAULT};
use futures::executor::block_on;
use kvproto::kvrpcpb::Context;
use tikv::{
    config::DbConfig,
    server::ttl::check_ttl_and_compact_files,
    storage::{
        kv::{SnapContext, TestEngineBuilder},
        lock_manager::DummyLockManager,
        raw::encoded::RawEncodeSnapshot,
        test_util::{expect_ok_callback, expect_value},
        Engine, Iterator, Snapshot, Statistics, TestStorageBuilder,
    },
};
use txn_types::Key;

#[test]
fn test_ttl_checker() {
    test_ttl_checker_impl::<ApiV1Ttl>();
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
    let key1 = b"zr\0key1";
    let value1 = RawValue {
        user_value: vec![0; 10],
        expire_ts: Some(10),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key1, &F::encode_raw_value_owned(value1))
        .unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();
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
    let key5 = b"zr\0key5";
    let value5 = RawValue {
        user_value: vec![0; 10],
        expire_ts: Some(10),
        is_delete: false,
    };
    kvdb.put_cf(CF_DEFAULT, key5, &F::encode_raw_value_owned(value5))
        .unwrap();
    kvdb.flush_cf(CF_DEFAULT, true).unwrap();

    assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_some());

    let _ = check_ttl_and_compact_files(&kvdb, b"zr\0key1", b"zr\0key25", false);
    assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_some());

    let _ = check_ttl_and_compact_files(&kvdb, b"zr\0key2", b"zr\0key6", false);
    assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
    assert!(kvdb.get_value_cf(CF_DEFAULT, key5).unwrap().is_none());
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
    let engine = TestEngineBuilder::new()
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
    let engine = TestEngineBuilder::new()
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
        .iter(IterOptions::new(None, None, false))
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

    let storage = TestStorageBuilder::<_, _, F>::new(DummyLockManager)
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

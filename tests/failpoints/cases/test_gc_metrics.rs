// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::AtomicU64, mpsc, Arc},
    thread,
    time::Duration,
};

use api_version::{ApiV2, KvFormat, RawValue};
use engine_rocks::{raw::FlushOptions, util::get_cf_handle, RocksEngine};
use engine_traits::{CF_DEFAULT, CF_WRITE};
use keys::DATA_PREFIX_KEY;
use keyspace_meta::KeyspaceLevelGCService;
use kvproto::{
    kvrpcpb::*,
    metapb::{Peer, Region},
};
use pd_client::FeatureGate;
use raft::StateRole;
use raftstore::{
    coprocessor::{
        region_info_accessor::MockRegionInfoProvider, CoprocessorHost, RegionChangeEvent,
    },
    RegionInfoAccessor,
};
use tikv::{
    config::DbConfig,
    server::gc_worker::{
        compaction_filter::{
            GC_COMPACTION_FILTERED, GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED,
            GC_COMPACTION_FILTER_MVCC_DELETION_MET, GC_COMPACTION_FILTER_PERFORM,
            GC_COMPACTION_FILTER_SKIP,
        },
        make_combined_key, make_keypsace_txnkv_mvcc_key_no_ts, make_keyspace_level_gc_service,
        rawkv_compaction_filter::make_key,
        AutoGcConfig, GcConfig, GcWorker, MockSafePointProvider, PrefixedEngine, TestGcRunner,
        STAT_RAW_KEYMODE, STAT_TXN_KEYMODE,
    },
    storage::{
        kv::{Modify, TestEngineBuilder, WriteData},
        mvcc::{tests::must_get, MVCC_VERSIONS_HISTOGRAM},
        txn::tests::{must_commit, must_prewrite_delete, must_prewrite_put},
        Engine,
    },
};
use txn_types::{Key, TimeStamp};

use crate::cases::test_table_properties::do_write;

#[test]
fn test_txn_create_compaction_filter() {
    GC_COMPACTION_FILTER_PERFORM.reset();
    GC_COMPACTION_FILTER_SKIP.reset();

    let mut cfg = DbConfig::default();
    cfg.writecf.disable_auto_compactions = true;
    cfg.writecf.dynamic_level_bytes = false;
    let dir = tempfile::TempDir::new().unwrap();
    let builder = TestEngineBuilder::new().path(dir.path());
    let mut engine = builder.build_with_cfg(&cfg).unwrap();
    let raw_engine = engine.get_rocksdb();

    let mut gc_runner = TestGcRunner::new(0);
    let value = vec![b'v'; 512];

    must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", 100);
    must_commit(&mut engine, b"zkey", 100, 110);

    gc_runner
        .safe_point(TimeStamp::new(1).into_inner())
        .gc(&raw_engine);
    assert_eq!(
        GC_COMPACTION_FILTER_PERFORM
            .with_label_values(&[STAT_TXN_KEYMODE])
            .get(),
        1
    );
    assert_eq!(
        GC_COMPACTION_FILTER_SKIP
            .with_label_values(&[STAT_TXN_KEYMODE])
            .get(),
        1
    );

    GC_COMPACTION_FILTER_PERFORM.reset();
    GC_COMPACTION_FILTER_SKIP.reset();
}

#[test]
fn test_txn_mvcc_filtered() {
    MVCC_VERSIONS_HISTOGRAM.reset();
    GC_COMPACTION_FILTERED.reset();

    let mut engine = TestEngineBuilder::new().build().unwrap();
    let raw_engine = engine.get_rocksdb();
    let value = vec![b'v'; 512];
    let mut gc_runner = TestGcRunner::new(0);

    // GC can't delete keys after the given safe point.
    must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", 100);
    must_commit(&mut engine, b"zkey", 100, 110);
    gc_runner.safe_point(50).gc(&raw_engine);
    must_get(&mut engine, b"zkey", 110, &value);

    // GC can't delete keys before the safe ponit if they are latest versions.
    gc_runner.safe_point(200).gc(&raw_engine);
    must_get(&mut engine, b"zkey", 110, &value);

    must_prewrite_put(&mut engine, b"zkey", &value, b"zkey", 120);
    must_commit(&mut engine, b"zkey", 120, 130);

    // GC can't delete the latest version before the safe ponit.
    gc_runner.safe_point(115).gc(&raw_engine);
    must_get(&mut engine, b"zkey", 110, &value);

    // GC a version will also delete the key on default CF.
    gc_runner.safe_point(200).gc(&raw_engine);
    assert_eq!(
        MVCC_VERSIONS_HISTOGRAM
            .with_label_values(&[STAT_TXN_KEYMODE])
            .get_sample_sum(),
        4_f64
    );
    assert_eq!(
        GC_COMPACTION_FILTERED
            .with_label_values(&[STAT_TXN_KEYMODE])
            .get(),
        1
    );

    MVCC_VERSIONS_HISTOGRAM.reset();
    GC_COMPACTION_FILTERED.reset();
}

#[test]
fn test_txn_mvcc_filtered_with_keyspace_level_gc() {
    let combined_vec = Vec::from(DATA_PREFIX_KEY);
    let user_key = b"key";
    // b"zkey"
    let api_v1_mvcc_key = make_combined_key(combined_vec, user_key.to_vec());
    test_txn_mvcc_filtered_by_keyspace(None, api_v1_mvcc_key);
    let keyspace_txnkv_mvcc_key = make_keypsace_txnkv_mvcc_key_no_ts(1, user_key.to_vec());
    test_txn_mvcc_filtered_by_keyspace(Some(1), keyspace_txnkv_mvcc_key);
}
fn test_txn_mvcc_filtered_by_keyspace(keyspace_id: Option<u32>, key: Vec<u8>) {
    MVCC_VERSIONS_HISTOGRAM.reset();
    GC_COMPACTION_FILTERED.reset();

    let mut engine = TestEngineBuilder::new().build().unwrap();
    let raw_engine = engine.get_rocksdb();
    let value = vec![b'v'; 512];
    let mut gc_runner = TestGcRunner::new(0);
    gc_runner.keyspace_level_gc_service = make_keyspace_level_gc_service().clone();

    // GC can't delete keys after the given safe point.
    must_prewrite_put(&mut engine, key.as_slice(), &value, key.as_slice(), 100);
    must_commit(&mut engine, key.as_slice(), 100, 110);

    gc_runner
        .update_gc_safe_point(keyspace_id, 50)
        .gc(&raw_engine);
    must_get(&mut engine, key.as_slice(), 110, &value);

    // GC can't delete keys before the safe ponit if they are latest versions.
    gc_runner
        .update_gc_safe_point(keyspace_id, 200)
        .gc(&raw_engine);
    must_get(&mut engine, key.as_slice(), 110, &value);

    must_prewrite_put(&mut engine, key.as_slice(), &value, key.as_slice(), 120);
    must_commit(&mut engine, key.as_slice(), 120, 130);

    // GC can't delete the latest version before the safe ponit.
    gc_runner
        .update_gc_safe_point(keyspace_id, 115)
        .gc(&raw_engine);
    must_get(&mut engine, key.as_slice(), 110, &value);

    // GC a version will also delete the key on default CF.
    gc_runner
        .update_gc_safe_point(keyspace_id, 200)
        .gc(&raw_engine);
    assert_eq!(
        MVCC_VERSIONS_HISTOGRAM
            .with_label_values(&[STAT_TXN_KEYMODE])
            .get_sample_sum(),
        4_f64
    );
    assert_eq!(
        GC_COMPACTION_FILTERED
            .with_label_values(&[STAT_TXN_KEYMODE])
            .get(),
        1
    );
    MVCC_VERSIONS_HISTOGRAM.reset();
    GC_COMPACTION_FILTERED.reset();
}

#[test]
fn test_txn_gc_keys_handled() {
    let store_id = 1;
    GC_COMPACTION_FILTER_MVCC_DELETION_MET.reset();
    GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED.reset();

    let engine = TestEngineBuilder::new().build().unwrap();
    let mut prefixed_engine = PrefixedEngine(engine.clone());

    let (tx, _rx) = mpsc::channel();
    let feature_gate = FeatureGate::default();
    feature_gate.set_version("5.0.0").unwrap();
    let mut gc_worker = GcWorker::new(
        prefixed_engine.clone(),
        tx,
        GcConfig::default(),
        feature_gate,
        Arc::new(MockRegionInfoProvider::new(vec![])),
        Arc::new(Some(KeyspaceLevelGCService::default())),
    );
    gc_worker.start(store_id).unwrap();

    let mut r1 = Region::default();
    r1.set_id(1);
    r1.mut_region_epoch().set_version(1);
    r1.set_start_key(b"".to_vec());
    r1.set_end_key(b"".to_vec());
    r1.mut_peers().push(Peer::default());
    r1.mut_peers()[0].set_store_id(store_id);

    let sp_provider = MockSafePointProvider(200);
    let mut host = CoprocessorHost::<RocksEngine>::default();
    let ri_provider = RegionInfoAccessor::new(&mut host, Arc::new(|| false));
    let auto_gc_cfg = AutoGcConfig::new(sp_provider, ri_provider, 1);
    let safe_point = Arc::new(AtomicU64::new(500));

    gc_worker.start_auto_gc(auto_gc_cfg, safe_point).unwrap();
    host.on_region_changed(&r1, RegionChangeEvent::Create, StateRole::Leader);

    let db = engine.kv_engine().unwrap().as_inner().clone();
    let cf = get_cf_handle(&db, CF_WRITE).unwrap();

    for i in 0..3 {
        let k = format!("k{:02}", i).into_bytes();
        must_prewrite_put(&mut prefixed_engine, &k, b"value", &k, 101);
        must_commit(&mut prefixed_engine, &k, 101, 102);
        must_prewrite_delete(&mut prefixed_engine, &k, &k, 151);
        must_commit(&mut prefixed_engine, &k, 151, 152);
    }

    let mut fopts = FlushOptions::default();
    fopts.set_wait(true);
    db.flush_cf(cf, &fopts).unwrap();

    db.compact_range_cf(cf, None, None);

    // This compaction can schedule gc task
    db.compact_range_cf(cf, None, None);
    thread::sleep(Duration::from_millis(100));

    assert_eq!(
        GC_COMPACTION_FILTER_MVCC_DELETION_MET
            .with_label_values(&[STAT_TXN_KEYMODE])
            .get(),
        6
    );

    assert_eq!(
        GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED
            .with_label_values(&[STAT_TXN_KEYMODE])
            .get(),
        3
    );

    GC_COMPACTION_FILTER_MVCC_DELETION_MET.reset();
    GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED.reset();
}

#[test]
fn test_raw_mvcc_filtered() {
    MVCC_VERSIONS_HISTOGRAM.reset();
    GC_COMPACTION_FILTERED.reset();

    let mut cfg = DbConfig::default();
    cfg.defaultcf.disable_auto_compactions = true;
    cfg.defaultcf.dynamic_level_bytes = false;

    let engine = TestEngineBuilder::new()
        .api_version(ApiVersion::V2)
        .build_with_cfg(&cfg)
        .unwrap();
    let raw_engine = engine.get_rocksdb();
    let mut gc_runner = TestGcRunner::new(0);

    let user_key = b"r\0aaaaaaaaaaa";

    let test_raws = vec![
        (user_key, 100, false),
        (user_key, 90, false),
        (user_key, 70, false),
    ];

    let modifies = test_raws
        .into_iter()
        .map(|(key, ts, is_delete)| {
            (
                make_key(key, ts),
                ApiV2::encode_raw_value(RawValue {
                    user_value: &[0; 10][..],
                    expire_ts: Some(TimeStamp::max().into_inner()),
                    is_delete,
                }),
            )
        })
        .map(|(k, v)| Modify::Put(CF_DEFAULT, Key::from_encoded_slice(k.as_slice()), v))
        .collect();

    let ctx = Context {
        api_version: ApiVersion::V2,
        ..Default::default()
    };
    let batch = WriteData::from_modifies(modifies);

    engine.write(&ctx, batch).unwrap();

    gc_runner.safe_point(80).gc_raw(&raw_engine);

    assert_eq!(
        MVCC_VERSIONS_HISTOGRAM
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get_sample_sum(),
        1_f64
    );
    assert_eq!(
        GC_COMPACTION_FILTERED
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        1
    );

    MVCC_VERSIONS_HISTOGRAM.reset();
    GC_COMPACTION_FILTERED.reset();
}

#[test]
fn test_raw_gc_keys_handled() {
    let store_id = 1;
    GC_COMPACTION_FILTER_MVCC_DELETION_MET.reset();
    GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED.reset();

    let engine = TestEngineBuilder::new()
        .api_version(ApiVersion::V2)
        .build()
        .unwrap();
    let prefixed_engine = PrefixedEngine(engine.clone());

    let (tx, _rx) = mpsc::channel();
    let feature_gate = FeatureGate::default();
    let mut gc_worker = GcWorker::new(
        prefixed_engine,
        tx,
        GcConfig::default(),
        feature_gate,
        Arc::new(MockRegionInfoProvider::new(vec![])),
        Arc::new(None),
    );
    gc_worker.start(store_id).unwrap();

    let mut r1 = Region::default();
    r1.set_id(1);
    r1.mut_region_epoch().set_version(1);
    r1.set_start_key(b"".to_vec());
    r1.set_end_key(b"".to_vec());
    r1.mut_peers().push(Peer::default());
    r1.mut_peers()[0].set_store_id(store_id);

    let sp_provider = MockSafePointProvider(200);
    let mut host = CoprocessorHost::<RocksEngine>::default();
    let ri_provider = RegionInfoAccessor::new(&mut host, Arc::new(|| false));
    let auto_gc_cfg = AutoGcConfig::new(sp_provider, ri_provider, store_id);
    let safe_point = Arc::new(AtomicU64::new(500));

    gc_worker.start_auto_gc(auto_gc_cfg, safe_point).unwrap();
    host.on_region_changed(&r1, RegionChangeEvent::Create, StateRole::Leader);

    let db = engine.kv_engine().unwrap().as_inner().clone();

    let user_key_del = b"r\0aaaaaaaaaaa";

    // If it's deleted, it will call async scheduler GcTask.
    let test_raws = vec![
        (user_key_del, 9, true),
        (user_key_del, 5, false),
        (user_key_del, 1, false),
    ];

    let modifies = test_raws
        .into_iter()
        .map(|(key, ts, is_delete)| {
            (
                make_key(key, ts),
                ApiV2::encode_raw_value(RawValue {
                    user_value: &[0; 10][..],
                    expire_ts: Some(TimeStamp::max().into_inner()),
                    is_delete,
                }),
            )
        })
        .map(|(k, v)| Modify::Put(CF_DEFAULT, Key::from_encoded_slice(k.as_slice()), v))
        .collect();

    let ctx = Context {
        api_version: ApiVersion::V2,
        ..Default::default()
    };

    let batch = WriteData::from_modifies(modifies);

    engine.write(&ctx, batch).unwrap();

    let cf = get_cf_handle(&db, CF_DEFAULT).unwrap();
    let mut fopts = FlushOptions::default();
    fopts.set_wait(true);
    db.flush_cf(cf, &fopts).unwrap();

    db.compact_range_cf(cf, None, None);

    thread::sleep(Duration::from_millis(100));

    assert_eq!(
        GC_COMPACTION_FILTER_MVCC_DELETION_MET
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        1
    );
    assert_eq!(
        GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        1
    );

    GC_COMPACTION_FILTER_MVCC_DELETION_MET.reset();
    GC_COMPACTION_FILTER_MVCC_DELETION_HANDLED.reset();
}

#[test]
fn test_keyspace_level_gc_service() {
    // Make empty cache in keyspace_level_gc_service.
    let keyspace_level_gc_service = Arc::new(Some(KeyspaceLevelGCService::new(
        Arc::clone(&Default::default()),
        Arc::clone(&Default::default()),
    )));

    // Case 1: If there is no keyspace level GC in cache, then
    // is_all_keyspace_level_gc_have_not_initialized return true.
    assert_eq!(true, keyspace_level_gc_service.is_some());
    if let Some(ref keyspace_level_gc_service) = *keyspace_level_gc_service {
        let is_all_keyspace_level_gc_have_not_initialized =
            keyspace_level_gc_service.is_all_keyspace_level_gc_have_not_initialized();
        assert_eq!(true, is_all_keyspace_level_gc_have_not_initialized);
    }

    // Case 2: If there have any keyspace level GC in cache, then
    // is_all_keyspace_level_gc_have_not_initialized return false.
    let keyspace_level_gc_service = make_keyspace_level_gc_service();
    assert_eq!(true, keyspace_level_gc_service.is_some());

    if let Some(ref keyspace_level_gc_service) = *keyspace_level_gc_service {
        let is_all_keyspace_level_gc_have_not_initialized =
            keyspace_level_gc_service.is_all_keyspace_level_gc_have_not_initialized();
        assert_eq!(false, is_all_keyspace_level_gc_have_not_initialized);
    }

    // Case 3: Check get_max_ts_of_all_ks_gc_safe_point will return max(all keyspace
    // level GC safe point).
    if let Some(ref keyspace_level_gc_service) = *keyspace_level_gc_service {
        let max_ts_of_all_ks_gc_safe_point =
            keyspace_level_gc_service.get_max_ts_of_all_ks_gc_safe_point();
        assert_eq!(69, max_ts_of_all_ks_gc_safe_point);
    }
}

#[test]
fn test_check_skip_gc_with_ks_level_gc() {
    test_check_skip_gc_with_ks_level_gc_by_kv_mode(true);
    test_check_skip_gc_with_ks_level_gc_by_kv_mode(false);
}
fn test_check_skip_gc_with_ks_level_gc_by_kv_mode(is_rawkv: bool) {
    GC_COMPACTION_FILTER_PERFORM.reset();
    GC_COMPACTION_FILTER_SKIP.reset();

    let mut cfg = DbConfig::default();
    cfg.defaultcf.disable_auto_compactions = true;
    cfg.defaultcf.dynamic_level_bytes = false;
    let dir = tempfile::TempDir::new().unwrap();
    let builder = TestEngineBuilder::new().path(dir.path());
    let engine = builder
        .api_version(ApiVersion::V2)
        .build_with_cfg(&cfg)
        .unwrap();
    let raw_engine = engine.get_rocksdb();

    let mut gc_runner = TestGcRunner::new(0);
    gc_runner.keyspace_level_gc_service = make_keyspace_level_gc_service().clone();

    do_write(&engine, false, 5, is_rawkv);

    let mut metrics_label = STAT_TXN_KEYMODE;
    if is_rawkv {
        metrics_label = STAT_RAW_KEYMODE;
    }

    // It Hasn't called gc_runner yet, check the initial metrics value is 0.
    assert_eq!(
        GC_COMPACTION_FILTER_PERFORM
            .with_label_values(&[metrics_label])
            .get(),
        0
    );
    // If check_need_gc return false, it will skip GC, GC_COMPACTION_FILTER_SKIP
    // will not 0.
    assert_eq!(
        GC_COMPACTION_FILTER_SKIP
            .with_label_values(&[metrics_label])
            .get(),
        0
    );

    // Call GC runner as global GC safe point is 0.
    if is_rawkv {
        gc_runner.safe_point(0).gc_raw(&raw_engine);
    } else {
        gc_runner.safe_point(0).gc(&raw_engine);
    }

    // Although global GC safe point is 0, but keyspace_id(1) GC safe point=60,
    // so is_all_ks_not_init_gc_sp is == false,
    // then GC_COMPACTION_FILTER_PERFORM + 1.
    assert_eq!(
        GC_COMPACTION_FILTER_PERFORM
            .with_label_values(&[metrics_label])
            .get(),
        1
    );
    // Global GC safe point = 0, keyspace_id(1) GC safe point=60
    // then check_need_gc return false, so GC_COMPACTION_FILTER_SKIP + 1.
    assert_eq!(
        GC_COMPACTION_FILTER_SKIP
            .with_label_values(&[metrics_label])
            .get(),
        1
    );
}

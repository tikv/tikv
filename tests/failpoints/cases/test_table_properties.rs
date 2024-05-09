// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use api_version::{ApiV2, KvFormat, RawValue};
use dashmap::DashMap;
use engine_rocks::RocksEngine;
use engine_traits::{MiscExt, CF_DEFAULT, CF_WRITE};
use keyspace_meta::KeyspaceLevelGCService;
use kvproto::{
    keyspacepb,
    kvrpcpb::{Context, *},
};
use tempfile::TempDir;
use tikv::{
    config::DbConfig,
    server::gc_worker::{
        compaction_filter::{
            test_utils::rocksdb_level_files, GC_COMPACTION_FILTER_PERFORM,
            GC_COMPACTION_FILTER_SKIP,
        },
        TestGcRunner, STAT_RAW_KEYMODE, STAT_TXN_KEYMODE,
    },
    storage::{
        kv::{Modify, TestEngineBuilder, WriteData},
        Engine,
    },
};
use txn_types::{Key, TimeStamp};

pub fn make_key(key: &[u8], ts: u64) -> Vec<u8> {
    let encode_key = ApiV2::encode_raw_key(key, Some(ts.into()));
    let res = keys::data_key(encode_key.as_encoded());
    res
}

#[test]
fn test_check_need_gc() {
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

    do_write(&engine, false, 5, true);

    // Check init value
    assert_eq!(
        GC_COMPACTION_FILTER_PERFORM
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        0
    );
    assert_eq!(
        GC_COMPACTION_FILTER_SKIP
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        0
    );

    // TEST 1: If ratio_threshold < 1.0 || context.is_bottommost_level() is true,
    // check_need_gc return true, call dofilter
    gc_runner
        .safe_point(TimeStamp::max().into_inner())
        .gc_raw(&raw_engine);

    assert_eq!(
        GC_COMPACTION_FILTER_PERFORM
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        1
    );
    assert_eq!(
        GC_COMPACTION_FILTER_SKIP
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        0
    );

    // TEST 2: props.num_versions as f64 > props.num_rows as f64 * ratio_threshold
    // return true.
    do_write(&engine, false, 5, true);
    engine.get_rocksdb().flush_cfs(&[], true).unwrap();

    do_gc(&raw_engine, 2, &mut gc_runner, &dir);

    do_write(&engine, false, 5, true);
    engine.get_rocksdb().flush_cfs(&[], true).unwrap();

    // Set ratio_threshold, let (props.num_versions as f64 > props.num_rows as
    // f64 * ratio_threshold) return true
    gc_runner.ratio_threshold = Option::Some(0.0f64);

    // is_bottommost_level = false
    do_gc(&raw_engine, 1, &mut gc_runner, &dir);

    assert_eq!(
        GC_COMPACTION_FILTER_PERFORM
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        3
    );
    assert_eq!(
        GC_COMPACTION_FILTER_SKIP
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        0
    );
}

fn make_keyspace_level_gc_service() -> Arc<Option<KeyspaceLevelGCService>> {
    // Init keyspace level gc cache.
    let ks_gc_map = DashMap::new();
    // make data ts < props.min_ts
    ks_gc_map.insert(1_u32, 60_u64);
    ks_gc_map.insert(2_u32, 69_u64);

    let keyspace_level_gc_cache = Arc::new(ks_gc_map);

    // Init keyspace meta cache.
    let keyspace_id_meta_map = DashMap::new();

    let mut keyspace_config = HashMap::new();
    keyspace_config.insert(
        keyspace_meta::KEYSPACE_CONFIG_KEY_GC_MGMT_TYPE.to_string(),
        keyspace_meta::GC_MGMT_TYPE_KEYSPACE_LEVEL_GC.to_string(),
    );
    let keyspace_1_meta = keyspacepb::KeyspaceMeta {
        id: 1,
        name: "test_keyspace".to_string(),
        state: Default::default(),
        created_at: 0,
        state_changed_at: 0,
        config: keyspace_config.clone(),
        unknown_fields: Default::default(),
        cached_size: Default::default(),
    };

    let keyspace_2_meta = keyspacepb::KeyspaceMeta {
        id: 2,
        name: "test_keyspace".to_string(),
        state: Default::default(),
        created_at: 0,
        state_changed_at: 0,
        config: keyspace_config,
        unknown_fields: Default::default(),
        cached_size: Default::default(),
    };

    // Make data ts < props.min_ts( props.min_ts = 70).
    keyspace_id_meta_map.insert(1_u32, keyspace_1_meta);
    keyspace_id_meta_map.insert(2_u32, keyspace_2_meta);

    let keyspace_id_meta_cache = Arc::new(keyspace_id_meta_map);

    Arc::new(Some(KeyspaceLevelGCService::new(
        Arc::clone(&keyspace_level_gc_cache),
        Arc::clone(&keyspace_id_meta_cache),
    )))
}

#[test]
fn test_keyspace_level_gc_service() {
    // Make empty cache in keyspace_level_gc_service.
    let keyspace_level_gc_service = Arc::new(Some(KeyspaceLevelGCService::new(
        Arc::clone(&Default::default()),
        Arc::clone(&Default::default()),
    )));

    // Case 1: If there is no keyspace level gc in cache, then
    // is_all_keyspace_level_gc_have_not_initialized return true.
    assert_eq!(true, keyspace_level_gc_service.is_some());
    if let Some(ref ks_meta_service) = *keyspace_level_gc_service {
        let is_all_keyspace_level_gc_have_not_initialized =
            ks_meta_service.is_all_keyspace_level_gc_have_not_initialized();
        assert_eq!(true, is_all_keyspace_level_gc_have_not_initialized);
    }

    // Case 2: If there have any keyspace level gc in cache, then
    // is_all_keyspace_level_gc_have_not_initialized return false.
    let keyspace_level_gc_service = make_keyspace_level_gc_service();
    assert_eq!(true, keyspace_level_gc_service.is_some());

    if let Some(ref ks_meta_service) = *keyspace_level_gc_service {
        let is_all_keyspace_level_gc_have_not_initialized =
            ks_meta_service.is_all_keyspace_level_gc_have_not_initialized();
        assert_eq!(false, is_all_keyspace_level_gc_have_not_initialized);
    }

    // Case 3: Check get_max_ts_of_all_ks_gc_safe_point will return max(all keyspace
    // level gc safe point).
    if let Some(ref ks_meta_service) = *keyspace_level_gc_service {
        let max_ts_of_all_ks_gc_safe_point = ks_meta_service.get_max_ts_of_all_ks_gc_safe_point();
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

fn do_write<E: Engine>(engine: &E, is_delete: bool, op_nums: u64, is_rawkv: bool) {
    let mut data_prefix = api_version::api_v2::TIDB_TABLE_KEY_PREFIX;
    if is_rawkv {
        data_prefix = api_version::api_v2::RAW_KEY_PREFIX;
    }
    let mut test_cf = CF_WRITE;
    if is_rawkv {
        test_cf = CF_DEFAULT;
    }

    // make data as keyspace id = 1.
    let user_key = vec![data_prefix, 0, 0, 1, 1, 2, 3];

    let mut test_raws = vec![];
    let start_mvcc = 70;

    let mut i = 0;
    while i < op_nums {
        test_raws.push((user_key.as_slice(), start_mvcc + i, is_delete));
        i += 1;
    }

    let modifies = test_raws
        .into_iter()
        .map(|(key, ts, is_delete)| {
            (
                make_key(key, ts),
                ApiV2::encode_raw_value(RawValue {
                    user_value: &[0; 1024][..],
                    expire_ts: Some(TimeStamp::max().into_inner()),
                    is_delete,
                }),
            )
        })
        .map(|(k, v)| Modify::Put(test_cf, Key::from_encoded_slice(k.as_slice()), v))
        .collect();

    let ctx = Context {
        api_version: ApiVersion::V2,
        ..Default::default()
    };

    let batch = WriteData::from_modifies(modifies);
    engine.write(&ctx, batch).unwrap();
}

fn do_gc(
    raw_engine: &RocksEngine,
    target_level: usize,
    gc_runner: &mut TestGcRunner<'_>,
    dir: &TempDir,
) {
    let gc_safepoint = TimeStamp::max().into_inner();
    let level_files = rocksdb_level_files(raw_engine, CF_DEFAULT);
    let l0_file = dir.path().join(&level_files[0][0]);
    let files = &[l0_file.to_str().unwrap().to_owned()];
    gc_runner.target_level = Some(target_level);
    gc_runner
        .safe_point(gc_safepoint)
        .gc_on_files(raw_engine, files, CF_DEFAULT);
}

#[test]
fn test_skip_gc_by_check() {
    GC_COMPACTION_FILTER_PERFORM.reset();
    GC_COMPACTION_FILTER_SKIP.reset();

    let mut cfg = DbConfig::default();
    cfg.defaultcf.disable_auto_compactions = true;
    cfg.defaultcf.dynamic_level_bytes = false;
    cfg.defaultcf.num_levels = 7;
    let dir = tempfile::TempDir::new().unwrap();
    let builder = TestEngineBuilder::new().path(dir.path());
    let engine = builder
        .api_version(ApiVersion::V2)
        .build_with_cfg(&cfg)
        .unwrap();
    let raw_engine = engine.get_rocksdb();
    let mut gc_runner = TestGcRunner::new(0);

    do_write(&engine, false, 5, true);
    engine.get_rocksdb().flush_cfs(&[], true).unwrap();

    // The min_mvcc_ts ts > gc safepoint, check_need_gc return false, don't call
    // dofilter
    gc_runner
        .safe_point(TimeStamp::new(1).into_inner())
        .gc_raw(&raw_engine);
    assert_eq!(
        GC_COMPACTION_FILTER_PERFORM
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        1
    );
    assert_eq!(
        GC_COMPACTION_FILTER_SKIP
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        1
    );

    // TEST 2:When is_bottommost_level = false,
    // write data to level2
    do_write(&engine, false, 5, true);
    engine.get_rocksdb().flush_cfs(&[], true).unwrap();

    do_gc(&raw_engine, 2, &mut gc_runner, &dir);

    do_write(&engine, false, 5, true);
    engine.get_rocksdb().flush_cfs(&[], true).unwrap();

    // Set ratio_threshold, let (props.num_versions as f64 > props.num_rows as
    // f64 * ratio_threshold) return false
    gc_runner.ratio_threshold = Option::Some(f64::MAX);

    // is_bottommost_level = false
    do_gc(&raw_engine, 1, &mut gc_runner, &dir);

    assert_eq!(
        GC_COMPACTION_FILTER_PERFORM
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        3
    );

    // The check_need_gc return false, GC_COMPACTION_FILTER_SKIP will add 1.
    assert_eq!(
        GC_COMPACTION_FILTER_SKIP
            .with_label_values(&[STAT_RAW_KEYMODE])
            .get(),
        2
    );
}

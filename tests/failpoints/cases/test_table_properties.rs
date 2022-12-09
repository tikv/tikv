// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::{ApiV2, KvFormat, RawValue};
use engine_rocks::RocksEngine;
use engine_traits::{MiscExt, CF_DEFAULT};
use kvproto::kvrpcpb::{Context, *};
use tempfile::TempDir;
use tikv::{
    config::DbConfig,
    server::gc_worker::{
        compaction_filter::{
            test_utils::rocksdb_level_files, GC_COMPACTION_FILTER_PERFORM,
            GC_COMPACTION_FILTER_SKIP,
        },
        TestGcRunner, STAT_RAW_KEYMODE,
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

    do_write(&engine, false, 5);

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
    do_write(&engine, false, 5);
    engine.get_rocksdb().flush_cfs(true).unwrap();

    do_gc(&raw_engine, 2, &mut gc_runner, &dir);

    do_write(&engine, false, 5);
    engine.get_rocksdb().flush_cfs(true).unwrap();

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

fn do_write<E: Engine>(engine: &E, is_delete: bool, op_nums: u64) {
    make_data(engine, is_delete, op_nums);
}

fn make_data<E: Engine>(engine: &E, is_delete: bool, op_nums: u64) {
    let user_key = b"r\0aaaaaaaaaaa";

    let mut test_raws = vec![];
    let start_mvcc = 70;

    let mut i = 0;
    while i < op_nums {
        test_raws.push((user_key, start_mvcc + i, is_delete));
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
        .map(|(k, v)| Modify::Put(CF_DEFAULT, Key::from_encoded_slice(k.as_slice()), v))
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

    do_write(&engine, false, 5);
    engine.get_rocksdb().flush_cfs(true).unwrap();

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
    do_write(&engine, false, 5);
    engine.get_rocksdb().flush_cfs(true).unwrap();

    do_gc(&raw_engine, 2, &mut gc_runner, &dir);

    do_write(&engine, false, 5);
    engine.get_rocksdb().flush_cfs(true).unwrap();

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

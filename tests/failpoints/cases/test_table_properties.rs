// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::{ApiV2, KvFormat, RawValue};
use engine_traits::CF_DEFAULT;
use kvproto::kvrpcpb::{Context, *};
use tikv::{
    config::DbConfig,
    server::gc_worker::{
        compaction_filter::{GC_COMPACTION_FILTER_PERFORM, GC_COMPACTION_FILTER_SKIP},
        TestGCRunner,
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
    let mut cfg = DbConfig::default();
    cfg.defaultcf.disable_auto_compactions = true;
    cfg.defaultcf.dynamic_level_bytes = false;

    let engine = TestEngineBuilder::new()
        .api_version(ApiVersion::V2)
        .build_with_cfg(&cfg)
        .unwrap();
    let raw_engine = engine.get_rocksdb();
    let mut gc_runner = TestGCRunner::new(0);

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

    // Check init value
    assert_eq!(GC_COMPACTION_FILTER_PERFORM.get(), 0);
    assert_eq!(GC_COMPACTION_FILTER_SKIP.get(), 0);

    // Set enable_compaction_filter = false
    // do_check_allowed will return false
    gc_runner
        .safe_point(TimeStamp::physical_now())
        .gc_raw_enable_filter(&raw_engine, false);

    assert_eq!(GC_COMPACTION_FILTER_PERFORM.get(), 0);
    assert_eq!(GC_COMPACTION_FILTER_SKIP.get(), 0);

    // The mvcc ts > gc safepoint,
    gc_runner
        .safe_point(TimeStamp::new(1).into_inner())
        .gc_raw(&raw_engine);
    assert_eq!(GC_COMPACTION_FILTER_PERFORM.get(), 1);
    assert_eq!(GC_COMPACTION_FILTER_SKIP.get(), 1);
}

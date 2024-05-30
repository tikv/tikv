// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use crossbeam::epoch;
use engine_traits::{CacheRange, CF_DEFAULT, CF_WRITE};
use region_cache_memory_engine::{
    encoding_for_filter, test_util::put_data, InternalBytes, RangeCacheEngineConfig,
    RangeCacheEngineContext, RangeCacheMemoryEngine, SkiplistHandle,
};
use tikv_util::config::{ReadableDuration, VersionTrack};
use txn_types::{Key, TimeStamp};

// We should not use skiplist.get directly as we only cares keys without
// sequence number suffix
fn key_exist(sl: &SkiplistHandle, key: &InternalBytes, guard: &epoch::Guard) -> bool {
    let mut iter = sl.iterator();
    iter.seek(key, guard);
    if iter.valid() && iter.key().same_user_key_with(key) {
        return true;
    }
    false
}

#[test]
fn test_gc_worker() {
    let mut config = RangeCacheEngineConfig::config_for_test();
    config.gc_interval = ReadableDuration(Duration::from_secs(1));
    let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(Arc::new(
        VersionTrack::new(config),
    )));
    let memory_controller = engine.memory_controller();
    let (write, default) = {
        let mut core = engine.core().write();
        core.mut_range_manager()
            .new_range(CacheRange::new(b"".to_vec(), b"z".to_vec()));
        let engine = core.engine();
        (engine.cf_handle(CF_WRITE), engine.cf_handle(CF_DEFAULT))
    };

    fail::cfg("oldest_seqno_ingest", "return(1000)").unwrap();

    let start_ts = TimeStamp::physical_now() - Duration::from_secs(10).as_millis() as u64;
    let commit_ts1 = TimeStamp::physical_now() - Duration::from_secs(9).as_millis() as u64;
    put_data(
        b"k",
        b"v1",
        start_ts,
        commit_ts1,
        100,
        false,
        &default,
        &write,
        memory_controller.clone(),
    );

    let start_ts = TimeStamp::physical_now() - Duration::from_secs(8).as_millis() as u64;
    let commit_ts2 = TimeStamp::physical_now() - Duration::from_secs(7).as_millis() as u64;
    put_data(
        b"k",
        b"v2",
        start_ts,
        commit_ts2,
        110,
        false,
        &default,
        &write,
        memory_controller.clone(),
    );

    let start_ts = TimeStamp::physical_now() - Duration::from_secs(6).as_millis() as u64;
    let commit_ts3 = TimeStamp::physical_now() - Duration::from_secs(5).as_millis() as u64;
    put_data(
        b"k",
        b"v3",
        start_ts,
        commit_ts3,
        110,
        false,
        &default,
        &write,
        memory_controller.clone(),
    );

    let start_ts = TimeStamp::physical_now() - Duration::from_secs(4).as_millis() as u64;
    let commit_ts4 = TimeStamp::physical_now() - Duration::from_secs(3).as_millis() as u64;
    put_data(
        b"k",
        b"v4",
        start_ts,
        commit_ts4,
        110,
        false,
        &default,
        &write,
        memory_controller.clone(),
    );

    let guard = &epoch::pin();
    for &ts in &[commit_ts1, commit_ts2, commit_ts3] {
        let key = Key::from_raw(b"k");
        let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(ts));

        assert!(key_exist(&write, &key, guard));
    }

    std::thread::sleep(Duration::from_secs_f32(1.5));

    let key = Key::from_raw(b"k");
    // now, the outdated mvcc versions should be gone
    for &ts in &[commit_ts1, commit_ts2, commit_ts3] {
        let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(ts));
        assert!(!key_exist(&write, &key, guard));
    }

    let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(commit_ts4));
    assert!(key_exist(&write, &key, guard));
}

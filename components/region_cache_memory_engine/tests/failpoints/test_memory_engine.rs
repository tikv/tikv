// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc::sync_channel, Arc},
    time::Duration,
};

use crossbeam::epoch;
use engine_rocks::util::new_engine;
use engine_traits::{
    CacheRange, Mutable, RangeCacheEngine, WriteBatch, WriteBatchExt, CF_DEFAULT, CF_WRITE,
    DATA_CFS,
};
use region_cache_memory_engine::{
    decode_key, encoding_for_filter, test_util::put_data, BackgroundTask, InternalBytes,
    InternalKey, RangeCacheEngineConfig, RangeCacheEngineContext, RangeCacheMemoryEngine,
    SkiplistHandle, ValueType,
};
use tempfile::Builder;
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
    let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
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

    fail::cfg("in_memory_engine_gc_oldest_seqno", "return(1000)").unwrap();

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("in_memory_engine_gc_finish", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

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

    let _ = rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let key = Key::from_raw(b"k");
    // now, the outdated mvcc versions should be gone
    for &ts in &[commit_ts1, commit_ts2, commit_ts3] {
        let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(ts));
        assert!(!key_exist(&write, &key, guard));
    }

    let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(commit_ts4));
    assert!(key_exist(&write, &key, guard));
}

#[test]
fn test_clean_up_tombstone() {
    let config = Arc::new(VersionTrack::new(RangeCacheEngineConfig::config_for_test()));
    let engine =
        RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(config.clone()));
    let range = CacheRange::new(b"".to_vec(), b"z".to_vec());

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("clean_lock_tombstone_done", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    engine.new_range(range.clone());
    let mut wb = engine.write_batch();
    wb.prepare_for_range(range.clone());
    wb.put_cf("lock", b"k", b"val").unwrap();
    wb.put_cf("lock", b"k1", b"val").unwrap();
    wb.put_cf("lock", b"k2", b"val").unwrap();
    wb.delete_cf("lock", b"k").unwrap();
    wb.delete_cf("lock", b"k1").unwrap();
    wb.delete_cf("lock", b"k2").unwrap();
    wb.put_cf("lock", b"k", b"val2").unwrap(); // seq 107
    wb.set_sequence_number(100).unwrap();
    wb.write().unwrap();

    let mut wb = engine.write_batch();
    wb.prepare_for_range(range.clone());
    wb.put_cf("lock", b"k", b"val").unwrap(); // seq 120
    wb.put_cf("lock", b"k1", b"val").unwrap(); // seq 121
    wb.put_cf("lock", b"k2", b"val").unwrap(); // seq 122
    wb.delete_cf("lock", b"k").unwrap(); // seq 123
    wb.delete_cf("lock", b"k1").unwrap(); // seq 124
    wb.delete_cf("lock", b"k2").unwrap(); // seq 125
    wb.set_sequence_number(120).unwrap();
    wb.write().unwrap();

    let lock_handle = engine.core().read().engine().cf_handle("lock");
    assert_eq!(lock_handle.len(), 13);

    engine
        .bg_worker_manager()
        .schedule_task(BackgroundTask::CleanLockTombstone(107))
        .unwrap();

    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut iter = engine.core().read().engine().cf_handle("lock").iterator();

    let mut first = true;
    let guard = &epoch::pin();
    for (k, seq, ty) in [
        (b"k".to_vec(), 123, ValueType::Deletion),
        (b"k".to_vec(), 120, ValueType::Value),
        (b"k".to_vec(), 106, ValueType::Value),
        (b"k1".to_vec(), 124, ValueType::Deletion),
        (b"k1".to_vec(), 121, ValueType::Value),
        (b"k2".to_vec(), 125, ValueType::Deletion),
        (b"k2".to_vec(), 122, ValueType::Value),
    ] {
        if first {
            iter.seek_to_first(guard);
            first = false;
        } else {
            iter.next(guard);
        }

        let key = iter.key();
        let InternalKey {
            user_key,
            sequence,
            v_type,
        } = decode_key(key.as_bytes());
        assert_eq!(sequence, seq);
        assert_eq!(user_key, &k);
        assert_eq!(v_type, ty);
    }
}

#[test]
fn test_evict_with_loading_range() {
    let path = Builder::new().prefix("test").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

    let config = RangeCacheEngineConfig::config_for_test();
    let mut engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
        VersionTrack::new(config),
    )));
    engine.set_disk_engine(rocks_engine);

    let range1 = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
    let range2 = CacheRange::new(b"k20".to_vec(), b"k30".to_vec());
    let range3 = CacheRange::new(b"k40".to_vec(), b"k50".to_vec());
    let (snapshot_load_tx, snapshot_load_rx) = sync_channel(0);
    fail::cfg_callback("on_snapshot_load_finished", move || {
        let _ = snapshot_load_tx.send(true);
    })
    .unwrap();

    let (loading_complete_tx, loading_complete_rx) = sync_channel(0);
    fail::cfg_callback("on_pending_range_completes_loading", move || {
        let _ = loading_complete_tx.send(true);
    })
    .unwrap();

    engine.load_range(range1.clone()).unwrap();
    engine.load_range(range2.clone()).unwrap();
    engine.load_range(range3.clone()).unwrap();

    let mut wb = engine.write_batch();
    // prepare range to trigger loading
    wb.prepare_for_range(range1.clone());
    wb.prepare_for_range(range2.clone());
    wb.prepare_for_range(range3.clone());
    wb.set_sequence_number(10).unwrap();
    wb.write().unwrap();

    // range1 and range2 will be evicted
    let r = CacheRange::new(b"k05".to_vec(), b"k25".to_vec());
    engine.evict_range(&r);

    snapshot_load_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    snapshot_load_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();

    loading_complete_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();

    engine.snapshot(range1, 100, 100).unwrap_err();
    engine.snapshot(range2, 100, 100).unwrap_err();
    engine.snapshot(range3, 100, 100).unwrap();

    drop(engine);

    std::thread::sleep(Duration::from_secs(1));
}

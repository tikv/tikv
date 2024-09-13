// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc::sync_channel, Arc},
    time::Duration,
};

use crossbeam::epoch;
use engine_rocks::util::new_engine;
use engine_traits::{
    CacheRegion, EvictReason, Mutable, RangeCacheEngine, RegionEvent, WriteBatch, WriteBatchExt,
    CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS,
};
use keys::{data_key, DATA_MAX_KEY, DATA_MIN_KEY};
use kvproto::metapb::Region;
use range_cache_memory_engine::{
    decode_key, encode_key_for_boundary_without_mvcc, encoding_for_filter,
    test_util::{new_region, put_data, put_data_in_rocks},
    BackgroundTask, InternalBytes, InternalKey, RangeCacheEngineConfig, RangeCacheEngineContext,
    RangeCacheMemoryEngine, RegionState, SkiplistHandle, ValueType,
};
use tempfile::Builder;
use tikv_util::config::{ReadableDuration, ReadableSize, VersionTrack};
use txn_types::{Key, TimeStamp, WriteType};

#[test]
fn test_set_disk_engine() {
    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("in_memory_engine_set_rocks_engine", move || {
        let _ = tx.send(true);
    })
    .unwrap();
    let mut engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
        VersionTrack::new(RangeCacheEngineConfig::config_for_test()),
    )));
    let path = Builder::new()
        .prefix("test_set_disk_engine")
        .tempdir()
        .unwrap();
    let path_str = path.path().to_str().unwrap();
    let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
    engine.set_disk_engine(rocks_engine.clone());
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
}

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
    engine
        .core()
        .region_manager()
        .new_region(CacheRegion::new(1, 0, DATA_MIN_KEY, DATA_MAX_KEY));
    let skip_engine = engine.core().engine();
    let write = skip_engine.cf_handle(CF_WRITE);
    let default = skip_engine.cf_handle(CF_DEFAULT);

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
        let key = Key::from_raw(b"zk");
        let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(ts));

        assert!(key_exist(&write, &key, guard));
    }

    let _ = rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let key = Key::from_raw(b"zk");
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
    let region = new_region(1, b"".to_vec(), b"z".to_vec());
    let cache_region = CacheRegion::from_region(&region);
    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("clean_lock_tombstone_done", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    engine.new_region(region.clone());
    let mut wb = engine.write_batch();
    wb.prepare_for_region(cache_region.clone());
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
    wb.prepare_for_region(cache_region.clone());
    wb.put_cf("lock", b"k", b"val").unwrap(); // seq 120
    wb.put_cf("lock", b"k1", b"val").unwrap(); // seq 121
    wb.put_cf("lock", b"k2", b"val").unwrap(); // seq 122
    wb.delete_cf("lock", b"k").unwrap(); // seq 123
    wb.delete_cf("lock", b"k1").unwrap(); // seq 124
    wb.delete_cf("lock", b"k2").unwrap(); // seq 125
    wb.set_sequence_number(120).unwrap();
    wb.write().unwrap();

    let lock_handle = engine.core().engine().cf_handle("lock");
    assert_eq!(lock_handle.len(), 13);

    engine
        .bg_worker_manager()
        .schedule_task(BackgroundTask::CleanLockTombstone(107))
        .unwrap();

    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut iter = engine.core().engine().cf_handle("lock").iterator();

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

    let r1 = new_region(1, b"k00".to_vec(), b"k10".to_vec());
    let r2 = new_region(2, b"k20".to_vec(), b"k30".to_vec());
    let r3 = new_region(3, b"k40".to_vec(), b"k50".to_vec());
    let (snapshot_load_tx, snapshot_load_rx) = sync_channel(0);

    // range1 and range2 will be evicted
    let r = new_region(4, b"k05".to_vec(), b"k25".to_vec());
    let engine_clone = engine.clone();
    fail::cfg_callback("on_snapshot_load_finished", move || {
        let _ = snapshot_load_tx.send(true);
        engine_clone.evict_region(&CacheRegion::from_region(&r), EvictReason::AutoEvict, None);
    })
    .unwrap();

    let (loading_complete_tx, loading_complete_rx) = sync_channel(0);
    fail::cfg_callback("on_completes_batch_loading", move || {
        let _ = loading_complete_tx.send(true);
    })
    .unwrap();

    let cache_region1 = CacheRegion::from_region(&r1);
    let cache_region2 = CacheRegion::from_region(&r2);
    let cache_region3 = CacheRegion::from_region(&r3);
    engine.load_region(cache_region1.clone()).unwrap();
    engine.load_region(cache_region2.clone()).unwrap();
    engine.load_region(cache_region3.clone()).unwrap();

    let mut wb = engine.write_batch();
    // prepare range to trigger loading
    wb.prepare_for_region(cache_region1.clone());
    wb.prepare_for_region(cache_region2.clone());
    wb.prepare_for_region(cache_region3.clone());
    wb.set_sequence_number(10).unwrap();
    wb.write().unwrap();

    snapshot_load_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    snapshot_load_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();

    loading_complete_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();

    let read_ts = TimeStamp::compose(TimeStamp::physical_now(), 0).into_inner();
    engine.snapshot(cache_region1, read_ts, 100).unwrap_err();
    engine.snapshot(cache_region2, read_ts, 100).unwrap_err();
    engine.snapshot(cache_region3, read_ts, 100).unwrap();
}

#[test]
fn test_cached_write_batch_cleared_when_load_failed() {
    let path = Builder::new().prefix("test").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

    let mut config = RangeCacheEngineConfig::config_for_test();
    config.stop_load_limit_threshold = Some(ReadableSize(20));
    config.soft_limit_threshold = Some(ReadableSize(30));
    config.hard_limit_threshold = Some(ReadableSize(40));
    let config = Arc::new(VersionTrack::new(config));
    let mut engine =
        RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(config.clone()));
    engine.set_disk_engine(rocks_engine);

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("on_snapshot_load_finished", move || {
        let _ = tx.send(true);
    })
    .unwrap();

    fail::cfg("on_snapshot_load_finished2", "pause").unwrap();

    // range1 will be canceled in on_snapshot_load_finished whereas range2 will be
    // canceled at begin
    let r1 = new_region(1, b"k00", b"k10");
    let r2 = new_region(2, b"k20", b"k30");
    let cache_region1 = CacheRegion::from_region(&r1);
    let cache_region2 = CacheRegion::from_region(&r2);
    engine.load_region(cache_region1.clone()).unwrap();
    engine.load_region(cache_region2.clone()).unwrap();

    let mut wb = engine.write_batch();
    // range1 starts to load
    wb.prepare_for_region(CacheRegion::from_region(&r1));
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    wb.put(b"zk05", b"val").unwrap();
    wb.put(b"zk06", b"val").unwrap();
    wb.prepare_for_region(CacheRegion::from_region(&r2));
    wb.put(b"zk25", b"val").unwrap();
    wb.set_sequence_number(100).unwrap();
    wb.write().unwrap();

    fail::remove("on_snapshot_load_finished2");

    test_util::eventually(
        Duration::from_millis(100),
        Duration::from_millis(2000),
        || {
            let regions_map = engine.core().region_manager().regions_map().read();
            // all failed regions should be removed.
            [1, 2]
                .into_iter()
                .all(|i| regions_map.region_meta(i).is_none())
        },
    );
}

#[test]
fn test_concurrency_between_delete_range_and_write_to_memory() {
    let path = Builder::new().prefix("test").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
    let mut wb = rocks_engine.write_batch();
    wb.put_cf(CF_LOCK, b"zk40", b"val").unwrap();
    wb.put_cf(CF_LOCK, b"zk41", b"val").unwrap();
    wb.put_cf(CF_LOCK, b"zk42", b"val").unwrap();
    wb.write().unwrap();

    let config = RangeCacheEngineConfig::config_for_test();
    let mut engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
        VersionTrack::new(config),
    )));
    engine.set_disk_engine(rocks_engine);

    let r1 = new_region(1, b"k00".to_vec(), b"k10".to_vec());
    let r2 = new_region(2, b"k20".to_vec(), b"k30".to_vec());
    let r3 = new_region(3, b"k40".to_vec(), b"k50".to_vec());
    let (snapshot_load_cancel_tx, snapshot_load_cancel_rx) = sync_channel(0);
    fail::cfg_callback("in_memory_engine_snapshot_load_canceled", move || {
        let _ = snapshot_load_cancel_tx.send(true);
    })
    .unwrap();
    let (snapshot_load_tx, snapshot_load_rx) = sync_channel(0);
    fail::cfg_callback("on_snapshot_load_finished", move || {
        let _ = snapshot_load_tx.send(true);
    })
    .unwrap();
    fail::cfg("before_clear_ranges_in_being_written", "pause").unwrap();

    let (write_batch_consume_tx, write_batch_consume_rx) = sync_channel(0);
    fail::cfg_callback("in_memory_engine_write_batch_consumed", move || {
        let _ = write_batch_consume_tx.send(true);
    })
    .unwrap();

    let (delete_range_tx, delete_range_rx) = sync_channel(0);
    fail::cfg_callback("in_memory_engine_delete_range_done", move || {
        let _ = delete_range_tx.send(true);
    })
    .unwrap();

    let cache_region1 = CacheRegion::from_region(&r1);
    let cache_region2 = CacheRegion::from_region(&r2);
    let cache_region3 = CacheRegion::from_region(&r3);

    engine.new_region(r1.clone());
    engine.new_region(r2.clone());
    engine.load_region(cache_region3.clone()).unwrap();

    let engine_clone = engine.clone();
    let (range_prepared_tx, range_prepared_rx) = sync_channel(0);
    let region1_clone = cache_region1.clone();
    let region2_clone = cache_region2.clone();
    let region3_clone = cache_region3.clone();
    let handle = std::thread::spawn(move || {
        let mut wb = engine_clone.write_batch();
        wb.prepare_for_region(region1_clone);
        wb.put_cf(CF_LOCK, b"zk02", b"val").unwrap();
        wb.put_cf(CF_LOCK, b"zk03", b"val").unwrap();
        wb.put_cf(CF_LOCK, b"zk04", b"val").unwrap();
        wb.set_sequence_number(100).unwrap();

        let mut wb2 = engine_clone.write_batch();
        wb2.prepare_for_region(region2_clone);
        wb.put_cf(CF_LOCK, b"zk22", b"val").unwrap();
        wb.put_cf(CF_LOCK, b"zk23", b"val").unwrap();
        wb2.set_sequence_number(200).unwrap();

        let mut wb3 = engine_clone.write_batch();
        wb3.prepare_for_region(region3_clone);
        wb3.set_sequence_number(300).unwrap();

        range_prepared_tx.send(true).unwrap();

        wb.write().unwrap();
        wb2.write().unwrap();
        wb3.write().unwrap();
    });

    range_prepared_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    // Now, three ranges are in write status, delete range will not be performed
    // until they leave the write status

    engine.evict_region(&cache_region1, EvictReason::AutoEvict, None);
    engine.evict_region(&cache_region2, EvictReason::AutoEvict, None);

    let verify_data = |r: &Region, expected_num: u64| {
        let handle = engine.core().engine().cf_handle(CF_LOCK);
        let (start, end) = encode_key_for_boundary_without_mvcc(&CacheRegion::from_region(r));
        let mut iter = handle.iterator();
        let guard = &epoch::pin();
        let mut count = 0;
        iter.seek(&start, guard);
        while iter.valid() && iter.key() < &end {
            count += 1;
            iter.next(guard);
        }
        assert_eq!(count, expected_num);
    };

    write_batch_consume_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    // Now, a DeleteRange task has been done: actually, the task will be delayed, so
    // the data has not be deleted
    verify_data(&r1, 3);
    // remove failpoint so that the range can leave write status
    fail::remove("before_clear_ranges_in_being_written");
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    // Now, data should be deleted
    verify_data(&r1, 0);

    // Next to test range2
    fail::cfg("before_clear_ranges_in_being_written", "pause").unwrap();
    write_batch_consume_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    verify_data(&r2, 2);
    // remove failpoint so that the range can leave write status
    fail::remove("before_clear_ranges_in_being_written");
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    verify_data(&r2, 0);

    // ensure the range enters on_snapshot_load_finished before eviction
    snapshot_load_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    engine.evict_region(&CacheRegion::from_region(&r3), EvictReason::AutoEvict, None);

    fail::cfg("before_clear_ranges_in_being_written", "pause").unwrap();
    write_batch_consume_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    verify_data(&r3, 3);
    snapshot_load_cancel_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    fail::remove("before_clear_ranges_in_being_written");
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    verify_data(&r3, 0);

    let _ = handle.join();
}

#[test]
fn test_double_delete_range_schedule() {
    let path = Builder::new().prefix("test").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

    let config = RangeCacheEngineConfig::config_for_test();
    let mut engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
        VersionTrack::new(config),
    )));
    engine.set_disk_engine(rocks_engine);

    let r1 = new_region(1, b"k00", b"k10");
    let r2 = new_region(2, b"k20", b"k30");
    let r3 = new_region(3, b"k40", b"k50");
    let (snapshot_load_tx, snapshot_load_rx) = sync_channel(0);
    let engine_clone = engine.clone();
    let r = new_region(4, b"k00", b"k60");
    fail::cfg_callback("on_snapshot_load_finished", move || {
        let _ = snapshot_load_tx.send(true);
        // evict all ranges. So the loading ranges will also be evicted and a delete
        // range task will be scheduled.
        engine_clone.evict_region(&CacheRegion::from_region(&r), EvictReason::AutoEvict, None);
    })
    .unwrap();

    let (delete_range_tx, delete_range_rx) = sync_channel(0);
    fail::cfg_callback("on_in_memory_engine_delete_range", move || {
        let _ = delete_range_tx.send(true);
    })
    .unwrap();

    engine.new_region(r1.clone());
    engine.new_region(r2.clone());
    let cache_region3 = CacheRegion::from_region(&r3);
    engine.load_region(cache_region3.clone()).unwrap();

    let snap1 = engine
        .snapshot(CacheRegion::from_region(&r1), 100, 100)
        .unwrap();
    let snap2 = engine
        .snapshot(CacheRegion::from_region(&r2), 100, 100)
        .unwrap();

    let mut wb = engine.write_batch();
    // prepare range to trigger loading
    wb.prepare_for_region(CacheRegion::from_region(&r3));
    wb.set_sequence_number(10).unwrap();
    wb.write().unwrap();

    snapshot_load_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();

    drop(snap1);
    drop(snap2);

    // two cached ranges
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    // sleep a while to ensure no further delete range will be scheduled
    delete_range_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap_err();
}

#[test]
fn test_load_with_gc() {
    let path = Builder::new().prefix("test").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

    let mut config = RangeCacheEngineConfig::config_for_test();
    config.gc_interval = ReadableDuration(Duration::from_secs(1));
    let mut engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
        VersionTrack::new(config),
    )));
    engine.set_disk_engine(rocks_engine.clone());

    // safe_point: 6
    // Rocks: [k1-5, k1-3, k2-4-d, k2-3, k3-7, k3-4, k3-1, k4-6-d, k4-5]
    // After load
    // IME: [k1-5, k3-7, k3-4]
    put_data_in_rocks(b"k1", b"val", 5, 4, false, &rocks_engine, WriteType::Put);
    put_data_in_rocks(b"k1", b"val", 3, 2, false, &rocks_engine, WriteType::Put);
    put_data_in_rocks(b"k2", b"val", 4, 3, false, &rocks_engine, WriteType::Delete);
    put_data_in_rocks(b"k2", b"val", 3, 2, false, &rocks_engine, WriteType::Put);
    put_data_in_rocks(b"k3", b"val", 7, 5, false, &rocks_engine, WriteType::Put);
    put_data_in_rocks(b"k3", b"val", 4, 3, false, &rocks_engine, WriteType::Put);
    put_data_in_rocks(b"k3", b"val", 1, 0, false, &rocks_engine, WriteType::Put);
    put_data_in_rocks(b"k4", b"val", 6, 0, false, &rocks_engine, WriteType::Delete);
    put_data_in_rocks(b"k4", b"val", 5, 0, false, &rocks_engine, WriteType::Put);

    fail::cfg("in_memory_engine_safe_point_in_loading", "return(6)").unwrap();
    let (load_tx, load_rx) = sync_channel(0);
    fail::cfg_callback("on_completes_batch_loading", move || {
        let _ = load_tx.send(true);
    })
    .unwrap();

    let region = new_region(1, b"", b"z");
    let range = CacheRegion::from_region(&region);
    engine.load_region(range.clone()).unwrap();
    let mut wb = engine.write_batch();
    wb.prepare_for_region(range.clone());
    wb.set_sequence_number(100).unwrap();
    wb.write().unwrap();

    load_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let expects = vec![(b"k1", 5), (b"k3", 7), (b"k3", 4)];
    let write_handle = engine.core().engine().cf_handle(CF_WRITE);

    let mut iter = write_handle.iterator();
    let guard = &epoch::pin();
    iter.seek_to_first(guard);
    for (key, commit_ts) in expects {
        let expect_key = Key::from_raw(&data_key(key)).into_encoded();
        let InternalKey { user_key, .. } = decode_key(iter.key().as_bytes());
        let (mem_key, ts) = Key::split_on_ts_for(user_key).unwrap();
        assert_eq!(expect_key, mem_key);
        assert_eq!(commit_ts, ts.into_inner());
        iter.next(guard);
    }

    // ensure the safe point of the engine
    engine.snapshot(range.clone(), 6, 100).unwrap_err();
    engine.snapshot(range, 7, 100).unwrap();
}

// test in-memory-engine can handle region split event after load region task
// is scheduled but before the task start running. As source region is split
// into multiple regions, IME should handle all the regions with in the range
// and update their state to active when batch loading task is finished.
#[test]
fn test_region_split_before_batch_loading_start() {
    let path = Builder::new().prefix("test").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

    let mut config = RangeCacheEngineConfig::config_for_test();
    config.gc_interval = ReadableDuration(Duration::from_secs(1));
    let mut engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
        VersionTrack::new(config),
    )));
    engine.set_disk_engine(rocks_engine.clone());

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("before_start_loading_region", move || {
        let _ = tx.send(());
    })
    .unwrap();
    fail::cfg("on_start_loading_region", "pause").unwrap();

    let region = new_region(1, b"k00", b"k30");
    let cache_region = CacheRegion::from_region(&region);

    // write some data into rocksdb to trigger batch loading.
    for i in 0..30 {
        let key = format!("k{:02}", i);
        put_data_in_rocks(
            key.as_bytes(),
            b"val",
            2,
            1,
            false,
            &rocks_engine,
            WriteType::Put,
        );
    }

    engine.load_region(cache_region.clone()).unwrap();

    // use write batch to trigger scheduling pending region loading task.
    let mut wb = engine.write_batch();
    wb.prepare_for_region(cache_region.clone());
    wb.set_sequence_number(10).unwrap();
    wb.put(b"zk00", b"val2").unwrap();
    wb.put(b"zk10", b"val2").unwrap();
    wb.put(b"zk20", b"val2").unwrap();
    wb.write().unwrap();
    assert_eq!(
        engine
            .core()
            .region_manager()
            .regions_map()
            .read()
            .region_meta(1)
            .unwrap()
            .get_state(),
        RegionState::Loading
    );

    // wait for task start.
    rx.recv().unwrap();

    // split source region into multiple new regions.
    let new_regions = vec![
        CacheRegion::new(1, 2, b"zk00", b"zk10"),
        CacheRegion::new(2, 2, b"zk10", b"zk20"),
        CacheRegion::new(3, 2, b"zk20", b"zk30"),
    ];
    let event = RegionEvent::Split {
        source: cache_region.clone(),
        new_regions: new_regions.clone(),
    };
    engine.on_region_event(event);
    {
        let regions_map = engine.core().region_manager().regions_map().read();
        for i in 1..=3 {
            assert_eq!(
                regions_map.region_meta(i).unwrap().get_state(),
                RegionState::Loading
            );
        }
    }

    // unblock batch loading.
    fail::remove("on_start_loading_region");

    // all new regions should be active after batch loading task finished.
    test_util::eventually(
        Duration::from_millis(50),
        Duration::from_millis(2000),
        || {
            let regions_map = engine.core().region_manager().regions_map().read();
            (1..=3).all(|i| regions_map.region_meta(i).unwrap().get_state() == RegionState::Active)
        },
    );
}

#[test]
fn test_cb_on_eviction() {
    let mut config = RangeCacheEngineConfig::config_for_test();
    config.gc_interval = ReadableDuration(Duration::from_secs(1));
    let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(Arc::new(
        VersionTrack::new(config),
    )));

    let region = new_region(1, b"", b"z");
    let cache_region = CacheRegion::from_region(&region);
    engine.new_region(region.clone());

    let mut wb = engine.write_batch();
    wb.prepare_for_region(cache_region.clone());
    wb.set_sequence_number(10).unwrap();
    wb.put(b"a", b"val1").unwrap();
    wb.put(b"b", b"val2").unwrap();
    wb.put(b"c", b"val3").unwrap();
    wb.write().unwrap();

    fail::cfg("in_memory_engine_on_delete_regions", "pause").unwrap();

    let (tx, rx) = sync_channel(0);
    engine.evict_region(
        &cache_region,
        EvictReason::BecomeFollower,
        Some(Box::new(move || {
            let _ = tx.send(true);
        })),
    );

    rx.recv_timeout(Duration::from_secs(1)).unwrap_err();
    fail::remove("in_memory_engine_on_delete_regions");
    let _ = rx.recv();

    {
        let regions_map = engine.core().region_manager().regions_map().read();
        assert!(regions_map.region_meta(1).is_none());
    }
}

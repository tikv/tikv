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
    decode_key, encode_key_for_boundary_without_mvcc, encoding_for_filter, test_util::put_data,
    BackgroundTask, InternalBytes, InternalKey, RangeCacheEngineConfig, RangeCacheEngineContext,
    RangeCacheMemoryEngine, SkiplistHandle, ValueType,
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

    fail::cfg("in_memry_engine_gc_oldest_seqno", "return(1000)").unwrap();

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
    let engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(config.clone()));
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
fn test_concurrency_between_delete_range_and_write_to_memory() {
    let path = Builder::new().prefix("test").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

    let config = Arc::new(VersionTrack::new(RangeCacheEngineConfig::config_for_test()));
    let mut engine = RangeCacheMemoryEngine::new(RangeCacheEngineContext::new(config.clone()));
    engine.set_disk_engine(rocks_engine);

    let range1 = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
    let range2 = CacheRange::new(b"k20".to_vec(), b"k30".to_vec());
    let range3 = CacheRange::new(b"k40".to_vec(), b"k50".to_vec());

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

    engine.new_range(range1.clone());
    engine.new_range(range2.clone());
    engine.load_range(range3.clone());

    let engine_clone = engine.clone();
    let (range_prepared_tx, range_prepared_rx) = sync_channel(0);
    let range1_clone = range1.clone();
    let range2_clone = range2.clone();
    let range3_clone = range3.clone();
    let handle = std::thread::spawn(move || {
        let mut wb = engine_clone.write_batch();
        wb.prepare_for_range(range1_clone);
        wb.put(b"k02", b"val").unwrap();
        wb.put(b"k03", b"val").unwrap();
        wb.put(b"k04", b"val").unwrap();
        wb.set_sequence_number(100).unwrap();

        let mut wb2 = engine_clone.write_batch();
        wb2.prepare_for_range(range2_clone);
        wb2.put(b"k22", b"val").unwrap();
        wb2.put(b"k23", b"val").unwrap();
        wb2.set_sequence_number(200).unwrap();

        let mut wb3 = engine_clone.write_batch();
        wb3.prepare_for_range(range3_clone);
        wb3.put(b"k32", b"val").unwrap();
        wb3.put(b"k33", b"val").unwrap();
        wb3.put(b"k34", b"val").unwrap();
        wb3.set_sequence_number(300).unwrap();

        range_prepared_tx.send(true).unwrap();

        wb.write().unwrap();
        wb2.write().unwrap();
        wb3.write();
    });

    range_prepared_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    // Now, three ranges are in write status, delete range will not be performed
    // until they leave the write status

    engine.evict_range(&range1);
    engine.evict_range(&range2);
    engine.evict_range(&range3);

    let verify_data = |range, expected_num: u64| {
        let handle = engine.core().write().engine().cf_handle(CF_DEFAULT);
        let (start, end) = encode_key_for_boundary_without_mvcc(range);
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
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    // Now, a DeleteRange task has been done: actually, the task will be delayed, so
    // the data has not be deleted
    verify_data(&range1, 3);
    // remove failpoint so that the range can leave write status
    fail::remove("before_clear_ranges_in_being_written");
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    // Now, data should be deleted
    verify_data(&range1, 0);

    // Next to test range2
    fail::cfg("before_clear_ranges_in_being_written", "pause").unwrap();
    write_batch_consume_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    verify_data(&range2, 2);
    // remove failpoint so that the range can leave write status
    fail::remove("before_clear_ranges_in_being_written");
    delete_range_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    verify_data(&range2, 0);
}

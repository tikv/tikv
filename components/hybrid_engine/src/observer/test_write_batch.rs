// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::mpsc::sync_channel, time::Duration};

use crossbeam::epoch;
use engine_traits::{CacheRegion, Mutable, Peekable, RegionCacheEngine, WriteBatch, WriteBatchExt};
use in_memory_engine::{
    decode_key, test_util::new_region, InMemoryEngineConfig, InternalKey, RegionCacheStatus,
    ValueType,
};
use raftstore::coprocessor::{WriteBatchObserver, WriteBatchWrapper};

use super::RegionCacheWriteBatchObserver;
use crate::{engine::SnapshotContext, util::hybrid_engine_for_tests};

#[test]
fn test_sequence_number_unique() {
    let (_path, hybrid_engine) =
        hybrid_engine_for_tests("temp", InMemoryEngineConfig::config_for_test(), |_| {}).unwrap();

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("ime_on_completes_batch_loading", move || {
        fail::cfg("ime_on_start_loading_region", "pause").unwrap();
        tx.send(true).unwrap();
    })
    .unwrap();

    let engine = hybrid_engine.region_cache_engine().clone();
    let observer = RegionCacheWriteBatchObserver::new(engine.clone());

    // first write some data, these data should be handled by batch loading.
    let mut wb = WriteBatchWrapper::new(
        hybrid_engine.disk_engine().write_batch(),
        Some(observer.create_observable_write_batch()),
    );

    wb.put(b"zk5", b"val").unwrap(); // seq 1
    wb.put(b"zk7", b"val").unwrap(); // seq 2

    let r = new_region(1, b"k", b"k5");
    engine.new_region(r.clone());
    wb.write().unwrap();

    // Mock that we have a loading range, and there are some keys written in it
    // during the load
    let r2 = new_region(2, b"k5", b"k7");
    let r3 = new_region(3, b"k7", b"k9");
    let cache_region2 = CacheRegion::from_region(&r2);
    let cache_region3 = CacheRegion::from_region(&r3);
    engine.load_region(cache_region2.clone()).unwrap();
    engine.load_region(cache_region3.clone()).unwrap();

    // The sequence number of write batch should be increased one by one, otherwise
    // if a delete and a put of the same key occurs in the same write batch,
    // the delete will be hidden by the put even the delete is performed
    // after the put.
    // while we block the batch loading of region3, it's new KVs are still directly
    // written into the skiplist.
    let mut wb = WriteBatchWrapper::new(
        hybrid_engine.disk_engine().write_batch(),
        Some(observer.create_observable_write_batch()),
    );
    wb.prepare_for_region(&r);
    wb.put(b"zk", b"val").unwrap(); // seq 3
    wb.delete(b"zk").unwrap(); // seq 4
    wb.put(b"zk2", b"val").unwrap(); // seq 5

    wb.prepare_for_region(&r2);
    wb.put(b"zk6", b"val").unwrap(); // seq 6
    wb.delete(b"zk5").unwrap(); // seq 7
    wb.put(b"zk5", b"val2").unwrap(); // seq 8

    wb.prepare_for_region(&r3);
    wb.put(b"zk8", b"val").unwrap(); // seq 9
    wb.put(b"zk7", b"val2").unwrap(); // seq 10

    rx.recv().unwrap();
    wb.write().unwrap();

    let mut iter = engine.core().engine().cf_handle("default").iterator();
    let guard = &epoch::pin();

    let mut first = true;

    for (k, sequence, v_type) in [
        (b"zk".to_vec(), 4, ValueType::Deletion),
        (b"zk".to_vec(), 3, ValueType::Value),
        (b"zk2".to_vec(), 5, ValueType::Value),
        (b"zk5".to_vec(), 8, ValueType::Value),
        (b"zk5".to_vec(), 7, ValueType::Deletion),
        // NOTE: for batch loading, we always use the current seq number
        // to write all the keys.
        (b"zk5".to_vec(), 2, ValueType::Value),
        (b"zk6".to_vec(), 6, ValueType::Value),
        (b"zk7".to_vec(), 10, ValueType::Value),
        // "zk7" with seq 2 is block, so invisible here.
        (b"zk8".to_vec(), 9, ValueType::Value),
    ] {
        if first {
            iter.seek_to_first(guard);
            first = false;
        } else {
            iter.next(guard);
        }

        let expected_key = InternalKey {
            user_key: k.as_slice(),
            v_type,
            sequence,
        };
        let key = iter.key();
        let got_key = decode_key(key.as_bytes());
        assert_eq!(expected_key, got_key);
    }
}

#[test]
fn test_write_to_both_engines() {
    let region = new_region(1, b"", b"z");
    let region_clone = region.clone();
    let (_path, hybrid_engine) = hybrid_engine_for_tests(
        "temp",
        InMemoryEngineConfig::config_for_test(),
        move |memory_engine| {
            let id = region_clone.id;
            memory_engine.new_region(region_clone);
            memory_engine.core().region_manager().set_safe_point(id, 5);
        },
    )
    .unwrap();
    let engine = hybrid_engine.region_cache_engine().clone();
    let observer = RegionCacheWriteBatchObserver::new(engine.clone());

    let cache_region = CacheRegion::from_region(&region);
    let mut ob_wb = observer.new_observable_write_batch();
    ob_wb.cache_write_batch.prepare_for_region(&region);
    ob_wb
        .cache_write_batch
        .set_region_cache_status(RegionCacheStatus::Cached);
    let mut write_batch = WriteBatchWrapper::new(
        hybrid_engine.disk_engine().write_batch(),
        Some(Box::new(ob_wb)),
    );
    write_batch.put(b"zhello", b"world").unwrap();
    let seq = write_batch.write().unwrap();
    assert!(seq > 0);
    let actual: &[u8] = &hybrid_engine
        .disk_engine()
        .get_value(b"zhello")
        .unwrap()
        .unwrap();
    assert_eq!(b"world", &actual);
    let ctx = SnapshotContext {
        region: Some(cache_region.clone()),
        read_ts: 10,
    };
    let snap = hybrid_engine.new_snapshot(Some(ctx));
    let actual: &[u8] = &snap.get_value(b"zhello").unwrap().unwrap();
    assert_eq!(b"world", &actual);
    let actual: &[u8] = &snap.disk_snap().get_value(b"zhello").unwrap().unwrap();
    assert_eq!(b"world", &actual);
    let actual: &[u8] = &snap
        .region_cache_snap()
        .unwrap()
        .get_value(b"zhello")
        .unwrap()
        .unwrap();
    assert_eq!(b"world", &actual);
}

#[test]
fn test_set_sequence_number() {
    let (_path, hybrid_engine) = hybrid_engine_for_tests(
        "temp",
        InMemoryEngineConfig::config_for_test(),
        |memory_engine| {
            let region = new_region(1, b"k00", b"k10");
            memory_engine.new_region(region);
            memory_engine.core().region_manager().set_safe_point(1, 10);
        },
    )
    .unwrap();

    let engine = hybrid_engine.region_cache_engine().clone();
    let observer = RegionCacheWriteBatchObserver::new(engine.clone());
    let mut write_batch = observer.new_observable_write_batch();

    write_batch
        .cache_write_batch
        .set_sequence_number(0)
        .unwrap(); // First call ok.
    assert!(
        write_batch
            .cache_write_batch
            .set_sequence_number(0)
            .is_err()
    ); // Second call err.
}

#[test]
fn test_delete_range() {
    let region1 = new_region(1, b"k00", b"k10");
    let region2 = new_region(2, b"k20", b"k30");
    let cache_region1 = CacheRegion::from_region(&region1);
    let cache_region2 = CacheRegion::from_region(&region2);

    let region1_clone = region1.clone();
    let region2_clone = region2.clone();
    let (_path, hybrid_engine) = hybrid_engine_for_tests(
        "temp",
        InMemoryEngineConfig::config_for_test(),
        move |memory_engine| {
            memory_engine.new_region(region1_clone);
            memory_engine.new_region(region2_clone);
        },
    )
    .unwrap();

    let engine = hybrid_engine.region_cache_engine().clone();
    let observer = RegionCacheWriteBatchObserver::new(engine.clone());

    let mut wb = WriteBatchWrapper::new(
        hybrid_engine.disk_engine().write_batch(),
        Some(observer.create_observable_write_batch()),
    );
    wb.prepare_for_region(&region1);
    wb.put(b"zk05", b"val").unwrap();
    wb.put(b"zk08", b"val2").unwrap();
    wb.prepare_for_region(&region2);
    wb.put(b"zk25", b"val3").unwrap();
    wb.put(b"zk27", b"val4").unwrap();
    wb.write().unwrap();

    hybrid_engine
        .region_cache_engine()
        .snapshot(cache_region1.clone(), 1000, 1000)
        .unwrap();
    hybrid_engine
        .region_cache_engine()
        .snapshot(cache_region2.clone(), 1000, 1000)
        .unwrap();
    assert_eq!(
        4,
        hybrid_engine
            .region_cache_engine()
            .core()
            .engine()
            .cf_handle("default")
            .len()
    );

    let mut wb = WriteBatchWrapper::new(
        hybrid_engine.disk_engine().write_batch(),
        Some(observer.create_observable_write_batch()),
    );
    // all ranges overlapped with it will be evicted
    wb.prepare_for_region(&region1);
    wb.delete_range(b"zk05", b"zk08").unwrap();
    wb.prepare_for_region(&region2);
    wb.delete_range(b"zk20", b"zk21").unwrap();
    wb.write().unwrap();

    hybrid_engine
        .region_cache_engine()
        .snapshot(cache_region1.clone(), 1000, 1000)
        .unwrap_err();
    hybrid_engine
        .region_cache_engine()
        .snapshot(cache_region2.clone(), 1000, 1000)
        .unwrap_err();
    let m_engine = hybrid_engine.region_cache_engine();

    test_util::eventually(
        Duration::from_millis(100),
        Duration::from_millis(2000),
        || m_engine.core().engine().cf_handle("default").is_empty(),
    );
}

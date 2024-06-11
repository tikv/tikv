// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::sync_channel;

use crossbeam::epoch;
use engine_traits::{CacheRange, Mutable, WriteBatch, WriteBatchExt};
use hybrid_engine::util::hybrid_engine_for_tests;
use region_cache_memory_engine::{decode_key, InternalKey, RangeCacheEngineConfig, ValueType};

#[test]
fn test_sequence_number_unique() {
    let (_path, hybrid_engine) =
        hybrid_engine_for_tests("temp", RangeCacheEngineConfig::config_for_test(), |_| {}).unwrap();

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("on_pending_range_completes_loading", move || {
        fail::cfg("on_snapshot_load_finished", "pause").unwrap();
        tx.send(true).unwrap();
    })
    .unwrap();

    let engine = hybrid_engine.region_cache_engine().clone();
    let r = CacheRange::new(b"k".to_vec(), b"k5".to_vec());
    engine.new_range(r.clone());

    // mock that we have a loading range, and there are some keys written in it
    // during the load
    let r2 = CacheRange::new(b"k5".to_vec(), b"k7".to_vec());
    let r3 = CacheRange::new(b"k7".to_vec(), b"k9".to_vec());

    engine.load_range(r2.clone()).unwrap();
    engine.load_range(r3.clone()).unwrap();

    // The sequence number of write batch should be increased one by one, otherwise
    // if a delete and a put of the same key occurs in the same write batch,
    // the delete will be hidden by the put even the delete is performed
    // after the put.
    let mut wb = hybrid_engine.write_batch();
    wb.prepare_for_range(r.clone());
    wb.put(b"k", b"val").unwrap(); // seq 6
    wb.delete(b"k").unwrap(); // seq 7
    wb.put(b"k2", b"val").unwrap(); // seq 8

    wb.prepare_for_range(r2.clone());
    wb.put(b"k6", b"val").unwrap(); // seq 3
    wb.put(b"k5", b"val").unwrap(); // seq 4
    wb.delete(b"k5").unwrap(); // seq 5

    wb.prepare_for_range(r3.clone());
    wb.put(b"k8", b"val").unwrap(); // seq 1
    wb.put(b"k7", b"val").unwrap(); // seq 2

    rx.recv().unwrap();
    wb.write().unwrap();

    // For sequence number increment, the loading range get increment first, the
    // loading range that completes the loading before consuming the write batch get
    // increment second, and the cached range get increment last.
    let mut iter = engine
        .core()
        .read()
        .engine()
        .cf_handle("default")
        .iterator();
    let guard = &epoch::pin();

    let mut first = true;

    for (k, seq, ty) in [
        (b"k".to_vec(), 7, ValueType::Deletion),
        (b"k".to_vec(), 6, ValueType::Value),
        (b"k2".to_vec(), 8, ValueType::Value),
        (b"k5".to_vec(), 5, ValueType::Deletion),
        (b"k5".to_vec(), 4, ValueType::Value),
        (b"k6".to_vec(), 3, ValueType::Value),
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
        } = decode_key(&key.as_bytes());
        assert_eq!(sequence, seq);
        assert_eq!(user_key, &k);
        assert_eq!(v_type, ty);
    }
}

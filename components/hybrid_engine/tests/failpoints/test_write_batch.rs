// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::sync_channel;

use crossbeam::epoch;
use engine_traits::{CacheRegion, Mutable, WriteBatch, WriteBatchExt};
use hybrid_engine::util::hybrid_engine_for_tests;
use in_memory_engine::{
    decode_key, test_util::new_region, InternalKey, RegionCacheEngineConfig, ValueType,
};

#[test]
fn test_sequence_number_unique() {
    let (_path, hybrid_engine) =
        hybrid_engine_for_tests("temp", RegionCacheEngineConfig::config_for_test(), |_| {})
            .unwrap();

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("on_completes_batch_loading", move || {
        fail::cfg("on_start_loading_region", "pause").unwrap();
        tx.send(true).unwrap();
    })
    .unwrap();

    // first write some data, these data should be handled by batch loading.
    let mut wb = hybrid_engine.write_batch();
    wb.put(b"zk5", b"val").unwrap(); // seq 1
    wb.put(b"zk7", b"val").unwrap(); // seq 2

    let engine = hybrid_engine.region_cache_engine().clone();
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
    let mut wb = hybrid_engine.write_batch();
    wb.prepare_for_region(CacheRegion::from_region(&r));
    wb.put(b"zk", b"val").unwrap(); // seq 3
    wb.delete(b"zk").unwrap(); // seq 4
    wb.put(b"zk2", b"val").unwrap(); // seq 5

    wb.prepare_for_region(CacheRegion::from_region(&r2));
    wb.put(b"zk6", b"val").unwrap(); // seq 6
    wb.delete(b"zk5").unwrap(); // seq 7
    wb.put(b"zk5", b"val2").unwrap(); // seq 8

    wb.prepare_for_region(CacheRegion::from_region(&r3));
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

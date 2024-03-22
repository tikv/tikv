use std::{sync::mpsc::sync_channel, time::Duration};

use engine_traits::{CacheRange, SnapshotContext, CF_WRITE};
use keys::{data_key, DATA_MAX_KEY, DATA_MIN_KEY};
use test_raftstore::new_node_cluster_with_hybrid_engine;
use txn_types::Key;

#[test]
fn test_basic_put_get() {
    let mut cluster = new_node_cluster_with_hybrid_engine(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 1;
    cluster.run();

    let range_cache_engine = cluster.get_range_cache_engine(1);
    // FIXME: load is not implemented, so we have to insert range manually
    {
        let mut core = range_cache_engine.core().write();
        let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec());
        core.mut_range_manager().new_range(cache_range.clone());
        core.mut_range_manager().set_safe_point(&cache_range, 1000);
        core.mut_range_manager()
            .set_range_readable(&cache_range, true);
    }

    cluster.put(b"k05", b"val").unwrap();
    let snap_ctx = SnapshotContext {
        read_ts: 1001,
        range: None,
    };
    let (tx, rx) = sync_channel(1);
    fail::cfg_callback("on_range_cache_get_value", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    let val = cluster.get_with_snap_ctx(b"k05", snap_ctx).unwrap();
    assert_eq!(&val, b"val");

    // verify it's read from range cache engine
    assert!(rx.try_recv().unwrap());
}

#[test]
fn test_load() {
    let test_load = |concurrent_with_split: bool| {
        let mut cluster = new_node_cluster_with_hybrid_engine(0, 1);
        cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
        cluster.run();

        for i in (0..30).step_by(2) {
            let key = format!("key-{:04}", i);
            let encoded_key = Key::from_raw(key.as_bytes())
                .append_ts(20.into())
                .into_encoded();
            cluster.must_put(&encoded_key, b"val-default");
            cluster.must_put_cf(CF_WRITE, &encoded_key, b"val-write");
        }
        let r = cluster.get_region(b"");
        let split_key1 = format!("key-{:04}", 10).into_bytes();
        cluster.must_split(&r, &split_key1);
        let r = cluster.get_region(&split_key1);
        let split_key2 = format!("key-{:04}", 20).into_bytes();
        cluster.must_split(&r, &split_key2);

        let (tx, rx) = sync_channel(1);
        fail::cfg_callback("on_snapshot_load_finished", move || {
            tx.send(true).unwrap();
        })
        .unwrap();

        // load range
        {
            let range_cache_engine = cluster.get_range_cache_engine(1);
            let mut core = range_cache_engine.core().write();
            if concurrent_with_split {
                // Load the whole range as if it is not splitted. Loading process should handle
                // it correctly.
                let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec());
                core.mut_range_manager().load_range(cache_range).unwrap();
            } else {
                let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), data_key(&split_key1));
                let cache_range2 = CacheRange::new(data_key(&split_key1), data_key(&split_key2));
                let cache_range3 = CacheRange::new(data_key(&split_key2), DATA_MAX_KEY.to_vec());
                core.mut_range_manager().load_range(cache_range).unwrap();
                core.mut_range_manager().load_range(cache_range2).unwrap();
                core.mut_range_manager().load_range(cache_range3).unwrap();
            }
        }

        // put key to trigger load task
        for i in &[0, 10, 20] {
            let key = format!("key-{:04}", i);
            let encoded_key = Key::from_raw(key.as_bytes())
                .append_ts(20.into())
                .into_encoded();
            cluster.must_put(&encoded_key, b"val-default");
            cluster.must_put_cf(CF_WRITE, &encoded_key, b"val-write");
        }

        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        rx.recv_timeout(Duration::from_secs(5)).unwrap();

        for i in (1..30).step_by(2) {
            let key = format!("key-{:04}", i);
            let encoded_key = Key::from_raw(key.as_bytes())
                .append_ts(20.into())
                .into_encoded();
            cluster.must_put(&encoded_key, b"val-default");
            cluster.must_put_cf(CF_WRITE, &encoded_key, b"val-write");
        }

        let (tx, rx) = sync_channel(1);
        fail::cfg_callback("on_range_cache_get_value", move || {
            tx.send(true).unwrap();
        })
        .unwrap();

        let snap_ctx = SnapshotContext {
            read_ts: 20,
            range: None,
        };

        for i in 0..30 {
            let key = format!("key-{:04}", i);
            let encoded_key = Key::from_raw(key.as_bytes())
                .append_ts(20.into())
                .into_encoded();
            let val = cluster
                .get_cf_with_snap_ctx(CF_WRITE, &encoded_key, snap_ctx.clone())
                .unwrap();
            assert_eq!(&val, b"val-write");
            // verify it's read from range cache engine
            assert!(rx.try_recv().unwrap());

            let val = cluster
                .get_with_snap_ctx(&encoded_key, snap_ctx.clone())
                .unwrap();
            assert_eq!(&val, b"val-default");
            // verify it's read from range cache engine
            assert!(rx.try_recv().unwrap());
        }
    };
    test_load(false);
    test_load(true);
}

#[test]
fn test_write_batch_cache_during_load() {
    let mut cluster = new_node_cluster_with_hybrid_engine(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    cluster.run();

    for i in 0..10 {
        let key = format!("key-{:04}", i);
        let encoded_key = Key::from_raw(key.as_bytes())
            .append_ts(20.into())
            .into_encoded();
        cluster.must_put(&encoded_key, b"val-default");
        cluster.must_put_cf(CF_WRITE, &encoded_key, b"val-write");
    }

    fail::cfg("on_snapshot_load_finished", "pause").unwrap();
    // load range
    {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec());
        core.mut_range_manager().load_range(cache_range).unwrap();
    }

    // First, cache some entries after the acquire of the snapshot
    // Then, cache some additional entries after the snapshot loaded and the
    // previous cache consumed
    for i in 10..20 {
        let key = format!("key-{:04}", i);
        let encoded_key = Key::from_raw(key.as_bytes())
            .append_ts(20.into())
            .into_encoded();
        cluster.must_put(&encoded_key, b"val-default");
        cluster.must_put_cf(CF_WRITE, &encoded_key, b"val-write");
    }

    let (tx1, rx1) = sync_channel(1);
    fail::cfg_callback("on_pending_range_completes_loading", move || {
        tx1.send(true).unwrap();
    })
    .unwrap();

    // use it to mock concurrency between consuming cached write batch and cache
    // further writes
    fail::cfg("on_cached_write_batch_consumed", "pause").unwrap();
    fail::remove("on_snapshot_load_finished");

    let (tx2, rx2) = sync_channel(1);
    fail::cfg_callback("on_range_cache_get_value", move || {
        tx2.send(true).unwrap();
    })
    .unwrap();
    let snap_ctx = SnapshotContext {
        read_ts: 20,
        range: None,
    };

    for i in 20..30 {
        if i == 29 {
            let key = format!("key-{:04}", 1);
            let encoded_key = Key::from_raw(key.as_bytes())
                .append_ts(20.into())
                .into_encoded();
            let val = cluster
                .get_cf_with_snap_ctx(CF_WRITE, &encoded_key, snap_ctx.clone())
                .unwrap();
            assert_eq!(&val, b"val-write");
            // We should not read the value in the memory engine at this phase.
            rx2.try_recv().unwrap_err();
            fail::remove("on_cached_write_batch_consumed");
        }
        let key = format!("key-{:04}", i);
        let encoded_key = Key::from_raw(key.as_bytes())
            .append_ts(20.into())
            .into_encoded();
        cluster.must_put(&encoded_key, b"val-default");
        cluster.must_put_cf(CF_WRITE, &encoded_key, b"val-write");
    }

    // ensure the pending range is transfered to normal range
    rx1.recv_timeout(Duration::from_secs(5)).unwrap();

    for i in 0..30 {
        let key = format!("key-{:04}", i);
        let encoded_key = Key::from_raw(key.as_bytes())
            .append_ts(20.into())
            .into_encoded();
        let val = cluster
            .get_cf_with_snap_ctx(CF_WRITE, &encoded_key, snap_ctx.clone())
            .unwrap();
        assert_eq!(&val, b"val-write");
        // verify it's read from range cache engine
        assert!(rx2.try_recv().unwrap());

        let val = cluster
            .get_with_snap_ctx(&encoded_key, snap_ctx.clone())
            .unwrap();
        assert_eq!(&val, b"val-default");
        // verify it's read from range cache engine
        assert!(rx2.try_recv().unwrap());
    }
}

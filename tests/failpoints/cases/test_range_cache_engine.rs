use std::{
    sync::{mpsc::sync_channel, Arc, Mutex},
    time::Duration,
};

use engine_traits::{CacheRange, RangeCacheEngine, SnapshotContext, CF_DEFAULT, CF_WRITE};
use keys::{data_key, DATA_MAX_KEY, DATA_MIN_KEY};
use kvproto::raft_cmdpb::RaftCmdRequest;
use test_raftstore::{
    make_cb, new_node_cluster_with_hybrid_engine_with_no_range_cache, new_put_cmd, new_request,
    Cluster, HybridEngineImpl, NodeCluster, Simulator,
};
use tikv_util::HandyRwLock;
use txn_types::Key;

#[test]
fn test_basic_put_get() {
    let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 1;
    cluster.run();

    let range_cache_engine = cluster.get_range_cache_engine(1);
    // FIXME: load is not implemented, so we have to insert range manually
    {
        let mut core = range_cache_engine.core().write();
        let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec());
        core.mut_range_manager().new_range(cache_range.clone());
        core.mut_range_manager().set_safe_point(&cache_range, 1000);
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

    let val = cluster.get_with_snap_ctx(b"k05", false, snap_ctx).unwrap();
    assert_eq!(&val, b"val");

    // verify it's read from range cache engine
    assert!(rx.try_recv().unwrap());
}

#[test]
fn test_load() {
    let test_load = |concurrent_with_split: bool| {
        let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
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

        // ensure the snapshot is loaded
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
                .get_cf_with_snap_ctx(CF_WRITE, &encoded_key, false, snap_ctx.clone())
                .unwrap();
            assert_eq!(&val, b"val-write");
            // verify it's read from range cache engine
            assert!(rx.try_recv().unwrap());

            let val = cluster
                .get_with_snap_ctx(&encoded_key, false, snap_ctx.clone())
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
    let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
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
                .get_cf_with_snap_ctx(CF_WRITE, &encoded_key, false, snap_ctx.clone())
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
            .get_cf_with_snap_ctx(CF_WRITE, &encoded_key, false, snap_ctx.clone())
            .unwrap();
        assert_eq!(&val, b"val-write");
        // verify it's read from range cache engine
        assert!(rx2.try_recv().unwrap());

        let val = cluster
            .get_with_snap_ctx(&encoded_key, false, snap_ctx.clone())
            .unwrap();
        assert_eq!(&val, b"val-default");
        // verify it's read from range cache engine
        assert!(rx2.try_recv().unwrap());
    }
}

#[test]
// It tests that after we schedule the pending range to load snapshot, the range
// splits.
fn test_load_with_split() {
    let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
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

    let (tx, rx) = sync_channel(0);
    // let channel to make load process block at finishing loading snapshot
    let (tx2, rx2) = sync_channel(0);
    let rx2 = Arc::new(Mutex::new(rx2));
    fail::cfg_callback("on_snapshot_load_finished", move || {
        tx.send(true).unwrap();
        let _ = rx2.lock().unwrap().recv().unwrap();
    })
    .unwrap();

    // load range
    {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        // Load the whole range as if it is not splitted. Loading process should handle
        // it correctly.
        let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec());
        core.mut_range_manager().load_range(cache_range).unwrap();
    }

    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // Now, the snapshot load is finished, and blocked before consuming cached
    // write batches. Let split the range.

    let r = cluster.get_region(b"");
    let split_key1 = format!("key-{:04}", 10).into_bytes();
    cluster.must_split(&r, &split_key1);
    let r = cluster.get_region(&split_key1);
    let split_key2 = format!("key-{:04}", 20).into_bytes();
    cluster.must_split(&r, &split_key2);
    // Now, we have 3 regions: [min, 10), [10, 20), [20, max)

    for i in (1..30).step_by(2) {
        let key = format!("key-{:04}", i);
        let encoded_key = Key::from_raw(key.as_bytes())
            .append_ts(20.into())
            .into_encoded();
        cluster.must_put(&encoded_key, b"val-default");
        cluster.must_put_cf(CF_WRITE, &encoded_key, b"val-write");
    }

    // unblock loading task
    tx2.send(true).unwrap();

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
            .get_cf_with_snap_ctx(CF_WRITE, &encoded_key, false, snap_ctx.clone())
            .unwrap();
        assert_eq!(&val, b"val-write");
        // verify it's read from range cache engine
        assert!(rx.try_recv().unwrap());

        let val = cluster
            .get_with_snap_ctx(&encoded_key, false, snap_ctx.clone())
            .unwrap();
        assert_eq!(&val, b"val-default");
        // verify it's read from range cache engine
        assert!(rx.try_recv().unwrap());
    }
}

// It tests race between split and load.
// Takes k1-k10 as an example:
// We want to load k1-k10 where k1-k10 is already split into k1-k5, and k5-k10.
// And before we `load_range` k1-k10, k1-k5 has cached some writes, say k1, in
// write_batch which means k1 cannot be loaded from snapshot. Now, `load_range`
// k1-k10 is called, and k5-k10 calls prepare_for_apply and the snapshot is
// acquired and load task of k1-k10 is scheduled. We will loss data of k1 before
// this PR.
#[test]
fn test_load_with_split2() {
    let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 4;
    cluster.run();

    cluster.must_put(b"k01", b"val");
    cluster.must_put(b"k10", b"val");

    let r = cluster.get_region(b"");
    cluster.must_split(&r, b"k05");

    fail::cfg("on_handle_put", "pause").unwrap();
    let write_req = make_write_req(&mut cluster, b"k02");
    let (cb, _) = make_cb::<HybridEngineImpl>(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();

    std::thread::sleep(Duration::from_secs(1));
    {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        core.mut_range_manager()
            .load_range(CacheRange::new(
                DATA_MIN_KEY.to_vec(),
                DATA_MAX_KEY.to_vec(),
            ))
            .unwrap();
    }

    let (tx, rx) = sync_channel(1);
    fail::cfg_callback("on_snapshot_load_finished", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    let write_req = make_write_req(&mut cluster, b"k09");
    let (cb2, _) = make_cb::<HybridEngineImpl>(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb2)
        .unwrap();
    let _ = rx.recv_timeout(Duration::from_secs(5)).unwrap();

    fail::remove("on_handle_put");
    std::thread::sleep(Duration::from_secs(1));

    let (tx, rx) = sync_channel(1);
    fail::cfg_callback("on_range_cache_get_value", move || {
        tx.send(true).unwrap();
    })
    .unwrap();
    let snap_ctx = SnapshotContext {
        read_ts: 20,
        range: None,
    };

    let _ = cluster
        .get_with_snap_ctx(b"k09", false, snap_ctx.clone())
        .unwrap();
    assert!(rx.try_recv().unwrap());

    // k1-k5 should not cached now
    let _ = cluster
        .get_with_snap_ctx(b"k02", false, snap_ctx.clone())
        .unwrap();
    rx.try_recv().unwrap_err();

    // write a key to trigger load task
    cluster.must_put(b"k03", b"val");
    let _ = cluster
        .get_with_snap_ctx(b"k02", false, snap_ctx.clone())
        .unwrap();
    assert!(rx.try_recv().unwrap());
}

fn make_write_req(
    cluster: &mut Cluster<HybridEngineImpl, NodeCluster<HybridEngineImpl>>,
    k: &[u8],
) -> RaftCmdRequest {
    let r = cluster.get_region(k);
    let mut req = new_request(
        r.get_id(),
        r.get_region_epoch().clone(),
        vec![new_put_cmd(k, b"v")],
        false,
    );
    let leader = cluster.leader_of_region(r.get_id()).unwrap();
    req.mut_header().set_peer(leader);
    req
}

#[test]
// It tests that for a apply delete, at the time it prepares to apply something,
// the range of it is in pending range. When it begins to write the write batch
// to engine, the range has finished the loading, became a normal range, and
// even been evicted.
fn test_load_with_eviction() {
    let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.run();
    // load range
    {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        // Load the whole range as if it is not splitted. Loading process should handle
        // it correctly.
        let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec());
        core.mut_range_manager().load_range(cache_range).unwrap();
    }

    let r = cluster.get_region(b"");
    cluster.must_split(&r, b"k10");

    fail::cfg("on_write_impl", "pause").unwrap();
    let write_req = make_write_req(&mut cluster, b"k01");
    let (cb, mut cb_rx) = make_cb::<HybridEngineImpl>(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();

    let write_req = make_write_req(&mut cluster, b"k15");
    let (cb, mut cb_rx2) = make_cb::<HybridEngineImpl>(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();

    {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut tried_count = 0;
        while range_cache_engine
            .snapshot(
                CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec()),
                u64::MAX,
                u64::MAX,
            )
            .is_err()
            && tried_count < 5
        {
            std::thread::sleep(Duration::from_millis(100));
            tried_count += 1;
        }
        // Now, the range (DATA_MIN_KEY, DATA_MAX_KEY) should be cached
        let range = CacheRange::new(data_key(b"k10"), DATA_MAX_KEY.to_vec());
        range_cache_engine.evict_range(&range);
    }

    fail::remove("on_write_impl");
    let _ = cb_rx.recv_timeout(Duration::from_secs(5));
    let _ = cb_rx2.recv_timeout(Duration::from_secs(5));

    let (tx, rx) = sync_channel(1);
    fail::cfg_callback("on_range_cache_get_value", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    let snap_ctx = SnapshotContext {
        read_ts: u64::MAX,
        range: None,
    };
    let val = cluster
        .get_cf_with_snap_ctx(CF_DEFAULT, b"k01", false, snap_ctx.clone())
        .unwrap();
    assert_eq!(&val, b"v");
    assert!(rx.try_recv().unwrap());

    let val = cluster
        .get_cf_with_snap_ctx(CF_DEFAULT, b"k15", false, snap_ctx.clone())
        .unwrap();
    assert_eq!(&val, b"v");
    rx.try_recv().unwrap_err();
}

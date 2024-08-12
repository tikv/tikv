use std::{
    fs::File,
    io::Read,
    sync::{mpsc::sync_channel, Arc, Mutex},
    time::Duration,
};

use engine_rocks::RocksSstWriterBuilder;
use engine_traits::{
    CacheRange, EvictReason, RangeCacheEngine, SnapshotContext, SstWriter, SstWriterBuilder,
    CF_DEFAULT, CF_WRITE,
};
use file_system::calc_crc32_bytes;
use keys::{data_key, DATA_MAX_KEY, DATA_MIN_KEY};
use kvproto::{
    import_sstpb::SstMeta,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request},
};
use tempfile::tempdir;
use test_raftstore::{
    make_cb, new_node_cluster_with_hybrid_engine_with_no_range_cache, new_peer, new_put_cmd,
    new_request, Cluster, HybridEngineImpl, NodeCluster, Simulator,
};
use tikv_util::HandyRwLock;
use txn_types::Key;

fn new_region<T1: Into<Vec<u8>>, T2: Into<Vec<u8>>>(
    id: u64,
    start_key: T1,
    end_key: T2,
) -> kvproto::metapb::Region {
    let mut region = kvproto::metapb::Region::new();
    region.id = id;
    region.start_key = start_key.into();
    region.end_key = end_key.into();
    region
}

#[test]
fn test_basic_put_get() {
    let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 1;
    cluster.run();

    let range_cache_engine = cluster.get_range_cache_engine(1);
    // FIXME: load is not implemented, so we have to insert range manually
    {
        let mut core = range_cache_engine.core().write();
        let cache_region = new_region(1, DATA_MIN_KEY, DATA_MAX_KEY);
        core.mut_range_manager().new_region(cache_region.clone());
        core.mut_range_manager()
            .set_safe_point(cache_region.id, 1000);
    }

    cluster.put(b"k05", b"val").unwrap();
    let snap_ctx = SnapshotContext {
        region_id: 0,
        epoch_version: 0,
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
                let mut cache_region = new_region(1, DATA_MIN_KEY, DATA_MAX_KEY);
                core.mut_range_manager().load_region(cache_region).unwrap();
            } else {
                let cache_range = new_region(1, DATA_MIN_KEY, &split_key1);
                let cache_range2 = new_region(2, data_key(&split_key1), data_key(&split_key2));
                let cache_range3 = new_region(3, data_key(&split_key2), DATA_MAX_KEY.to_vec());
                core.mut_range_manager().load_region(cache_range).unwrap();
                core.mut_range_manager().load_region(cache_range2).unwrap();
                core.mut_range_manager().load_region(cache_range3).unwrap();
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
            region_id: 0,
            epoch_version: 0,
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
        let cache_range = new_region(1, DATA_MIN_KEY, DATA_MAX_KEY);
        core.mut_range_manager().load_region(cache_range).unwrap();
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
    fail::cfg_callback("pending_range_completes_loading", move || {
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
        region_id: 0,
        epoch_version: 0,
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
        let cache_range = new_region(1, DATA_MIN_KEY, DATA_MAX_KEY);
        core.mut_range_manager().load_region(cache_range).unwrap();
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
        region_id: 0,
        epoch_version: 0,
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
// And before we `load_region` k1-k10, k1-k5 has cached some writes, say k1, in
// write_batch which means k1 cannot be loaded from snapshot. Now, `load_region`
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
            .load_region(new_region(1, DATA_MIN_KEY, DATA_MAX_KEY))
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
        region_id: 0,
        epoch_version: 0,
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
        let cache_range = new_region(1, DATA_MIN_KEY, DATA_MAX_KEY);
        core.mut_range_manager().load_region(cache_range).unwrap();
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
            .snapshot(1, 0, u64::MAX, u64::MAX)
            .is_err()
            && tried_count < 5
        {
            std::thread::sleep(Duration::from_millis(100));
            tried_count += 1;
        }
        // Now, the range (DATA_MIN_KEY, DATA_MAX_KEY) should be cached
        let region = new_region(1, b"k10", DATA_MAX_KEY);
        range_cache_engine.evict_range(&region, EvictReason::AutoEvict);
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
        region_id: 0,
        epoch_version: 0,
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

#[test]
fn test_evictions_after_transfer_leader() {
    let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 2);
    cluster.run();

    let r = cluster.get_region(b"");
    cluster.must_transfer_leader(r.id, new_peer(1, 1));

    let cache_region = new_region(1, DATA_MIN_KEY, DATA_MAX_KEY);
    let range_cache_engine = {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        core.mut_range_manager().new_region(cache_region.clone());
        drop(core);
        range_cache_engine
    };

    range_cache_engine
        .snapshot(
            cache_region.id,
            0,
            CacheRange::from_region(&cache_region),
            100,
            100,
        )
        .unwrap();

    cluster.must_transfer_leader(r.id, new_peer(2, 2));
    range_cache_engine
        .snapshot(
            cache_region.id,
            0,
            CacheRange::from_region(&cache_region),
            100,
            100,
        )
        .unwrap_err();
}

#[test]
fn test_eviction_after_merge() {
    let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.run();
    let r = cluster.get_region(b"");
    cluster.must_split(&r, b"key1");

    let r = cluster.get_region(b"");
    let range1 = CacheRange::from_region(&r);
    let r2 = cluster.get_region(b"key1");
    let range2 = CacheRange::from_region(&r2);

    let range_cache_engine = {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        core.mut_range_manager().new_range(range1.clone());
        core.mut_range_manager().new_range(range2.clone());
        drop(core);
        range_cache_engine
    };

    range_cache_engine
        .snapshot(range1.clone(), 100, 100)
        .unwrap();
    range_cache_engine
        .snapshot(range2.clone(), 100, 100)
        .unwrap();

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.must_merge(r.get_id(), r2.get_id());

    range_cache_engine.snapshot(range1, 100, 100).unwrap_err();
    range_cache_engine.snapshot(range2, 100, 100).unwrap_err();
}

#[test]
fn test_eviction_after_ingest_sst() {
    let mut cluster = new_node_cluster_with_hybrid_engine_with_no_range_cache(0, 1);
    cluster.run();

    // Generate a sst file.
    let tmp_dir = tempdir().unwrap();
    let sst_file_path = tmp_dir.path().join("test.sst");
    let mut writer = RocksSstWriterBuilder::new()
        .build(sst_file_path.to_str().unwrap())
        .unwrap();
    writer.put(&data_key(b"key"), b"value").unwrap();
    writer.finish().unwrap();

    // Add region r to cache.
    let region = cluster.get_region(b"");
    let range = CacheRange::from_region(&region);
    let range_cache_engine = {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write();
        core.mut_range_manager().new_range(range.clone());
        drop(core);
        range_cache_engine
    };

    range_cache_engine
        .snapshot(range.clone(), 100, 100)
        .unwrap();

    // Ingest the sst file.
    //
    // Build sst meta.
    let mut sst_meta = SstMeta::default();
    sst_meta.region_id = region.get_id();
    sst_meta.set_region_epoch(region.get_region_epoch().clone());
    sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    let mut content = vec![];
    let mut sst_file = File::open(&sst_file_path).unwrap();
    sst_file.read_to_end(&mut content).unwrap();
    sst_meta.crc32 = calc_crc32_bytes(&content);
    sst_meta.length = content.len() as _;
    sst_meta.cf_name = CF_DEFAULT.to_owned();

    // Prepare ingest.
    let importer = cluster.sim.rl().get_importer(1).unwrap();
    let mut f = importer.create(&sst_meta).unwrap();
    f.append(&content).unwrap();
    f.finish().unwrap();

    // Make ingest command.
    let mut ingest = Request::default();
    ingest.set_cmd_type(CmdType::IngestSst);
    ingest.mut_ingest_sst().set_sst(sst_meta.clone());
    let mut header = RaftRequestHeader::default();
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    header.set_peer(leader);
    header.set_region_id(region.get_id());
    header.set_region_epoch(region.get_region_epoch().clone());
    let mut cmd = RaftCmdRequest::default();
    cmd.set_header(header);
    cmd.mut_requests().push(ingest);
    let resp = cluster
        .call_command_on_leader(cmd, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    range_cache_engine.snapshot(range, 100, 100).unwrap_err();
}

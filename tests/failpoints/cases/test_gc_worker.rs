// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::Duration,
};

use collections::HashMap;
use engine_traits::Peekable;
use grpcio::{ChannelBuilder, Environment};
use keys::data_key;
use kvproto::{kvrpcpb::*, metapb::Region, tikvpb::TikvClient};
use raftstore::coprocessor::{
    RegionInfo, RegionInfoCallback, RegionInfoProvider, Result as CopResult, SeekRegionCallback,
};
use test_raftstore::*;
use tikv::{
    server::gc_worker::{
        AutoGcConfig, GcSafePointProvider, GcTask, Result as GcWorkerResult, TestGCRunner,
    },
    storage::{
        kv::TestEngineBuilder,
        mvcc::tests::must_get_none,
        txn::tests::{must_commit, must_prewrite_delete, must_prewrite_put},
    },
};
use tikv_util::HandyRwLock;
use txn_types::{Key, TimeStamp};

// In theory, raft can propose conf change as long as there is no pending one. Replicas
// don't apply logs synchronously, so it's possible the old leader is removed before the new
// leader applies all logs.
// In the current implementation, the new leader rejects conf change until it applies all logs.
// It guarantees the correctness of green GC. This test is to prevent breaking it in the
// future.
#[test]
fn test_collect_lock_from_stale_leader() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    let leader = cluster.leader_of_region(region_id).unwrap();

    // Create clients.
    let env = Arc::new(Environment::new(1));
    let mut clients = HashMap::default();
    for node_id in cluster.get_node_ids() {
        let channel =
            ChannelBuilder::new(Arc::clone(&env)).connect(&cluster.sim.rl().get_addr(node_id));
        let client = TikvClient::new(channel);
        clients.insert(node_id, client);
    }

    // Start transferring the region to store 2.
    let new_peer = new_peer(2, 1003);
    cluster.pd_client.must_add_peer(region_id, new_peer.clone());

    // Create the ctx of the first region.
    let leader_client = clients.get(&leader.get_store_id()).unwrap();
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(cluster.get_region_epoch(region_id));

    // Pause the new peer applying so that when it becomes the leader, it doesn't apply all logs.
    let new_leader_apply_fp = "on_handle_apply_1003";
    fail::cfg(new_leader_apply_fp, "pause").unwrap();
    must_kv_prewrite(
        leader_client,
        ctx,
        vec![new_mutation(Op::Put, b"k1", b"v")],
        b"k1".to_vec(),
        10,
    );

    // Leader election only considers the progress of appending logs, so it can succeed.
    cluster.must_transfer_leader(region_id, new_peer.clone());
    // It shouldn't succeed in the current implementation.
    cluster.pd_client.remove_peer(region_id, leader.clone());
    std::thread::sleep(Duration::from_secs(1));
    cluster.pd_client.must_have_peer(region_id, leader);

    // Must scan the lock from the old leader.
    let locks = must_physical_scan_lock(leader_client, Context::default(), 100, b"", 10);
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get_key(), b"k1");

    // Can't scan the lock from the new leader.
    let leader_client = clients.get(&new_peer.get_store_id()).unwrap();
    must_register_lock_observer(leader_client, 100);
    let locks = must_check_lock_observer(leader_client, 100, true);
    assert!(locks.is_empty());
    let locks = must_physical_scan_lock(leader_client, Context::default(), 100, b"", 10);
    assert!(locks.is_empty());

    fail::remove(new_leader_apply_fp);
}

#[test]
fn test_observer_send_error() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let max_ts = 100;
    must_register_lock_observer(&client, max_ts);
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![new_mutation(Op::Put, b"k1", b"v")],
        b"k1".to_vec(),
        10,
    );
    assert_eq!(must_check_lock_observer(&client, max_ts, true).len(), 1);

    let observer_send_fp = "lock_observer_send_full";
    fail::cfg(observer_send_fp, "return").unwrap();
    must_kv_prewrite(
        &client,
        ctx,
        vec![new_mutation(Op::Put, b"k2", b"v")],
        b"k1".to_vec(),
        10,
    );
    let resp = check_lock_observer(&client, max_ts);
    assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
    // Should mark dirty if fails to send locks.
    assert!(!resp.get_is_clean());
}

#[test]
fn test_notify_observer_after_apply() {
    fn retry_until(mut f: impl FnMut() -> bool) {
        for _ in 0..100 {
            sleep_ms(10);
            if f() {
                break;
            }
        }
    }

    let (mut cluster, client, ctx) = must_new_cluster_and_kv_client();
    cluster.pd_client.disable_default_operator();
    let post_apply_query_fp = "notify_lock_observer_query";
    let apply_plain_kvs_fp = "notify_lock_observer_snapshot";

    // Write a lock and pause before notifying the lock observer.
    let max_ts = 100;
    must_register_lock_observer(&client, max_ts);
    fail::cfg(post_apply_query_fp, "pause").unwrap();
    let key = b"k";
    let (client_clone, ctx_clone) = (client.clone(), ctx.clone());
    let handle = std::thread::spawn(move || {
        must_kv_prewrite(
            &client_clone,
            ctx_clone,
            vec![new_mutation(Op::Put, key, b"v")],
            key.to_vec(),
            10,
        );
    });
    // We can use physical_scan_lock to get the lock because we notify the lock observer after writing data to the rocskdb.
    let mut locks = vec![];
    retry_until(|| {
        assert!(must_check_lock_observer(&client, max_ts, true).is_empty());
        locks.extend(must_physical_scan_lock(
            &client,
            ctx.clone(),
            max_ts,
            b"",
            100,
        ));
        !locks.is_empty()
    });
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get_key(), key);
    assert!(must_check_lock_observer(&client, max_ts, true).is_empty());
    fail::remove(post_apply_query_fp);
    handle.join().unwrap();
    assert_eq!(must_check_lock_observer(&client, max_ts, true).len(), 1);

    // Add a new store.
    let store_id = cluster.add_new_engine();
    let channel = ChannelBuilder::new(Arc::new(Environment::new(1)))
        .connect(&cluster.sim.rl().get_addr(store_id));
    let replica_client = TikvClient::new(channel);

    // Add a new peer and pause before notifying the lock observer.
    must_register_lock_observer(&replica_client, max_ts);
    fail::cfg(apply_plain_kvs_fp, "pause").unwrap();
    cluster
        .pd_client
        .must_add_peer(ctx.get_region_id(), new_peer(store_id, store_id));
    // We can use physical_scan_lock to get the lock because we notify the lock observer after writing data to the rocksdb.
    let mut locks = vec![];
    retry_until(|| {
        assert!(must_check_lock_observer(&replica_client, max_ts, true).is_empty());
        locks.extend(must_physical_scan_lock(
            &replica_client,
            ctx.clone(),
            max_ts,
            b"",
            100,
        ));
        !locks.is_empty()
    });
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get_key(), key);
    assert!(must_check_lock_observer(&replica_client, max_ts, true).is_empty());
    fail::remove(apply_plain_kvs_fp);
    retry_until(|| !must_check_lock_observer(&replica_client, max_ts, true).is_empty());
    assert_eq!(
        must_check_lock_observer(&replica_client, max_ts, true).len(),
        1
    );
}

// It may cause locks missing during green GC if the raftstore notifies the lock observer before writing data to the rocksdb:
//   1. Store-1 transfers a region to store-2 and store-2 is applying logs.
//   2. GC worker registers lock observer on store-2 after calling lock observer's callback and before finishing applying which means the lock won't be observed.
//   3. GC worker scans locks on each store independently. It's possible GC worker has scanned all locks on store-2 and hasn't scanned locks on store-1.
//   4. Store-2 applies all logs and removes the peer on store-1.
//   5. GC worker can't scan the lock on store-1 because the peer has been destroyed.
//   6. GC worker can't get the lock from store-2 because it can't observe the lock and has scanned it.
#[test]
fn test_collect_applying_locks() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    let leader = cluster.leader_of_region(region_id).unwrap();

    // Create clients.
    let env = Arc::new(Environment::new(1));
    let mut clients = HashMap::default();
    for node_id in cluster.get_node_ids() {
        let channel =
            ChannelBuilder::new(Arc::clone(&env)).connect(&cluster.sim.rl().get_addr(node_id));
        let client = TikvClient::new(channel);
        clients.insert(node_id, client);
    }

    // Start transferring the region to store 2.
    let new_peer = new_peer(2, 1003);
    cluster.pd_client.must_add_peer(region_id, new_peer.clone());

    // Create the ctx of the first region.
    let store_1_client = clients.get(&leader.get_store_id()).unwrap();
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(cluster.get_region_epoch(region_id));

    // Pause store-2 after calling observer callbacks and before writing to the rocksdb.
    let new_leader_apply_fp = "post_handle_apply_1003";
    fail::cfg(new_leader_apply_fp, "pause").unwrap();

    // Write 1 lock.
    must_kv_prewrite(
        store_1_client,
        ctx,
        vec![new_mutation(Op::Put, b"k1", b"v")],
        b"k1".to_vec(),
        10,
    );
    // Wait for store-2 applying.
    std::thread::sleep(Duration::from_secs(3));

    // Starting the process of green GC at safe point 20:
    //   1. Register lock observers on all stores.
    //   2. Scan locks physically on each store independently.
    //   3. Get locks from all observers.
    let safe_point = 20;

    // Register lock observers.
    clients.iter().for_each(|(_, c)| {
        must_register_lock_observer(c, safe_point);
    });

    // Finish scanning locks on store-2 and find nothing.
    let store_2_client = clients.get(&new_peer.get_store_id()).unwrap();
    let locks = must_physical_scan_lock(store_2_client, Context::default(), safe_point, b"", 1);
    assert!(locks.is_empty(), "{:?}", locks);

    // Transfer the region from store-1 to store-2.
    fail::remove(new_leader_apply_fp);
    cluster.must_transfer_leader(region_id, new_peer);
    cluster.pd_client.must_remove_peer(region_id, leader);
    // Wait for store-1 desroying the region.
    std::thread::sleep(Duration::from_secs(3));

    // Scan locks on store-1 after the region has been destroyed.
    let locks = must_physical_scan_lock(store_1_client, Context::default(), safe_point, b"", 1);
    assert!(locks.is_empty(), "{:?}", locks);

    // Check lock observers.
    let mut locks = vec![];
    clients.iter().for_each(|(_, c)| {
        locks.extend(must_check_lock_observer(c, safe_point, true));
    });
    // Must observe the applying lock even through we can't use scan to get it.
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get_key(), b"k1");
}

// Test write CF's compaction filter can call `orphan_versions_handler` correctly.
#[test]
fn test_error_in_compaction_filter() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let raw_engine = engine.get_rocksdb();

    let large_value = vec![b'x'; 300];
    must_prewrite_put(&engine, b"zkey", &large_value, b"zkey", 101);
    must_commit(&engine, b"zkey", 101, 102);
    must_prewrite_put(&engine, b"zkey", &large_value, b"zkey", 103);
    must_commit(&engine, b"zkey", 103, 104);
    must_prewrite_delete(&engine, b"zkey", b"zkey", 105);
    must_commit(&engine, b"zkey", 105, 106);

    let fp = "write_compaction_filter_flush_write_batch";
    fail::cfg(fp, "return").unwrap();

    let mut gc_runner = TestGCRunner::new(200);
    gc_runner.gc(&raw_engine);

    match gc_runner.gc_receiver.recv().unwrap() {
        GcTask::OrphanVersions { wb, .. } => assert_eq!(wb.as_inner().count(), 2),
        GcTask::GcKeys { .. } => {}
        _ => unreachable!(),
    }

    // Although versions on default CF is not cleaned, write CF is GCed correctly.
    must_get_none(&engine, b"zkey", 102);
    must_get_none(&engine, b"zkey", 104);

    fail::remove(fp);
}

// Test GC worker can receive and handle orphan versions emit from write CF's compaction filter
// correctly.
#[test]
fn test_orphan_versions_from_compaction_filter() {
    let (cluster, leader, ctx) = must_new_and_configure_cluster(|cluster| {
        cluster.cfg.gc.enable_compaction_filter = true;
        cluster.cfg.gc.compaction_filter_skip_version_check = true;
        cluster.pd_client.disable_default_operator();
    });

    let env = Arc::new(Environment::new(1));
    let leader_store = leader.get_store_id();
    let channel = ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader_store));
    let client = TikvClient::new(channel);

    init_compaction_filter(&cluster, leader_store);
    let engine = cluster.engines.get(&leader_store).unwrap();

    let pk = b"k1".to_vec();
    let large_value = vec![b'x'; 300];
    for &start_ts in &[10, 20, 30, 40] {
        let commit_ts = start_ts + 5;
        let op = if start_ts < 40 { Op::Put } else { Op::Del };
        let muts = vec![new_mutation(op, b"k1", &large_value)];
        must_kv_prewrite(&client, ctx.clone(), muts, pk.clone(), start_ts);
        let keys = vec![pk.clone()];
        must_kv_commit(&client, ctx.clone(), keys, start_ts, commit_ts, commit_ts);
        if start_ts < 40 {
            let key = Key::from_raw(b"k1").append_ts(start_ts.into());
            let key = data_key(key.as_encoded());
            assert!(engine.kv.get_value(&key).unwrap().is_some());
        }
    }

    let fp = "write_compaction_filter_flush_write_batch";
    fail::cfg(fp, "return").unwrap();

    let mut gc_runner = TestGCRunner::new(100);
    gc_runner.gc_scheduler = cluster.sim.rl().get_gc_worker(1).scheduler();
    gc_runner.gc(&engine.kv);

    'IterKeys: for &start_ts in &[10, 20, 30] {
        let key = Key::from_raw(b"k1").append_ts(start_ts.into());
        let key = data_key(key.as_encoded());
        for _ in 0..100 {
            if engine.kv.get_value(&key).unwrap().is_some() {
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            continue 'IterKeys;
        }
        panic!("orphan versions should already been cleaned by GC worker");
    }

    fail::remove(fp);
}

// Call `start_auto_gc` like `cmd/src/server.rs` does. It will combine compaction filter and GC
// worker so that GC worker can help to process orphan versions on default CF.
fn init_compaction_filter(cluster: &Cluster<ServerCluster>, store_id: u64) {
    #[derive(Clone)]
    struct MockSafePointProvider;
    impl GcSafePointProvider for MockSafePointProvider {
        fn get_safe_point(&self) -> GcWorkerResult<TimeStamp> {
            Ok(TimeStamp::from(0))
        }
    }

    #[derive(Clone)]
    struct MockRegionInfoProvider;
    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(&self, _: &[u8], _: SeekRegionCallback) -> CopResult<()> {
            Ok(())
        }
        fn find_region_by_id(
            &self,
            _: u64,
            _: RegionInfoCallback<Option<RegionInfo>>,
        ) -> CopResult<()> {
            Ok(())
        }
        fn get_regions_in_range(
            &self,
            _start_key: &[u8],
            _end_key: &[u8],
        ) -> CopResult<Vec<Region>> {
            Ok(vec![])
        }
    }

    let sim = cluster.sim.rl();
    let gc_worker = sim.get_gc_worker(store_id);
    gc_worker
        .start_auto_gc(
            AutoGcConfig::new(MockSafePointProvider, MockRegionInfoProvider, 1),
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();
}

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::*;
use kvproto::tikvpb::TikvClient;
use test_raftstore::*;
use tikv::storage::kv::SnapContext;
use tikv_util::HandyRwLock;
use txn_types::{Key, PessimisticLock};

/// When a follower applies log slowly, leader should not transfer leader
/// to it. Otherwise, new leader may wait a long time to serve read/write
/// requests.
#[test]
fn test_transfer_leader_slow_apply() {
    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);

    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 1002));
    pd_client.must_add_peer(r1, new_peer(3, 1003));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let fp = "on_handle_apply_1003";
    fail::cfg(fp, "pause").unwrap();
    for i in 0..=cluster.cfg.raft_store.leader_transfer_max_log_lag {
        let bytes = format!("k{:03}", i).into_bytes();
        cluster.must_put(&bytes, &bytes);
    }
    cluster.transfer_leader(r1, new_peer(3, 1003));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    assert_ne!(cluster.leader_of_region(r1).unwrap(), new_peer(3, 1003));
    fail::remove(fp);
    cluster.must_transfer_leader(r1, new_peer(3, 1003));
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
}

#[test]
fn test_prewrite_before_max_ts_is_synced() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let do_prewrite = |cluster: &mut Cluster<ServerCluster>| {
        let region_id = 1;
        let leader = cluster.leader_of_region(region_id).unwrap();
        let epoch = cluster.get_region_epoch(region_id);
        let mut ctx = Context::default();
        ctx.set_region_id(region_id);
        ctx.set_peer(leader);
        ctx.set_region_epoch(epoch);

        let mut req = PrewriteRequest::default();
        req.set_context(ctx);
        req.set_primary_lock(b"key".to_vec());
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.set_key(b"key".to_vec());
        mutation.set_value(b"value".to_vec());
        req.mut_mutations().push(mutation);
        req.set_start_version(100);
        req.set_lock_ttl(20000);
        req.set_use_async_commit(true);
        client.kv_prewrite(&req).unwrap()
    };

    cluster.must_transfer_leader(1, new_peer(2, 2));
    fail::cfg("test_raftstore_get_tso", "return(50)").unwrap();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    let resp = do_prewrite(&mut cluster);
    assert!(resp.get_region_error().has_max_timestamp_not_synced());
    fail::remove("test_raftstore_get_tso");
    thread::sleep(Duration::from_millis(200));
    let resp = do_prewrite(&mut cluster);
    assert!(!resp.get_region_error().has_max_timestamp_not_synced());
}

#[test]
fn test_locks_deleted_earlier_than_proposing_locks() {
    use tikv::storage::Engine;

    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let region_id = 1;
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let leader = cluster.leader_of_region(region_id).unwrap();
    let store_id = leader.store_id;
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(epoch);

    let storage = cluster.sim.rl().storages.get(&store_id).unwrap().clone();
    let snap_ctx = SnapContext {
        pb_ctx: &ctx,
        ..Default::default()
    };
    let txn_ext = storage.snapshot(snap_ctx).unwrap().txn_ext.unwrap();
    txn_ext.pessimistic_locks.write().map.insert(
        Key::from_raw(b"key"),
        PessimisticLock {
            primary: b"key".to_vec().into_boxed_slice(),
            start_ts: 10.into(),
            ttl: 1000,
            for_update_ts: 10.into(),
            min_commit_ts: 20.into(),
        },
    );

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut req = CleanupRequest::default();
    req.set_context(ctx);
    req.set_key(b"key".to_vec());
    req.set_start_version(10);
    req.set_current_ts(u64::MAX);

    // Pause the command after it deletes memory locks
    fail::cfg("raftkv_async_write", "pause").unwrap();
    let (tx, resp_rx) = mpsc::channel();
    thread::spawn(move || tx.send(client.kv_cleanup(&req).unwrap()).unwrap());

    thread::sleep(Duration::from_millis(200));
    assert!(resp_rx.try_recv().is_err());
    assert!(txn_ext.pessimistic_locks.read().map.is_empty());

    cluster.transfer_leader(1, new_peer(2, 2));
    thread::sleep(Duration::from_millis(200));

    // Transfer leader will not make the command fail.
    fail::remove("raftkv_async_write");
    let resp = resp_rx.recv().unwrap();
    assert!(!resp.has_region_error());

    for _ in 0..10 {
        thread::sleep(Duration::from_millis(100));
        cluster.reset_leader_of_region(region_id);
        if cluster.leader_of_region(region_id).unwrap().id == 2 {
            return;
        }
    }
    panic!("region should succeed to transfer leader to peer 2");
}

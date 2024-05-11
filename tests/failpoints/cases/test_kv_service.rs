// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use test_raftstore::{
    configure_for_lease_read, must_kv_commit, must_kv_prewrite, must_new_cluster_and_kv_client,
    must_new_cluster_mul, new_server_cluster,
};
use tikv_util::{config::ReadableDuration, HandyRwLock};

#[test]
fn test_batch_get_memory_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let mut req = BatchGetRequest::default();
    req.set_context(ctx);
    req.set_keys(vec![b"a".to_vec(), b"b".to_vec()].into());
    req.version = 50;

    fail::cfg("raftkv_async_snapshot_err", "return").unwrap();
    let resp = client.kv_batch_get(&req).unwrap();
    // the injected error should be returned at both places for backward
    // compatibility.
    assert!(!resp.pairs[0].get_error().get_abort().is_empty());
    assert!(!resp.get_error().get_abort().is_empty());
    fail::remove("raftkv_async_snapshot_err");
}

#[test]
fn test_kv_scan_memory_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let mut req = ScanRequest::default();
    req.set_context(ctx);
    req.set_start_key(b"a".to_vec());
    req.version = 50;

    fail::cfg("raftkv_async_snapshot_err", "return").unwrap();
    let resp = client.kv_scan(&req).unwrap();
    // the injected error should be returned at both places for backward
    // compatibility.
    assert!(!resp.pairs[0].get_error().get_abort().is_empty());
    assert!(!resp.get_error().get_abort().is_empty());
    fail::remove("raftkv_async_snapshot_err");
}

#[test]
fn test_snapshot_not_block_grpc() {
    let (cluster, leader, ctx) = must_new_cluster_mul(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env)
        .keepalive_time(Duration::from_millis(500))
        .keepalive_timeout(Duration::from_millis(500))
        .connect(&cluster.sim.read().unwrap().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k".to_vec());
    mutation.set_value(b"v".to_vec());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation.clone()],
        b"k".to_vec(),
        10,
    );
    // Block getting snapshot. It shouldn't trigger keepalive watchdog timeout.
    fail::cfg("after-snapshot", "sleep(2000)").unwrap();
    must_kv_prewrite(&client, ctx, vec![mutation], b"k".to_vec(), 10);
    fail::remove("after-snapshot");
}

#[test]
fn test_stale_read_on_local_leader() {
    let mut cluster = new_server_cluster(0, 1);
    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let max_lease = Duration::from_secs(2);
    cluster.cfg.tikv.raft_store.raft_store_max_leader_lease = ReadableDuration(max_lease);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(epoch);
    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let v1 = b"value1".to_vec();

    // Write record.
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v.clone());
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), 10);
    must_kv_commit(&client, ctx.clone(), vec![k.clone()], 10, 30, 30);

    // Prewrite and leave a lock.
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v1);
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), 50);

    let mut req = GetRequest::default();
    req.set_context(ctx);
    req.set_key(k);
    req.version = 40;
    req.mut_context().set_stale_read(true);

    // The stale read should fallback and succeed on the leader peer.
    let resp = client.kv_get(&req).unwrap();
    assert!(resp.error.is_none());
    assert!(resp.region_error.is_none());
    assert_eq!(v, resp.get_value());
}
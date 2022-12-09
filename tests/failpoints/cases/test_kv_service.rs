// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use test_raftstore::{must_kv_prewrite, must_new_cluster_and_kv_client, must_new_cluster_mul};

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

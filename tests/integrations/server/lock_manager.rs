// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::*;
use kvproto::metapb::{Peer, Region};
use kvproto::tikvpb::TikvClient;

use test_raftstore::*;
use tikv_util::HandyRwLock;

fn acquire_pessimistic_lock(
    client: &TikvClient,
    ctx: Context,
    key: Vec<u8>,
    ts: u64,
) -> PessimisticLockResponse {
    let mut req = PessimisticLockRequest::default();
    req.set_context(ctx);
    let mut mutation = Mutation::default();
    mutation.set_op(Op::PessimisticLock);
    mutation.set_key(key.clone());
    mutation.set_value(key.clone());
    req.set_mutations(vec![mutation].into_iter().collect());
    req.primary_lock = key;
    req.start_version = ts;
    req.for_update_ts = ts;
    req.lock_ttl = 20;
    req.is_first_lock = false;
    client.kv_pessimistic_lock(&req).unwrap()
}

fn must_acquire_pessimistic_lock(client: &TikvClient, ctx: Context, key: Vec<u8>, ts: u64) {
    let resp = acquire_pessimistic_lock(client, ctx, key, ts);
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert!(resp.errors.is_empty(), "{:?}", resp.get_errors());
}

fn delete_pessimistic_lock(
    client: &TikvClient,
    ctx: Context,
    key: Vec<u8>,
    ts: u64,
) -> PessimisticRollbackResponse {
    let mut req = PessimisticRollbackRequest::default();
    req.set_context(ctx);
    req.set_keys(vec![key].into_iter().collect());
    req.start_version = ts;
    req.for_update_ts = ts;
    client.kv_pessimistic_rollback(&req).unwrap()
}

fn must_delete_pessimistic_lock(client: &TikvClient, ctx: Context, key: Vec<u8>, ts: u64) {
    let resp = delete_pessimistic_lock(client, ctx, key, ts);
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert!(resp.errors.is_empty(), "{:?}", resp.get_errors());
}

fn must_deadlock(client: &TikvClient, ctx: Context, key1: &[u8], ts: u64) {
    let key1 = key1.to_vec();
    let mut key2 = key1.clone();
    key2.push(0);
    must_acquire_pessimistic_lock(client, ctx.clone(), key1.clone(), ts);
    must_acquire_pessimistic_lock(client, ctx.clone(), key2.clone(), ts + 1);

    let (client_clone, ctx_clone, key1_clone) = (client.clone(), ctx.clone(), key1.clone());
    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = acquire_pessimistic_lock(&client_clone, ctx_clone, key1_clone, ts + 1);
        tx.send(1).unwrap();
    });
    // Sleep to make sure txn(ts+1) is waiting for txn(ts)
    thread::sleep(Duration::from_millis(500));
    let resp = acquire_pessimistic_lock(client, ctx.clone(), key2.clone(), ts);
    assert_eq!(resp.errors.len(), 1);
    assert!(resp.errors[0].has_deadlock(), "{:?}", resp.get_errors());
    rx.recv().unwrap();

    // Clean up
    must_delete_pessimistic_lock(client, ctx.clone(), key1, ts);
    must_delete_pessimistic_lock(client, ctx, key2, ts);
}

fn build_leader_client(cluster: &mut Cluster<ServerCluster>, key: &[u8]) -> (TikvClient, Context) {
    let region_id = cluster.get_region_id(key);
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(epoch);

    (client, ctx)
}

/// Creates a deadlock on the store containing key.
fn must_detect_deadlock(cluster: &mut Cluster<ServerCluster>, key: &[u8], ts: u64) {
    let (client, ctx) = build_leader_client(cluster, key);
    must_deadlock(&client, ctx, key, ts);
}

fn deadlock_detector_leader_must_be(cluster: &mut Cluster<ServerCluster>, store_id: u64) {
    let leader_region = cluster.get_region(b"");
    assert_eq!(
        cluster
            .leader_of_region(leader_region.get_id())
            .unwrap()
            .get_store_id(),
        store_id
    );
    let leader_peer = find_peer_of_store(&leader_region, store_id);
    cluster
        .pd_client
        .region_leader_must_be(leader_region.get_id(), leader_peer);
}

fn must_transfer_leader(cluster: &mut Cluster<ServerCluster>, region_key: &[u8], store_id: u64) {
    let region = cluster.get_region(region_key);
    let target_peer = find_peer_of_store(&region, store_id);
    cluster.must_transfer_leader(region.get_id(), target_peer.clone());
    cluster
        .pd_client
        .region_leader_must_be(region.get_id(), target_peer);
    // Make sure the new leader can get snapshot locally.
    cluster.must_get(region_key);
}

/// Transfers the region containing region_key from source store to target peer.
///
/// REQUIRE: The source store must be the leader the region and the target store must not have
/// this region.
fn must_transfer_region(
    cluster: &mut Cluster<ServerCluster>,
    region_key: &[u8],
    source_store_id: u64,
    target_store_id: u64,
    target_peer_id: u64,
) {
    let target_peer = new_peer(target_store_id, target_peer_id);
    let region = cluster.get_region(region_key);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), target_peer);
    must_transfer_leader(cluster, region_key, target_store_id);
    let source_peer = find_peer_of_store(&region, source_store_id);
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), source_peer);
}

fn must_merge_region(
    cluster: &mut Cluster<ServerCluster>,
    source_region_key: &[u8],
    target_region_key: &[u8],
) {
    let (source_id, target_id) = (
        cluster.get_region(source_region_key).get_id(),
        cluster.get_region(target_region_key).get_id(),
    );
    cluster.pd_client.must_merge(source_id, target_id);
}

fn find_peer_of_store(region: &Region, store_id: u64) -> Peer {
    region
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .unwrap()
        .clone()
}

/// Creates a cluster with only one region and store(1) is the leader of the region.
fn new_cluster_for_deadlock_test(count: usize) -> Cluster<ServerCluster> {
    let mut cluster = new_server_cluster(0, count);
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    // Region 1 has 3 peers. And peer(1, 1) is the leader of region 1.
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    deadlock_detector_leader_must_be(&mut cluster, 1);
    must_detect_deadlock(&mut cluster, b"k", 10);
    cluster
}

#[test]
fn test_detect_deadlock_when_transfer_leader() {
    let mut cluster = new_cluster_for_deadlock_test(3);
    // Transfer the leader of region 1 to store(2).
    // The leader of deadlock detector should also be transfered to store(2).
    must_transfer_leader(&mut cluster, b"", 2);
    deadlock_detector_leader_must_be(&mut cluster, 2);
    must_detect_deadlock(&mut cluster, b"k", 10);
}

#[test]
fn test_detect_deadlock_when_split_region() {
    let mut cluster = new_cluster_for_deadlock_test(3);
    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k1");
    // After split, the leader is still store(1).
    deadlock_detector_leader_must_be(&mut cluster, 1);
    must_detect_deadlock(&mut cluster, b"k", 10);
    // Transfer the new region's leader to store(2) and deadlock occours on it.
    must_transfer_leader(&mut cluster, b"k1", 2);
    deadlock_detector_leader_must_be(&mut cluster, 1);
    must_detect_deadlock(&mut cluster, b"k1", 10);
}

#[test]
fn test_detect_deadlock_when_transfer_region() {
    let mut cluster = new_cluster_for_deadlock_test(4);
    // Transfer the leader region to store(4) and the leader of deadlock detector should be
    // also transfered.
    must_transfer_region(&mut cluster, b"k", 1, 4, 4);
    deadlock_detector_leader_must_be(&mut cluster, 4);
    must_detect_deadlock(&mut cluster, b"k", 10);

    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k1");
    // Transfer the new region to store(1). It shouldn't affect deadlock detector.
    must_transfer_region(&mut cluster, b"k1", 4, 1, 5);
    deadlock_detector_leader_must_be(&mut cluster, 4);
    must_detect_deadlock(&mut cluster, b"k", 10);
    must_detect_deadlock(&mut cluster, b"k1", 10);

    // Transfer the new region back to store(4) which will send a role change message with empty
    // key range. It shouldn't affect deadlock detector.
    must_transfer_region(&mut cluster, b"k1", 1, 4, 6);
    deadlock_detector_leader_must_be(&mut cluster, 4);
    must_detect_deadlock(&mut cluster, b"k", 10);
    must_detect_deadlock(&mut cluster, b"k1", 10);
}

#[test]
fn test_detect_deadlock_when_merge_region() {
    let mut cluster = new_cluster_for_deadlock_test(3);

    // Source region will be destroyed.
    for as_target in &[false, true] {
        let region = cluster.get_region(b"");
        cluster.must_split(&region, b"k1");
        if *as_target {
            must_merge_region(&mut cluster, b"k1", b"");
        } else {
            must_merge_region(&mut cluster, b"", b"k1");
        }
        deadlock_detector_leader_must_be(&mut cluster, 1);
        must_detect_deadlock(&mut cluster, b"k", 10);
    }

    // Leaders of two regions are on different store.
    for as_target in &[false, true] {
        let region = cluster.get_region(b"");
        cluster.must_split(&region, b"k1");
        must_transfer_leader(&mut cluster, b"k1", 2);
        if *as_target {
            must_merge_region(&mut cluster, b"k1", b"");
            deadlock_detector_leader_must_be(&mut cluster, 1);
        } else {
            must_merge_region(&mut cluster, b"", b"k1");
            deadlock_detector_leader_must_be(&mut cluster, 2);
        }
        must_detect_deadlock(&mut cluster, b"k", 10);
        must_transfer_leader(&mut cluster, b"", 1);
    }
}

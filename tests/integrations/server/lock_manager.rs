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

fn must_deadlock(client: &TikvClient, ctx: Context, key1: &[u8], ts: u64) {
    let key1 = key1.to_vec();
    let mut key2 = key1.clone();
    key2.push(1);
    must_acquire_pessimistic_lock(client, ctx.clone(), key1.clone(), ts);
    must_acquire_pessimistic_lock(client, ctx.clone(), key2.clone(), ts + 1);

    let client1 = client.clone();
    let ctx1 = ctx.clone();
    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = acquire_pessimistic_lock(&client1, ctx1, key1, ts + 1);
        tx.send(1).unwrap();
    });
    // Sleep to make sure txn(ts+1) is waiting for txn(ts)
    thread::sleep(Duration::from_millis(500));
    let resp = acquire_pessimistic_lock(client, ctx, key2, ts);
    assert_eq!(resp.errors.len(), 1);
    assert!(resp.errors[0].has_deadlock());
    rx.recv().unwrap();
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
}

fn find_peer_of_store(region: &Region, store_id: u64) -> Peer {
    region
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .unwrap()
        .clone()
}

#[test]
fn test_detect_deadlock_when_shuffle_region() {
    let mut cluster = new_server_cluster(0, 4);
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    // Region 1 has 3 peers. And peer(1, 1) is the leader of region 1.
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));

    // The leader of deadlock detector is the leader of region 1.
    deadlock_detector_leader_must_be(&mut cluster, 1);
    let (client, ctx) = build_leader_client(&mut cluster, b"k1");
    must_deadlock(&client, ctx, b"k1", 10);

    // The leader of region 1 has transfered. The leader of deadlock detector should also transfer.
    must_transfer_leader(&mut cluster, b"", 2);
    deadlock_detector_leader_must_be(&mut cluster, 2);
    let (client, ctx) = build_leader_client(&mut cluster, b"k2");
    must_deadlock(&client, ctx, b"k2", 20);

    // Split region and transfer the leader of new region to store(3).
    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k10");
    must_transfer_leader(&mut cluster, b"k10", 3);
    // Deadlock occurs on store(3) and the leader of deadlock detector is store(2).
    deadlock_detector_leader_must_be(&mut cluster, 2);
    let (client, ctx) = build_leader_client(&mut cluster, b"k10");
    must_deadlock(&client, ctx, b"k10", 30);

    // Transfer the new region from store(1, 2, 3) to store(2, 3, 4).
    let new_region = cluster.get_region(b"k10");
    pd_client.must_add_peer(new_region.get_id(), new_peer(4, 4));
    must_transfer_leader(&mut cluster, b"k10", 4);
    let peer = find_peer_of_store(&new_region, 1);
    pd_client.must_remove_peer(region_id, peer);

    // Transfer the leader of deadlock detector to store(1) and
    // deadlock occours on store(4) again.
    must_transfer_leader(&mut cluster, b"", 1);
    deadlock_detector_leader_must_be(&mut cluster, 1);
    let (client, ctx) = build_leader_client(&mut cluster, b"k11");
    must_deadlock(&client, ctx, b"k11", 30);

    // Add store(1) back again which will send a role change message with empty region key range to
    // the deadlock detector. It misleads the leader of deadlock detector stepping down.
    pd_client.must_add_peer(new_region.get_id(), new_peer(1, 5));
    deadlock_detector_leader_must_be(&mut cluster, 1);
    let (client, ctx) = build_leader_client(&mut cluster, b"k3");
    must_deadlock(&client, ctx, b"k3", 10);
}

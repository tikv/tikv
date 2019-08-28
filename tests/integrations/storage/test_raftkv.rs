// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time;

use kvproto::kvrpcpb::Context;
use raft::eraftpb::MessageType;

use engine::IterOption;
use engine::{CfName, CF_DEFAULT};
use test_raftstore::*;
use tikv::storage::kv::*;
use tikv::storage::{CFStatistics, Key};
use tikv_util::codec::bytes;
use tikv_util::HandyRwLock;

#[test]
fn test_raftkv() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b"k1"), None);

    let region = cluster.get_region(b"");
    let leader_id = cluster.leader_of_region(region.get_id()).unwrap();
    let storage = cluster.sim.rl().storages[&leader_id.get_id()].clone();

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(region.get_peers()[0].clone());

    get_put(&ctx, &storage);
    batch(&ctx, &storage);
    seek(&ctx, &storage);
    near_seek(&ctx, &storage);
    cf(&ctx, &storage);
    empty_write(&ctx, &storage);
    wrong_context(&ctx, &storage);
    // TODO: test multiple node
}

#[test]
fn test_read_leader_in_lease() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let k1 = b"k1";
    let (k2, v2) = (b"k2", b"v2");

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(k1), None);

    let region = cluster.get_region(b"");
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let storage = cluster.sim.rl().storages[&leader.get_id()].clone();

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader.clone());

    // write some data
    assert_none(&ctx, &storage, k2);
    must_put(&ctx, &storage, k2, v2);

    // isolate leader
    cluster.add_send_filter(IsolationFilterFactory::new(leader.get_store_id()));

    // leader still in lease, check if can read on leader
    assert_eq!(can_read(&ctx, &storage, k2, v2), true);
}

#[test]
fn test_read_index_on_replica() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let k1 = b"k1";
    let (k2, v2) = (b"k2", b"v2");

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(k1), None);

    let region = cluster.get_region(b"");
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let storage = cluster.sim.rl().storages[&leader.get_id()].clone();

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader.clone());

    // write some data
    let peers = region.get_peers();
    assert_none(&ctx, &storage, k2);
    must_put(&ctx, &storage, k2, v2);

    // read on follower
    let mut follower_peer = None;
    for p in peers {
        if p.get_id() != leader.get_id() {
            follower_peer = Some(p.clone());
            break;
        }
    }

    assert!(follower_peer.is_some());
    ctx.set_peer(follower_peer.as_ref().unwrap().clone());
    let resp = read_index_on_peer(
        &mut cluster,
        follower_peer.unwrap(),
        region.clone(),
        false,
        std::time::Duration::from_secs(5),
    );
    assert!(!resp.as_ref().unwrap().get_header().has_error());
    assert_ne!(
        resp.unwrap().get_responses()[0]
            .get_read_index()
            .get_read_index(),
        0
    );
}

#[test]
fn test_read_on_replica() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let k1 = b"k1";
    let (k2, v2) = (b"k2", b"v2");
    let (k3, v3) = (b"k3", b"v3");
    let (k4, v4) = (b"k4", b"v4");

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(k1), None);

    let region = cluster.get_region(b"");
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let leader_storage = cluster.sim.rl().storages[&leader.get_id()].clone();

    let mut leader_ctx = Context::default();
    leader_ctx.set_region_id(region.get_id());
    leader_ctx.set_region_epoch(region.get_region_epoch().clone());
    leader_ctx.set_peer(leader.clone());

    // write some data
    let peers = region.get_peers();
    assert_none(&leader_ctx, &leader_storage, k2);
    must_put(&leader_ctx, &leader_storage, k2, v2);

    // read on follower
    let mut follower_peer = None;
    let mut follower_id = 0;
    for p in peers {
        if p.get_id() != leader.get_id() {
            follower_id = p.get_id();
            follower_peer = Some(p.clone());
            break;
        }
    }

    assert!(follower_peer.is_some());
    let mut follower_ctx = Context::default();
    follower_ctx.set_region_id(region.get_id());
    follower_ctx.set_region_epoch(region.get_region_epoch().clone());
    follower_ctx.set_peer(follower_peer.as_ref().unwrap().clone());
    follower_ctx.set_replica_read(true);
    let follower_storage = cluster.sim.rl().storages[&follower_id].clone();
    assert_has(&follower_ctx, &follower_storage, k2, v2);

    must_put(&leader_ctx, &leader_storage, k3, v3);
    assert_has(&follower_ctx, &follower_storage, k3, v3);

    cluster.stop_node(follower_id);
    must_put(&leader_ctx, &leader_storage, k4, v4);
    cluster.run_node(follower_id).unwrap();
    let follower_storage = cluster.sim.rl().storages[&follower_id].clone();
    // sleep to ensure the follower has received a heartbeat from the leader
    thread::sleep(time::Duration::from_millis(300));
    assert_has(&follower_ctx, &follower_storage, k4, v4);
}

#[test]
fn test_invaild_read_index_when_no_leader() {
    // Initialize cluster
    let mut cluster = new_node_cluster(0, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(3));
    cluster.cfg.raft_store.raft_heartbeat_ticks = 1;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    // Set region and peers
    let r1 = cluster.run_conf_change();
    let p1 = new_peer(1, 1);
    cluster.must_put(b"k0", b"v0");
    let p2 = new_peer(2, 2);
    cluster.pd_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.pd_client.must_add_peer(r1, p3.clone());
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    // Transfer leader to p2
    let region = cluster.get_region(b"k0");
    cluster.must_transfer_leader(region.get_id(), p2.clone());

    // Delay all raft messages to p1.
    let heartbeat_filter = Box::new(
        RegionPacketFilter::new(region.get_id(), 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgHeartbeat)
            .when(Arc::new(AtomicBool::new(true))),
    );
    cluster.sim.wl().add_recv_filter(1, heartbeat_filter);
    let vote_resp_filter = Box::new(
        RegionPacketFilter::new(region.get_id(), 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgRequestVoteResponse)
            .when(Arc::new(AtomicBool::new(true))),
    );
    cluster.sim.wl().add_recv_filter(1, vote_resp_filter);

    // wait for election timeout
    thread::sleep(time::Duration::from_millis(300));
    // send read index requests to p1
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_read_index_cmd()],
        true,
    );
    request.mut_header().set_peer(p1.clone());
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request.clone(), cb)
        .unwrap();

    let resp = rx.recv_timeout(time::Duration::from_millis(500)).unwrap();
    assert!(resp
        .get_header()
        .get_error()
        .get_message()
        .contains("can not read index due to no leader"));
}

fn must_put<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) {
    engine.put(ctx, Key::from_raw(key), value.to_vec()).unwrap();
}

fn must_put_cf<E: Engine>(ctx: &Context, engine: &E, cf: CfName, key: &[u8], value: &[u8]) {
    engine
        .put_cf(ctx, cf, Key::from_raw(key), value.to_vec())
        .unwrap();
}

fn must_delete<E: Engine>(ctx: &Context, engine: &E, key: &[u8]) {
    engine.delete(ctx, Key::from_raw(key)).unwrap();
}

fn must_delete_cf<E: Engine>(ctx: &Context, engine: &E, cf: CfName, key: &[u8]) {
    engine.delete_cf(ctx, cf, Key::from_raw(key)).unwrap();
}

fn assert_has<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap().unwrap(), value);
}

fn can_read<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) -> bool {
    if let Ok(s) = engine.snapshot(ctx) {
        assert_eq!(s.get(&Key::from_raw(key)).unwrap().unwrap(), value);
        return true;
    }
    false
}

fn assert_has_cf<E: Engine>(ctx: &Context, engine: &E, cf: CfName, key: &[u8], value: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(
        snapshot.get_cf(cf, &Key::from_raw(key)).unwrap().unwrap(),
        value
    );
}

fn assert_none<E: Engine>(ctx: &Context, engine: &E, key: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap(), None);
}

fn assert_none_cf<E: Engine>(ctx: &Context, engine: &E, cf: CfName, key: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get_cf(cf, &Key::from_raw(key)).unwrap(), None);
}

fn assert_seek<E: Engine>(ctx: &Context, engine: &E, key: &[u8], pair: (&[u8], &[u8])) {
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot
        .iter(IterOption::default(), ScanMode::Mixed)
        .unwrap();
    let mut statistics = CFStatistics::default();
    cursor.seek(&Key::from_raw(key), &mut statistics).unwrap();
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn assert_seek_cf<E: Engine>(
    ctx: &Context,
    engine: &E,
    cf: CfName,
    key: &[u8],
    pair: (&[u8], &[u8]),
) {
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot
        .iter_cf(cf, IterOption::default(), ScanMode::Mixed)
        .unwrap();
    let mut statistics = CFStatistics::default();
    cursor.seek(&Key::from_raw(key), &mut statistics).unwrap();
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn assert_near_seek<I: Iterator>(cursor: &mut Cursor<I>, key: &[u8], pair: (&[u8], &[u8])) {
    let mut statistics = CFStatistics::default();
    assert!(
        cursor
            .near_seek(&Key::from_raw(key), &mut statistics)
            .unwrap(),
        hex::encode_upper(key)
    );
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn assert_near_reverse_seek<I: Iterator>(cursor: &mut Cursor<I>, key: &[u8], pair: (&[u8], &[u8])) {
    let mut statistics = CFStatistics::default();
    assert!(
        cursor
            .near_reverse_seek(&Key::from_raw(key), &mut statistics)
            .unwrap(),
        hex::encode_upper(key)
    );
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn get_put<E: Engine>(ctx: &Context, engine: &E) {
    assert_none(ctx, engine, b"x");
    must_put(ctx, engine, b"x", b"1");
    assert_has(ctx, engine, b"x", b"1");
    must_put(ctx, engine, b"x", b"2");
    assert_has(ctx, engine, b"x", b"2");
}

fn batch<E: Engine>(ctx: &Context, engine: &E) {
    engine
        .write(
            ctx,
            vec![
                Modify::Put(CF_DEFAULT, Key::from_raw(b"x"), b"1".to_vec()),
                Modify::Put(CF_DEFAULT, Key::from_raw(b"y"), b"2".to_vec()),
            ],
        )
        .unwrap();
    assert_has(ctx, engine, b"x", b"1");
    assert_has(ctx, engine, b"y", b"2");

    engine
        .write(
            ctx,
            vec![
                Modify::Delete(CF_DEFAULT, Key::from_raw(b"x")),
                Modify::Delete(CF_DEFAULT, Key::from_raw(b"y")),
            ],
        )
        .unwrap();
    assert_none(ctx, engine, b"y");
    assert_none(ctx, engine, b"y");
}

fn seek<E: Engine>(ctx: &Context, engine: &E) {
    must_put(ctx, engine, b"x", b"1");
    assert_seek(ctx, engine, b"x", (b"x", b"1"));
    assert_seek(ctx, engine, b"a", (b"x", b"1"));
    must_put(ctx, engine, b"z", b"2");
    assert_seek(ctx, engine, b"y", (b"z", b"2"));
    assert_seek(ctx, engine, b"x\x00", (b"z", b"2"));
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut iter = snapshot
        .iter(IterOption::default(), ScanMode::Mixed)
        .unwrap();
    let mut statistics = CFStatistics::default();
    assert!(!iter
        .seek(&Key::from_raw(b"z\x00"), &mut statistics)
        .unwrap());
    must_delete(ctx, engine, b"x");
    must_delete(ctx, engine, b"z");
}

fn near_seek<E: Engine>(ctx: &Context, engine: &E) {
    must_put(ctx, engine, b"x", b"1");
    must_put(ctx, engine, b"z", b"2");
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot
        .iter(IterOption::default(), ScanMode::Mixed)
        .unwrap();
    assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
    assert_near_seek(&mut cursor, b"a", (b"x", b"1"));
    assert_near_reverse_seek(&mut cursor, b"z1", (b"z", b"2"));
    assert_near_reverse_seek(&mut cursor, b"x1", (b"x", b"1"));
    assert_near_seek(&mut cursor, b"y", (b"z", b"2"));
    assert_near_seek(&mut cursor, b"x\x00", (b"z", b"2"));
    let mut statistics = CFStatistics::default();
    assert!(!cursor
        .near_seek(&Key::from_raw(b"z\x00"), &mut statistics)
        .unwrap());
    must_delete(ctx, engine, b"x");
    must_delete(ctx, engine, b"z");
}

fn cf<E: Engine>(ctx: &Context, engine: &E) {
    assert_none_cf(ctx, engine, "default", b"key");
    must_put_cf(ctx, engine, "default", b"key", b"value");
    assert_has_cf(ctx, engine, "default", b"key", b"value");
    assert_seek_cf(ctx, engine, "default", b"k", (b"key", b"value"));
    must_delete_cf(ctx, engine, "default", b"key");
    assert_none_cf(ctx, engine, "default", b"key");
}

fn empty_write<E: Engine>(ctx: &Context, engine: &E) {
    engine.write(ctx, vec![]).unwrap_err();
}

fn wrong_context<E: Engine>(ctx: &Context, engine: &E) {
    let region_id = ctx.get_region_id();
    let mut ctx = ctx.to_owned();
    ctx.set_region_id(region_id + 1);
    assert!(engine.write(&ctx, vec![]).is_err());
}

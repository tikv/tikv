// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{self, time::Duration};

use engine_traits::{Peekable, RaftEngineReadOnly, CF_DEFAULT};
use futures::executor::block_on;
use kvproto::{
    raft_cmdpb::{AdminCmdType, RaftCmdRequest},
    raft_serverpb::{PeerState, RaftMessage},
};
use raft::prelude::{ConfChangeType, MessageType};
use raftstore_v2::{
    router::{PeerMsg, PeerTick},
    SimpleWriteEncoder,
};
use tikv_util::store::{new_learner_peer, new_peer};

use crate::cluster::{check_skip_wal, Cluster};

#[test]
fn test_simple_change() {
    let mut cluster = Cluster::with_node_count(2, None);
    let (region_id, peer_id, offset_id) = (2, 10, 1);

    // 1. add learner on store-2
    add_learner(&cluster, offset_id, region_id, peer_id);
    let meta = cluster.routers[0]
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    let match_index = meta.raft_apply.applied_index;

    // 2. write one kv after snapshot
    let (key, val) = (b"key", b"value");
    write_kv(&cluster, region_id, key, val);
    let meta = cluster.routers[1]
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    // the learner truncated index muse be equal the leader applied index and can
    // read the new written kv.
    assert_eq!(match_index, meta.raft_apply.truncated_state.index);
    assert!(meta.raft_apply.applied_index >= match_index);
    let snap = cluster.routers[offset_id].stale_snapshot(region_id);
    assert_eq!(snap.get_value(key).unwrap().unwrap(), val);
    // 3. remove peer from store-2
    remove_peer(&cluster, offset_id, region_id, peer_id);

    // To avaid that some status doesn't clear after destroying, it can support to
    // create peer by many times.
    let repeat = 3;
    for i in 1..repeat {
        add_learner(&cluster, offset_id, region_id, peer_id + i);
        write_kv(&cluster, region_id, key, val);
        remove_peer(&cluster, offset_id, region_id, peer_id + i);
    }

    add_learner(&cluster, offset_id, region_id, peer_id + repeat);
    write_kv(&cluster, region_id, key, val);
    let snap = cluster.routers[offset_id].stale_snapshot(region_id);
    assert_eq!(snap.get_value(key).unwrap().unwrap(), val);

    // TODO: check if the peer is removed once life trace is implemented or
    // snapshot is implemented.
    // Check if WAL is skipped for admin command.
    let mut cached = cluster.node(0).tablet_registry().get(region_id).unwrap();
    check_skip_wal(cached.latest().unwrap().as_inner().path());
}

/// Test if a peer can be destroyed by conf change if logs after conf change are
/// also replicated.
#[test]
fn test_remove_by_conf_change() {
    let cluster = Cluster::with_node_count(2, None);
    let (region_id, peer_id, offset_id) = (2, 10, 1);
    let mut req = add_learner(&cluster, offset_id, region_id, peer_id);

    // write one kv to make flow control replicated.
    let (key, val) = (b"key", b"value");
    write_kv(&cluster, region_id, key, val);

    let new_conf_ver = req.get_header().get_region_epoch().get_conf_ver() + 1;
    req.mut_header()
        .mut_region_epoch()
        .set_conf_ver(new_conf_ver);
    req.mut_admin_request()
        .mut_change_peer()
        .set_change_type(ConfChangeType::RemoveNode);
    let (admin_msg, admin_sub) = PeerMsg::admin_command(req.clone());
    // write one kv after removal
    let (key, val) = (b"key1", b"value");
    let header = Box::new(cluster.routers[0].new_request_for(region_id).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, key, val);
    let (msg, sub) = PeerMsg::simple_write(header, put.encode());
    // Send them at the same time so they will be all sent to learner.
    cluster.routers[0].send(region_id, admin_msg).unwrap();
    cluster.routers[0].send(region_id, msg).unwrap();
    let resp = block_on(admin_sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let resp = block_on(sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    // Dispatch messages so the learner will receive conf remove and write at the
    // same time.
    cluster.dispatch(region_id, vec![]);
    cluster.routers[1].wait_flush(region_id, Duration::from_millis(300));
    // Wait for apply.
    std::thread::sleep(Duration::from_millis(100));
    let raft_engine = &cluster.node(1).running_state().unwrap().raft_engine;
    let region_state = raft_engine
        .get_region_state(region_id, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_state(), PeerState::Tombstone);
    assert_eq!(raft_engine.get_raft_state(region_id).unwrap(), None);
}

fn add_learner(
    cluster: &Cluster,
    offset_id: usize,
    region_id: u64,
    peer_id: u64,
) -> RaftCmdRequest {
    let store_id = cluster.node(offset_id).id();
    let mut req = cluster.routers[0].new_request_for(region_id);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddLearnerNode);
    let new_peer = new_learner_peer(store_id, peer_id);
    admin_req.mut_change_peer().set_peer(new_peer.clone());
    let resp = cluster.routers[0]
        .admin_command(region_id, req.clone())
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = cluster.routers[0]
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, new_peer]);

    // heartbeat will create a learner.
    cluster.dispatch(region_id, vec![]);
    cluster.routers[0]
        .send(region_id, PeerMsg::Tick(PeerTick::Raft))
        .unwrap();
    let meta = cluster.routers[offset_id]
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.id, peer_id, "{:?}", meta);

    // Wait some time so snapshot can be generated.
    std::thread::sleep(Duration::from_millis(100));
    cluster.dispatch(region_id, vec![]);
    req
}

fn write_kv(cluster: &Cluster, region_id: u64, key: &[u8], val: &[u8]) {
    let header = Box::new(cluster.routers[0].new_request_for(region_id).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, key, val);
    let (msg, _) = PeerMsg::simple_write(header, put.encode());
    cluster.routers[0].send(region_id, msg).unwrap();
    std::thread::sleep(Duration::from_millis(1000));
    cluster.dispatch(region_id, vec![]);
}

fn remove_peer(cluster: &Cluster, offset_id: usize, region_id: u64, peer_id: u64) {
    let store_id = cluster.node(offset_id).id();
    let mut req = cluster.routers[0].new_request_for(region_id);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::RemoveNode);
    admin_req
        .mut_change_peer()
        .set_peer(new_learner_peer(store_id, peer_id));
    let resp = cluster.routers[0]
        .admin_command(region_id, req.clone())
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    cluster.routers[offset_id]
        .send(region_id, PeerMsg::Tick(PeerTick::Raft))
        .unwrap();
    cluster.dispatch(region_id, vec![]);
    std::thread::sleep(Duration::from_millis(100));

    let raft_engine = &cluster.node(offset_id).running_state().unwrap().raft_engine;
    let region_state = raft_engine
        .get_region_state(region_id, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_state(), PeerState::Tombstone);
    assert_eq!(raft_engine.get_raft_state(region_id).unwrap(), None);
}

/// The peer should be able to respond an unknown sender, otherwise the
/// liveness of configuration change can't be guaranteed.
#[test]
fn test_unknown_peer() {
    let cluster = Cluster::with_node_count(1, None);

    let router = &cluster.routers[0];
    let header = router.new_request_for(2).take_header();

    // Create a fake message to see whether it's responded.
    let from_peer = new_peer(10, 10);
    let mut msg = Box::<RaftMessage>::default();
    msg.set_region_id(2);
    msg.set_to_peer(header.get_peer().clone());
    msg.set_region_epoch(header.get_region_epoch().clone());
    msg.set_from_peer(from_peer.clone());
    let raft_message = msg.mut_message();
    raft_message.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
    raft_message.set_from(10);
    raft_message.set_term(10);

    router.send_raft_message(msg).unwrap();
    router.wait_flush(2, Duration::from_secs(3));
    // If peer cache is updated correctly, it should be able to respond.
    let msg = cluster.receiver(0).try_recv().unwrap();
    assert_eq!(*msg.get_to_peer(), from_peer);
    assert_eq!(msg.get_from_peer(), header.get_peer());
    assert_eq!(
        msg.get_message().get_msg_type(),
        MessageType::MsgHeartbeatResponse
    );
}

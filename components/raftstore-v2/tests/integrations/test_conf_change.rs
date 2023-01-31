// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{self, time::Duration};

use engine_traits::{Peekable, RaftEngineReadOnly, CF_DEFAULT};
use futures::executor::block_on;
use kvproto::{raft_cmdpb::AdminCmdType, raft_serverpb::PeerState};
use raft::prelude::ConfChangeType;
use raftstore_v2::{
    router::{PeerMsg, PeerTick},
    SimpleWriteEncoder,
};
use tikv_util::store::new_learner_peer;

use crate::cluster::{check_skip_wal, Cluster};

#[test]
fn test_simple_change() {
    let mut cluster = Cluster::with_node_count(2, None);
    let region_id = 2;
    let mut req = cluster.routers[0].new_request_for(2);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddLearnerNode);
    let store_id = cluster.node(1).id();
    let new_peer = new_learner_peer(store_id, 10);
    admin_req.mut_change_peer().set_peer(new_peer.clone());
    let resp = cluster.routers[0].admin_command(2, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = cluster.routers[0]
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    let match_index = meta.raft_apply.applied_index;
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, new_peer]);

    // So heartbeat will create a learner.
    cluster.dispatch(2, vec![]);
    let meta = cluster.routers[1]
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.id, 10, "{:?}", meta);
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(
        meta.raft_status.soft_state.leader_id,
        req.get_header().get_peer().get_id()
    );
    // Trigger the raft tick to replica the log to the learner and execute the
    // snapshot task.
    cluster.routers[0]
        .send(region_id, PeerMsg::Tick(PeerTick::Raft))
        .unwrap();
    cluster.dispatch(region_id, vec![]);

    // write one kv after snapshot
    let (key, val) = (b"key", b"value");
    let header = Box::new(cluster.routers[0].new_request_for(region_id).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, key, val);
    let (msg, _) = PeerMsg::simple_write(header, put.encode());
    cluster.routers[0].send(region_id, msg).unwrap();
    std::thread::sleep(Duration::from_millis(1000));
    cluster.dispatch(region_id, vec![]);

    let meta = cluster.routers[1]
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    // the learner truncated index muse be equal the leader applied index and can
    // read the new written kv.
    assert_eq!(match_index, meta.raft_apply.truncated_state.index);
    assert!(meta.raft_apply.applied_index >= match_index);
    let snap = cluster.routers[1].stale_snapshot(2);
    assert_eq!(snap.get_value(key).unwrap().unwrap(), val);

    req.mut_header()
        .mut_region_epoch()
        .set_conf_ver(new_conf_ver);
    req.mut_admin_request()
        .mut_change_peer()
        .set_change_type(ConfChangeType::RemoveNode);
    let resp = cluster.routers[0].admin_command(2, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = cluster.routers[0]
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer]);
    // TODO: check if the peer is removed once life trace is implemented or
    // snapshot is implemented.

    // Check if WAL is skipped for admin command.
    let mut cached = cluster.node(0).tablet_registry().get(2).unwrap();
    check_skip_wal(cached.latest().unwrap().as_inner().path());
}

/// Test if a peer can be destroyed by conf change if logs after conf change are
/// also replicated.
#[test]
fn test_remove_by_conf_change() {
    let cluster = Cluster::with_node_count(2, None);
    let region_id = 2;
    let mut req = cluster.routers[0].new_request_for(2);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddLearnerNode);
    let store_id = cluster.node(1).id();
    let new_peer = new_learner_peer(store_id, 10);
    admin_req.mut_change_peer().set_peer(new_peer);
    let resp = cluster.routers[0].admin_command(2, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    // So heartbeat will create a learner.
    cluster.dispatch(2, vec![]);
    // Trigger the raft tick to replica the log to the learner and execute the
    // snapshot task.
    cluster.routers[0]
        .send(region_id, PeerMsg::Tick(PeerTick::Raft))
        .unwrap();
    cluster.dispatch(region_id, vec![]);
    // Wait some time so snapshot can be generated.
    std::thread::sleep(Duration::from_millis(100));
    cluster.dispatch(region_id, vec![]);

    // write one kv to make flow control replicated.
    let (key, val) = (b"key", b"value");
    let header = Box::new(cluster.routers[0].new_request_for(region_id).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, key, val);
    let (msg, _) = PeerMsg::simple_write(header, put.encode());
    cluster.routers[0].send(region_id, msg).unwrap();
    cluster.dispatch(region_id, vec![]);

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

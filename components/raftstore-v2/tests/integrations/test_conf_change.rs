// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{self, time::Duration};

use kvproto::{
    raft_cmdpb::{AdminCmdType, CmdType, Request},
    raft_serverpb::RaftMessage,
};
use raft::prelude::ConfChangeType;
use raftstore_v2::router::PeerMsg;
use tikv_util::store::new_learner_peer;

use crate::cluster::Cluster;

#[test]
fn test_add_learner() {
    let cluster = Cluster::with_node_count(2, None);
    let region_id = 2;
    let learner_peer = new_learner_peer(cluster.node(1).id(), 10);

    let router0 = cluster.router(0);
    let mut req = router0.new_request_for(region_id);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddLearnerNode);
    admin_req.mut_change_peer().set_peer(learner_peer.clone());

    let leader_peer = req.get_header().get_peer().clone();
    let resp = router0.command(region_id, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;

    let meta = router0
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    let match_index = meta.raft_apply.applied_index;
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    // So heartbeat will create a learner.
    cluster.dispatch(region_id, vec![]);
    let router1 = cluster.router(1);
    let meta = router1
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();

    let mut raft_msg = RaftMessage::default();
    raft_msg.set_region_id(region_id);
    raft_msg.set_to_peer(learner_peer.clone());
    raft_msg.set_from_peer(leader_peer.clone());

    let raft_message = raft_msg.mut_message();
    raft_message.set_msg_type(raft::prelude::MessageType::MsgAppendResponse);
    raft_message.set_from(leader_peer.id);
    raft_message.set_to(learner_peer.id);
    raft_message.set_term(meta.raft_status.hard_state.term);
    raft_message.set_reject(true);
    raft_message.set_request_snapshot(1);
    // trigger the leader send snapshot to the new learner, it will create new
    // snapshot generate task.
    let msgs = vec![Box::new(raft_msg)];
    cluster.dispatch(region_id, msgs.clone());

    let mut req = router0.new_request_for(region_id);
    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(b"key".to_vec());
    put_req.mut_put().set_value(b"value".to_vec());
    req.mut_requests().push(put_req);
    // trigger the leader execute snapshot generator task.
    let (msg, _) = PeerMsg::raft_command(req.clone());
    router0.send(2, msg).unwrap();

    // trigger the learner execute persisted log.
    std::thread::sleep(Duration::from_millis(1000));
    cluster.dispatch(region_id, vec![]);

    let meta = router1
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    // the learner truncated index muse be equal the leader applied index
    assert_eq!(match_index, meta.raft_apply.truncated_state.index);
    assert!(meta.raft_apply.applied_index >= match_index);
}

#[test]
fn test_simple_change() {
    let cluster = Cluster::with_node_count(2, None);
    let router0 = cluster.router(0);
    let mut req = router0.new_request_for(2);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddLearnerNode);
    let store_id = cluster.node(1).id();
    let new_peer = new_learner_peer(store_id, 10);
    admin_req.mut_change_peer().set_peer(new_peer.clone());
    let resp = router0.command(2, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, new_peer]);

    // So heartbeat will create a learner.
    cluster.dispatch(2, vec![]);
    let router1 = cluster.router(1);
    let meta = router1
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.id, 10, "{:?}", meta);
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(
        meta.raft_status.soft_state.leader_id,
        req.get_header().get_peer().get_id()
    );

    req.mut_header()
        .mut_region_epoch()
        .set_conf_ver(new_conf_ver);
    req.mut_admin_request()
        .mut_change_peer()
        .set_change_type(ConfChangeType::RemoveNode);
    let resp = router0.command(2, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer]);
    // TODO: check if the peer is removed once life trace is implemented or
    // snapshot is implemented.
}

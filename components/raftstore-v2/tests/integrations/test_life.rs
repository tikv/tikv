// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{RaftEngineReadOnly, CF_DEFAULT};
use futures::executor::block_on;
use kvproto::{raft_cmdpb::AdminCmdType, raft_serverpb::RaftMessage};
use raft::prelude::{ConfChangeType, MessageType};
use raftstore_v2::{
    router::{PeerMsg, PeerTick},
    SimpleWriteEncoder,
};
use tikv_util::store::{new_learner_peer, new_peer};

use crate::cluster::{
    life_helper::{
        assert_peer_not_exist, assert_tombstone, assert_tombstone_msg, assert_valid_report,
    },
    Cluster,
};

/// Test a peer can be created by general raft message and destroyed tombstone
/// message.
#[test]
fn test_life_by_message() {
    let mut cluster = Cluster::default();
    let router = &cluster.routers[0];
    let test_region_id = 4;
    let test_peer_id = 5;
    let test_leader_id = 6;
    assert_peer_not_exist(test_region_id, test_peer_id, router);

    // Build a correct message.
    let mut msg = Box::<RaftMessage>::default();
    msg.set_region_id(test_region_id);
    msg.set_to_peer(new_peer(1, test_peer_id));
    msg.mut_region_epoch().set_conf_ver(1);
    msg.set_from_peer(new_peer(2, test_leader_id));
    let raft_message = msg.mut_message();
    raft_message.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
    raft_message.set_from(6);
    raft_message.set_term(5);

    let assert_wrong = |f: &dyn Fn(&mut RaftMessage)| {
        let mut wrong_msg = msg.clone();
        f(&mut wrong_msg);
        router.send_raft_message(wrong_msg).unwrap();
        assert_peer_not_exist(test_region_id, test_peer_id, router);
    };

    // Check mismatch store id.
    assert_wrong(&|msg| msg.mut_to_peer().set_store_id(4));

    // Check missing region epoch.
    assert_wrong(&|msg| {
        msg.take_region_epoch();
    });

    // Correct message will create a peer, but the peer will not be initialized.
    router.send_raft_message(msg.clone()).unwrap();
    let timeout = Duration::from_secs(3);
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.region_state.id, test_region_id);
    assert_eq!(meta.raft_status.id, test_peer_id);
    assert_eq!(meta.region_state.tablet_index, 0);
    // But leader should be set.
    assert_eq!(meta.raft_status.soft_state.leader_id, test_leader_id);

    // The peer should survive restart.
    cluster.restart(0);
    let router = &cluster.routers[0];
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id);
    let raft_engine = &cluster.node(0).running_state().unwrap().raft_engine;
    raft_engine.get_raft_state(test_region_id).unwrap().unwrap();
    raft_engine
        .get_apply_state(test_region_id, 0)
        .unwrap()
        .unwrap();

    // The peer should be destroyed by tombstone message.
    let mut tombstone_msg = msg.clone();
    tombstone_msg.set_is_tombstone(true);
    router.send_raft_message(tombstone_msg).unwrap();
    assert_peer_not_exist(test_region_id, test_peer_id, router);
    assert_tombstone(raft_engine, test_region_id, &new_peer(1, test_peer_id));

    // Restart should not recreate tombstoned peer.
    cluster.restart(0);
    let router = &cluster.routers[0];
    assert_peer_not_exist(test_region_id, test_peer_id, router);
    let raft_engine = &cluster.node(0).running_state().unwrap().raft_engine;
    assert_tombstone(raft_engine, test_region_id, &new_peer(1, test_peer_id));
}

#[test]
fn test_destroy_by_larger_id() {
    let mut cluster = Cluster::default();
    let router = &cluster.routers[0];
    let test_region_id = 4;
    let test_peer_id = 6;
    let init_term = 5;
    let mut msg = Box::<RaftMessage>::default();
    msg.set_region_id(test_region_id);
    msg.set_to_peer(new_peer(1, test_peer_id));
    msg.mut_region_epoch().set_conf_ver(1);
    msg.set_from_peer(new_peer(2, 8));
    let raft_message = msg.mut_message();
    raft_message.set_msg_type(MessageType::MsgHeartbeat);
    raft_message.set_from(6);
    raft_message.set_term(init_term);
    // Create the peer.
    router.send_raft_message(msg.clone()).unwrap();
    // There must be heartbeat response.
    let hb = cluster
        .receiver(0)
        .recv_timeout(Duration::from_millis(300))
        .unwrap();
    assert_eq!(
        hb.get_message().get_msg_type(),
        MessageType::MsgHeartbeatResponse
    );

    let timeout = Duration::from_secs(3);
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id);

    // Smaller ID should be ignored.
    let mut smaller_id_msg = msg;
    smaller_id_msg.set_to_peer(new_peer(1, test_peer_id - 1));
    smaller_id_msg.mut_message().set_term(init_term + 1);
    router.send_raft_message(smaller_id_msg.clone()).unwrap();
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id);
    assert_eq!(meta.raft_status.hard_state.term, init_term);
    cluster
        .receiver(0)
        .recv_timeout(Duration::from_millis(300))
        .unwrap_err();

    // Smaller ID tombstone message should trigger report.
    let mut smaller_id_tombstone_msg = smaller_id_msg.clone();
    smaller_id_tombstone_msg.set_is_tombstone(true);
    router.send_raft_message(smaller_id_tombstone_msg).unwrap();
    let report = cluster
        .receiver(0)
        .recv_timeout(Duration::from_millis(300))
        .unwrap();
    assert_valid_report(&report, test_region_id, test_peer_id - 1);

    // Larger ID should trigger destroy.
    let mut larger_id_msg = smaller_id_msg;
    larger_id_msg.set_to_peer(new_peer(1, test_peer_id + 1));
    router.send_raft_message(larger_id_msg).unwrap();
    assert_peer_not_exist(test_region_id, test_peer_id, router);
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id + 1);
    assert_eq!(meta.raft_status.hard_state.term, init_term + 1);

    // New peer should survive restart.
    cluster.restart(0);
    let router = &cluster.routers[0];
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id + 1);
    assert_eq!(meta.raft_status.hard_state.term, init_term + 1);
}

#[test]
fn test_gc_peer_request() {
    let cluster = Cluster::default();
    let router = &cluster.routers[0];
    let test_region_id = 4;
    let test_peer_id = 5;
    let test_leader_id = 6;

    let mut msg = Box::<RaftMessage>::default();
    msg.set_region_id(test_region_id);
    msg.set_to_peer(new_peer(1, test_peer_id));
    msg.mut_region_epoch().set_conf_ver(1);
    msg.set_from_peer(new_peer(2, test_leader_id));
    let raft_message = msg.mut_message();
    raft_message.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
    raft_message.set_from(6);
    raft_message.set_term(5);

    // Tombstone message should create the peer and then destroy it.
    let mut tombstone_msg = msg.clone();
    tombstone_msg.set_is_tombstone(true);
    router.send_raft_message(tombstone_msg.clone()).unwrap();
    cluster.routers[0].wait_flush(test_region_id, Duration::from_millis(300));
    assert_peer_not_exist(test_region_id, test_peer_id, router);
    // Resend a normal message will not create the peer.
    router.send_raft_message(msg).unwrap();
    assert_peer_not_exist(test_region_id, test_peer_id, router);
    cluster
        .receiver(0)
        .recv_timeout(Duration::from_millis(300))
        .unwrap_err();
    // Resend tombstone message should trigger report.
    router.send_raft_message(tombstone_msg).unwrap();
    assert_peer_not_exist(test_region_id, test_peer_id, router);
    let report = cluster
        .receiver(0)
        .recv_timeout(Duration::from_millis(300))
        .unwrap();
    assert_valid_report(&report, test_region_id, test_peer_id);
}

#[test]
fn test_gc_peer_response() {
    let cluster = Cluster::with_node_count(2, None);
    let region_id = 2;
    let mut req = cluster.routers[0].new_request_for(region_id);
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
    let raft_engine = &cluster.node(0).running_state().unwrap().raft_engine;
    let region_state = raft_engine
        .get_region_state(region_id, u64::MAX)
        .unwrap()
        .unwrap();
    assert!(region_state.get_removed_records().is_empty());

    let new_conf_ver = req.get_header().get_region_epoch().get_conf_ver() + 1;
    req.mut_header()
        .mut_region_epoch()
        .set_conf_ver(new_conf_ver);
    req.mut_admin_request()
        .mut_change_peer()
        .set_change_type(ConfChangeType::RemoveNode);
    let resp = cluster.routers[0]
        .admin_command(region_id, req.clone())
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    cluster.routers[0].wait_flush(region_id, Duration::from_millis(300));
    // Drain all existing messages.
    while cluster.receiver(0).try_recv().is_ok() {}

    let mut msg = Box::<RaftMessage>::default();
    msg.set_region_id(region_id);
    msg.set_to_peer(req.get_header().get_peer().clone());
    msg.set_from_peer(new_peer);
    let receiver = &cluster.receiver(0);
    for ty in &[MessageType::MsgRequestVote, MessageType::MsgRequestPreVote] {
        msg.mut_message().set_msg_type(*ty);
        cluster.routers[0].send_raft_message(msg.clone()).unwrap();
        let tombstone_msg = match receiver.recv_timeout(Duration::from_millis(300)) {
            Ok(msg) => msg,
            Err(e) => panic!("failed to receive tombstone message {:?}: {:?}", ty, e),
        };
        assert_tombstone_msg(&tombstone_msg, region_id, 10);
    }
    // Non-vote message should not trigger tombstone.
    msg.mut_message().set_msg_type(MessageType::MsgHeartbeat);
    cluster.routers[0].send_raft_message(msg).unwrap();
    cluster
        .receiver(0)
        .recv_timeout(Duration::from_millis(300))
        .unwrap_err();

    // GcTick should also trigger tombstone.
    cluster.routers[0]
        .send(region_id, PeerMsg::Tick(PeerTick::GcPeer))
        .unwrap();
    let tombstone_msg = cluster
        .receiver(0)
        .recv_timeout(Duration::from_millis(300))
        .unwrap();
    assert_tombstone_msg(&tombstone_msg, region_id, 10);

    // First message to create the peer and destroy.
    cluster.routers[1]
        .send_raft_message(Box::new(tombstone_msg.clone()))
        .unwrap();
    cluster.routers[1].wait_flush(region_id, Duration::from_millis(300));
    cluster
        .receiver(1)
        .recv_timeout(Duration::from_millis(300))
        .unwrap_err();
    // Send message should trigger tombstone report.
    cluster.routers[1]
        .send_raft_message(Box::new(tombstone_msg))
        .unwrap();
    let report = cluster
        .receiver(1)
        .recv_timeout(Duration::from_millis(300))
        .unwrap();
    assert_valid_report(&report, region_id, 10);
    cluster.routers[0]
        .send_raft_message(Box::new(report))
        .unwrap();
    let raft_engine = &cluster.node(0).running_state().unwrap().raft_engine;
    let region_state = raft_engine
        .get_region_state(region_id, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_removed_records().len(), 1);
    // Tick should flush records gc.
    cluster.routers[0]
        .send(region_id, PeerMsg::Tick(PeerTick::GcPeer))
        .unwrap();
    // Trigger a write to make sure records gc is finished.
    let header = Box::new(cluster.routers[0].new_request_for(region_id).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key", b"value");
    let (msg, sub) = PeerMsg::simple_write(header, put.encode());
    cluster.routers[0].send(region_id, msg).unwrap();
    block_on(sub.result()).unwrap();
    cluster.routers[0].wait_flush(region_id, Duration::from_millis(300));
    let region_state = raft_engine
        .get_region_state(region_id, u64::MAX)
        .unwrap()
        .unwrap();
    assert!(region_state.get_removed_records().is_empty());
}

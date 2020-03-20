// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use crate::pd_client::PdClient;
use crate::tikv_util::HandyRwLock;
use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::ConfChangeType;
use raft::eraftpb::MessageType;
use raftstore::store::QuorumAlgorithm;
use test_raftstore::*;

// Test QuorumAlgorithm::IntegrationOnHalfFail works as expected.
#[test]
fn test_integration_on_half_fail_quorum_fn() {
    let mut cluster = new_node_cluster(0, 5);
    // Disable hibernate region feature so that it will check quorum after some ticks.
    cluster.cfg.raft_store.hibernate_regions = false;
    cluster.cfg.raft_store.quorum_algorithm = QuorumAlgorithm::IntegrationOnHalfFail;
    cluster.run();
    cluster.must_put(b"k1", b"v0");

    let (tx, rx) = mpsc::sync_channel(1024);
    let msg_cb = Arc::new(Box::new(move |_: &RaftMessage| drop(tx.send(0)))
        as Box<dyn Fn(&RaftMessage) + Send + Sync>);
    let filter = Box::new(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .msg_type(MessageType::MsgRequestPreVote)
            .set_msg_callback(msg_cb)
            .when(Arc::new(AtomicBool::new(false))),
    );
    cluster.sim.wl().add_send_filter(1, filter);

    // After peer 4 and 5 fail, peer 1 should start a new election because check quorum fail.
    cluster.stop_node(4);
    cluster.stop_node(5);
    if rx.recv_timeout(Duration::from_secs(3)).is_err() {
        panic!("region 1 must start a new election");
    }

    cluster.sim.wl().clear_send_filters(1);

    let (tx, rx) = mpsc::sync_channel(1024);
    let msg_cb = Arc::new(Box::new(move |_: &RaftMessage| drop(tx.send(0)))
        as Box<dyn Fn(&RaftMessage) + Send + Sync>);
    let filter = Box::new(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .msg_type(MessageType::MsgAppend)
            .msg_type(MessageType::MsgAppendResponse)
            .set_msg_callback(msg_cb)
            .when(Arc::new(AtomicBool::new(false))),
    );
    cluster.sim.wl().add_send_filter(1, filter);

    // No new leader can be elected.
    let election_timeout = {
        let cfg = &cluster.cfg.raft_store;
        cfg.raft_election_timeout_ticks as u32 * cfg.raft_base_tick_interval.0
    };
    if rx.recv_timeout(election_timeout) != Err(mpsc::RecvTimeoutError::Timeout) {
        panic!("No new leader should be elected");
    }
}

#[test]
fn test_check_conf_change() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.quorum_algorithm = QuorumAlgorithm::IntegrationOnHalfFail;
    cluster.pd_client.disable_default_operator();
    let rid = cluster.run_conf_change();

    cluster.pd_client.must_add_peer(rid, new_peer(2, 2));
    cluster.pd_client.must_add_peer(rid, new_learner_peer(3, 3));

    let r1 = cluster
        .pd_client
        .get_region_by_id(rid)
        .wait()
        .unwrap()
        .unwrap();

    // Stop peer 3 and compact logs on peer 1 and 2, so that peer 3 can't become voter.
    cluster.stop_node(3);
    for _ in 0..100 {
        cluster.must_put(b"k1", b"v1");
    }
    let compact_log = new_compact_log_request(100, 10);
    let req = new_admin_request(rid, r1.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(!res.get_header().has_error());

    // Peer 3 can't be promoted.
    let cc = new_change_peer_request(ConfChangeType::AddNode, new_peer(3, 3));
    let req = new_admin_request(rid, r1.get_region_epoch(), cc);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res
        .get_header()
        .get_error()
        .get_message()
        .contains("unsafe to perform conf change peer"));
}

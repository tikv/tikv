// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::mpsc, thread, time::Duration};

use futures::executor::block_on;
use kvproto::{
    raft_cmdpb::{CmdType, RaftCmdRequest, Request},
    raft_serverpb::RaftMessage,
};
use raft::eraftpb::MessageType;
use raftstore::store::{util::new_peer, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER};
use raftstore_v2::router::PeerMsg;
use test_raftstore::DropSnapshotFilter;

use crate::Cluster;

/// Test basic write flow.
#[test]
fn test_basic_generate_snapshot() {
    test_util::init_log_for_test();
    let cluster = Cluster::with_node_count(2);
    let router1 = cluster.router(0);
    let mut router2 = cluster.router(1);
    let (tx, rx) = mpsc::channel();
    router2.add_recv_filter(Box::new(DropSnapshotFilter::new(tx)));
    let mut raft_msg = RaftMessage::default();
    raft_msg.set_region_id(3);
    raft_msg.set_to_peer(new_peer(1, 4));
    raft_msg.set_from_peer(new_peer(2, 5));
    let epoch = raft_msg.mut_region_epoch();
    epoch.set_version(INIT_EPOCH_VER);
    epoch.set_conf_ver(INIT_EPOCH_CONF_VER);

    let raft_message = raft_msg.mut_message();
    raft_message.set_msg_type(raft::prelude::MessageType::MsgAppendResponse);
    raft_message.set_from(5);
    raft_message.set_to(10);
    raft_message.set_term(6);
    raft_message.set_reject(true);
    raft_message.set_index(100);
    raft_message.set_request_snapshot(1);
    std::thread::sleep(Duration::from_secs(1));
    router1.send_raft_message(Box::new(raft_msg)).unwrap();
    // Check the snapshot message
    let _first_snap_idx = rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
use std::sync::{atomic::AtomicBool, mpsc, Arc};
use std::time::Duration;

use fail;
use raft::eraftpb::MessageType;
use tikv::util::HandyRwLock;

use test_raftstore::*;

#[test]
fn test_slow_split() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);
    let region = cluster.get_region(b"");

    // Only need peer 1 and 3.
    pd_client.must_remove_peer(1, new_peer(2, 2));
    cluster.stop_node(2);
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Only allow 1 MsgRequestPreVote per follower.
    let filter = Box::new(
        RegionPacketFilter::new(1000, 1) // new region id is 1000
            .msg_type(MessageType::MsgRequestPreVote)
            .direction(Direction::Send)
            .allow(1),
    );
    cluster.sim.wl().add_send_filter(1, filter);

    // Ensure pre-vote message is really sended.
    let (tx1, rx1) = mpsc::channel();
    let prevote_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVote,
        tx1.clone(),
        Arc::from(AtomicBool::new(true)),
    ));
    cluster.sim.wl().add_send_filter(1, prevote_notifier);

    // Ensure pre-vote response is really sended.
    let (tx2, rx2) = mpsc::channel();
    let prevote_resp_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx2.clone(),
        Arc::from(AtomicBool::new(true)),
    ));
    cluster.sim.wl().add_send_filter(3, prevote_resp_notifier);

    // After split, pre-vote message should be sent to peer 2, but we can't receive
    // pre-vote response because it's buffered in pending_votes.
    fail::cfg("apply_before_split_1_3", "pause").unwrap();
    cluster.must_split(&region, b"k2");
    assert!(rx1.recv_timeout(Duration::from_millis(100)).is_ok());
    assert!(rx2.recv_timeout(Duration::from_millis(200)).is_err());

    fail::cfg("apply_before_split_1_3", "off").unwrap();
    assert!(rx2.recv_timeout(Duration::from_secs(2)).is_ok());
}

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
use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use fail;
use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::MessageType;
use tikv::raftstore::store::util::is_vote_msg;
use tikv::raftstore::Result;
use tikv::util::HandyRwLock;

use test_raftstore::*;

#[test]
fn test_follower_slow_split() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.run();
    let region = cluster.get_region(b"");

    // Only need peer 1 and 3. Stop node 2 to avoid extra vote messages.
    pd_client.must_remove_peer(1, new_peer(2, 2));
    cluster.stop_node(2);
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Use a channel to retrieve start_key and end_key in pre-vote messages.
    let (range_tx, range_rx) = mpsc::channel();
    let prevote_filter = PrevoteRangeFilter {
        // Only send 1 pre-vote message to peer 3 so if peer 3 drops it,
        // it needs to start a new election.
        filter: RegionPacketFilter::new(1000, 1) // new region id is 1000
            .msg_type(MessageType::MsgRequestPreVote)
            .direction(Direction::Send)
            .allow(1),
        tx: Mutex::new(range_tx),
    };
    cluster.sim.wl().add_send_filter(1, box prevote_filter);

    // Ensure pre-vote response is really sended.
    let (tx, rx) = mpsc::channel();
    let prevote_resp_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));
    cluster.sim.wl().add_send_filter(3, prevote_resp_notifier);

    // After split, pre-vote message should be sent to peer 2.
    fail::cfg("apply_before_split_1_3", "pause").unwrap();
    cluster.must_split(&region, b"k2");
    let range = range_rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_eq!(range.0, b"");
    assert_eq!(range.1, b"k2");

    // After the follower split success, it will response to the pending vote.
    fail::cfg("apply_before_split_1_3", "off").unwrap();
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_ok());
}

// Filter prevote message and record the range.
struct PrevoteRangeFilter {
    filter: RegionPacketFilter,
    tx: Mutex<mpsc::Sender<(Vec<u8>, Vec<u8>)>>,
}

impl Filter<RaftMessage> for PrevoteRangeFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        self.filter.before(msgs)?;
        if let Some(msg) = msgs.iter().filter(|m| is_vote_msg(m.get_message())).last() {
            let start_key = msg.get_start_key().to_owned();
            let end_key = msg.get_end_key().to_owned();
            let tx = self.tx.lock().unwrap();
            let _ = tx.send((start_key, end_key));
        }
        Ok(())
    }
}

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
use tikv::pd::PdClient;
use tikv::util::HandyRwLock;

use test_raftstore::*;

#[test]
fn test_slow_split() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_transfer_leader(region.get_id(), new_peer(1, 1));

    let (tx, rx) = mpsc::channel();
    let notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));
    cluster.sim.wl().add_send_filter(2, notifier);

    fail::cfg("raftstore_follower_slow_split", "pause").unwrap();
    cluster.must_split(&region, b"k2");
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());

    fail::cfg("raftstore_follower_slow_split", "off").unwrap();
    assert!(rx.recv_timeout(Duration::from_secs(3)).is_ok());
}

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

use std::sync::mpsc;
use std::time::Duration;

use fail;
use kvproto::metapb;
use kvproto::raft_cmdpb::RaftCmdResponse;
use tikv::util::HandyRwLock;

use raftstore::cluster::{Cluster, Simulator};
use raftstore::node::new_node_cluster;
use raftstore::transport_simulate::*;
use raftstore::util::*;

fn stale_read_after_split(right_derive: bool) {
    let _guard = ::setup();
    let local_read_fp = "local_reader_on_run";

    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    cluster.cfg.raft_store.right_derive_when_split = right_derive;
    let election_timeout = configure_for_lease_read(&mut cluster, None, None);

    cluster.run();

    // Write the initial values.
    let key1 = b"k1";
    cluster.must_put(key1, b"v1");
    let key2 = b"k2";
    cluster.must_put(key2, b"v2");

    // Get the first region.
    let region_left = cluster.get_region(key1);
    let region_right = cluster.get_region(key2);
    assert_eq!(region_left, region_right);
    let region1 = region_left;

    // Get the current leader.
    let leader1 = cluster.leader_of_region(region1.get_id()).unwrap();

    // Install the lease read detector.
    let detector = LeaseReadFilter::default();
    cluster.add_send_filter(CloneFilterFactory(detector.clone()));

    // Issue reads request and check the value on response.
    must_read_on_peer(&mut cluster, leader1.clone(), region1.clone(), key1, b"v1");
    must_read_on_peer(&mut cluster, leader1.clone(), region1.clone(), key2, b"v2");
    // Check if the leader does a local read.
    assert_eq!(detector.ctx.rl().len(), 0);

    // Pause the localreader.
    fail::cfg(local_read_fp, "pause").unwrap();

    // Issue reads, they will be buffered in localreader's channel.
    let rx1 = must_async_read_on_peer(&cluster, leader1.clone(), region1.clone(), key1);
    let rx2 = must_async_read_on_peer(&cluster, leader1.clone(), region1.clone(), key2);

    // Split the frist region.
    cluster.must_split(&region1, key2);

    // Get the second region;
    let region_left = cluster.get_region(key1);
    let region_right = cluster.get_region(key2);
    assert_ne!(region_left, region_right);
    let region2 = if right_derive {
        region_left
    } else {
        region_right
    };

    // Get the leader of the second region.
    let leader2 = cluster.leader_of_region(region2.get_id()).unwrap();
    assert_eq!(leader1.get_store_id(), leader2.get_store_id());

    // Add a follower.
    let follower2 = region2
        .get_peers()
        .iter()
        .find(|pr| *pr != &leader2)
        .unwrap()
        .clone();

    // Issue a transfer leader request to transfer leader from `leader2` to `follower2`.
    cluster.must_transfer_leader(region2.get_id(), follower2);

    // Continue the localreader.
    fail::remove(local_read_fp);

    must_wait_stale_epoch(rx1, election_timeout);
    must_wait_stale_epoch(rx2, election_timeout);
}

fn must_wait_stale_epoch(rx: mpsc::Receiver<RaftCmdResponse>, timeout: Duration) {
    let resp = rx.recv_timeout(timeout).unwrap();
    assert!(
        resp.get_header().get_error().has_stale_epoch(),
        "expect stale epoch, got: {:?}",
        resp
    );
}

// Execute requests asynchronously, return a channel for receiving the result.
fn must_async_read_on_peer<T: Simulator>(
    cluster: &Cluster<T>,
    peer: metapb::Peer,
    mut region: metapb::Region,
    key: &[u8],
) -> mpsc::Receiver<RaftCmdResponse> {
    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![new_get_cmd(key)],
        false,
    );
    let node_id = peer.get_store_id();
    req.mut_header().set_peer(peer);
    let (cb, rx) = make_cb(&req);
    cluster
        .sim
        .read()
        .unwrap()
        .async_command_on_node(node_id, req, cb)
        .unwrap();
    rx
}

#[test]
fn test_node_stale_read_after_split_left_derive() {
    stale_read_after_split(false);
}

#[test]
fn test_node_stale_read_after_split_right_derive() {
    stale_read_after_split(true);
}

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

use std::thread;
use std::time::Duration;

use fail;
use kvproto::metapb::{Peer, Region};

use raftstore::cluster::Cluster;
use raftstore::node::{new_node_cluster, NodeCluster};
use raftstore::util::*;
use tikv::raftstore::store::Callback;

fn stale_read_during_splitting(right_derive: bool) {
    let _guard = ::setup();

    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    configure_for_lease_read(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = right_derive;
    let base_tick = cluster.cfg.raft_store.raft_base_tick_interval.0;
    let election_timeout = base_tick * cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    cluster.run();

    // Write the initial values.
    let key1 = b"k1";
    let v1 = b"v1";
    cluster.must_put(key1, v1);
    let key2 = b"k2";
    let v2 = b"v2";
    cluster.must_put(key2, v2);

    // Get the first region.
    let region_left = cluster.get_region(key1);
    let region_right = cluster.get_region(key2);
    assert_eq!(region_left, region_right);
    let region1 = region_left;
    assert_eq!(region1.get_id(), 1);
    let peer3 = region1
        .get_peers()
        .iter()
        .find(|p| p.get_id() == 3)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(region1.get_id(), peer3.clone());

    // Get the current leader.
    let leader1 = peer3;

    // Pause the apply worker of peer 3.
    let apply_split = "apply_before_split_1_3";
    fail::cfg(apply_split, "pause").unwrap();

    // Split the frist region.
    cluster.split_region(&region1, key2, Callback::Write(Box::new(move |_| {})));

    // Sleep for a while.
    // The TiKVs that have followers of the old region will elected a leader
    // of the new region.
    //           TiKV A  TiKV B  TiKV C
    // Region 1    L       F       F
    // Region 2    X       L       F
    // Note: A has the peer 3,
    //       L: leader, F: follower, X: peer is not ready.
    thread::sleep(election_timeout);

    // A key that is covered by the old region and the new region.
    let stale_key = if right_derive { key1 } else { key2 };
    // Get the new region.
    let region2 = cluster.get_region_with(stale_key, |region| region != &region1);

    // Get the leader of the new region.
    let leader2 = cluster.leader_of_region(region2.get_id()).unwrap();
    assert_ne!(leader1.get_store_id(), leader2.get_store_id());

    // A new value for key2.
    let v3 = b"v3";
    let mut request = new_request(
        region2.get_id(),
        region2.get_region_epoch().clone(),
        vec![new_put_cf_cmd("default", stale_key, v3)],
        false,
    );
    request.mut_header().set_peer(leader2.clone());
    cluster
        .call_command_on_node(leader2.get_store_id(), request, Duration::from_secs(5))
        .unwrap();

    // LocalRead.
    let read_quorum = false;
    must_not_eq_on_key(
        &mut cluster,
        stale_key,
        v3,
        read_quorum,
        &region1,
        &leader1,
        &region2,
        &leader2,
    );

    // ReadIndex.
    let read_quorum = true;
    must_not_eq_on_key(
        &mut cluster,
        stale_key,
        v3,
        read_quorum,
        &region1,
        &leader1,
        &region2,
        &leader2,
    );

    // Leaders can always propose read index despite split.
    let propose_readindex = "before_propose_readindex";
    fail::cfg(propose_readindex, "return(true)").unwrap();

    // Can not execute reads that are queued.
    let value1 = read_on_peer(
        &mut cluster,
        leader1.clone(),
        region1.clone(),
        stale_key,
        read_quorum,
        Duration::from_secs(1),
    );
    debug!("stale_key: {:?}, {:?}", stale_key, value1);
    value1.unwrap_err(); // Error::Timeout

    // Split shall be processed on peer 3.
    fail::remove(apply_split);

    // It should read an error stale epoch instead of timeout.
    let value1 = read_on_peer(
        &mut cluster,
        leader1.clone(),
        region1.clone(),
        stale_key,
        read_quorum,
        Duration::from_secs(5),
    );
    debug!("stale_key: {:?}, {:?}", stale_key, value1);
    assert!(value1.unwrap().get_header().get_error().has_stale_epoch());

    // Clean up.
    fail::remove(propose_readindex);
}

#[allow(too_many_arguments)]
fn must_not_eq_on_key(
    cluster: &mut Cluster<NodeCluster>,
    key: &[u8],
    value: &[u8],
    read_quorum: bool,
    old_region: &Region,
    old_leader: &Peer,
    new_region: &Region,
    new_leader: &Peer,
) {
    let value1 = read_on_peer(
        cluster,
        old_leader.clone(),
        old_region.clone(),
        key,
        read_quorum,
        Duration::from_secs(1),
    );
    let value2 = read_on_peer(
        cluster,
        new_leader.clone(),
        new_region.clone(),
        key,
        read_quorum,
        Duration::from_secs(1),
    );
    debug!("key: {:?}, {:?} vs {:?}", key, value1, value2);
    assert_eq!(must_get_value(value2.as_ref().unwrap()).as_slice(), value);
    // The old leader should return an error.
    assert!(
        value1.as_ref().unwrap().get_header().has_error(),
        "{:?}",
        value1
    );
}

#[test]
fn test_node_stale_read_during_splitting_left_derive() {
    stale_read_during_splitting(false);
}

#[test]
fn test_node_stale_read_during_splitting_right_derive() {
    stale_read_during_splitting(true);
}

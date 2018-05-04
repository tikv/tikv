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

use raftstore::node::new_node_cluster;
use raftstore::util::*;
use tikv::pd::PdClient;
use tikv::raftstore::store::Callback;

fn stale_read_during_splitting(right_derive: bool) {
    let _guard = ::setup();
    let apply_split = "apply_before_split_1_3";

    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    configure_for_lease_read(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = right_derive;
    let base_tick = cluster.cfg.raft_store.raft_base_tick_interval.0;
    let election_timeout = base_tick * cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    cluster.run();

    // Write the initial values.
    let key1 = b"k1";
    cluster.must_put(key1, b"v1");
    let key2 = b"k2";
    cluster.must_put(key2, b"v2");

    // Get the first region.
    let region1 = {
        let region_left = cluster.get_region(key1);
        let region_right = cluster.get_region(key2);
        assert_eq!(region_left, region_right);
        region_left
    };
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

    // Pause the apply worker of peer 1.
    fail::cfg(apply_split, "pause").unwrap();

    // Split the frist region.
    cluster.split_region(&region1, key2, Callback::Write(Box::new(move |_| {})));

    // Sleep for a while
    // The TiKVs that have followers of the old region will elected a leader
    // of the new region.
    //           TiKV 1  TiKV 2  TiKV 3
    // Region 1    L       F       F
    // Region 2    X       L       F
    // L: leader, F: follower, X: peer is not ready.
    thread::sleep(election_timeout);

    // A common key that is covered by the old region and the new region.
    let common_key = if right_derive { key1 } else { key2 };
    // Get the new region;
    let region2 = loop {
        match cluster.pd_client.get_region(common_key) {
            Ok(ref region) if &region1 != region => break region.clone(),
            _ => {
                // We may meet range gap after split, so here we will
                // retry to get the region again.
                sleep_ms(20);
            }
        }
    };

    // Get the leader of the new region.
    let leader2 = {
        let leader = cluster.leader_of_region(region2.get_id()).unwrap();
        assert_ne!(leader1.get_store_id(), leader.get_store_id());
        leader
    };

    // A new value for key2.
    let mut request = new_request(
        region2.get_id(),
        region2.get_region_epoch().clone(),
        vec![new_put_cf_cmd("default", common_key, b"v3")],
        false,
    );
    request.mut_header().set_peer(leader2.clone());

    cluster
        .call_command_on_node(leader2.get_store_id(), request, Duration::from_secs(5))
        .unwrap();
    // Issue read requests and check the value on response.
    let value1 = read_on_peer(
        &mut cluster,
        leader1,
        region1.clone(),
        common_key,
        Duration::from_secs(1),
    );
    let value2 = read_on_peer(
        &mut cluster,
        leader2,
        region2.clone(),
        common_key,
        Duration::from_secs(1),
    );

    error!(
        "STALE READ!!!! common_key: {:?}, {:?} vs {:?}",
        common_key, value1, value2
    );

    // Clean up.
    fail::remove(apply_split);
}

#[test]
fn test_node_stale_read_during_splitting_left_derive() {
    stale_read_during_splitting(false);
}

#[test]
fn test_node_stale_read_during_splitting_right_derive() {
    stale_read_during_splitting(true);
}

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

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use fail;
use kvproto::metapb::{Peer, Region};

use test_raftstore::*;
use tikv::pd::PdClient;
use tikv::raftstore::store::Callback;

fn stale_read_during_splitting(right_derive: bool) {
    let _guard = ::setup();

    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    cluster.cfg.raft_store.right_derive_when_split = right_derive;
    let election_timeout = configure_for_lease_read(&mut cluster, None, None);
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

    must_not_stale_read(
        &mut cluster,
        stale_key,
        &region1,
        &leader1,
        &region2,
        &leader2,
        apply_split,
    );
}

fn must_not_stale_read(
    cluster: &mut Cluster<NodeCluster>,
    stale_key: &[u8],
    old_region: &Region,
    old_leader: &Peer,
    new_region: &Region,
    new_leader: &Peer,
    fp: &str,
) {
    // A new value for stale_key.
    let v3 = b"v3";
    let mut request = new_request(
        new_region.get_id(),
        new_region.get_region_epoch().clone(),
        vec![new_put_cf_cmd("default", stale_key, v3)],
        false,
    );
    request.mut_header().set_peer(new_leader.clone());
    cluster
        .call_command_on_node(new_leader.get_store_id(), request, Duration::from_secs(5))
        .unwrap();

    // LocalRead.
    let read_quorum = false;
    must_not_eq_on_key(
        cluster,
        stale_key,
        v3,
        read_quorum,
        old_region,
        old_leader,
        new_region,
        new_leader,
    );

    // ReadIndex.
    let read_quorum = true;
    must_not_eq_on_key(
        cluster,
        stale_key,
        v3,
        read_quorum,
        old_region,
        old_leader,
        new_region,
        new_leader,
    );

    // Leaders can always propose read index despite split/merge.
    let propose_readindex = "before_propose_readindex";
    fail::cfg(propose_readindex, "return(true)").unwrap();

    // Can not execute reads that are queued.
    let value1 = read_on_peer(
        cluster,
        old_leader.clone(),
        old_region.clone(),
        stale_key,
        read_quorum,
        Duration::from_secs(1),
    );
    debug!("stale_key: {:?}, {:?}", stale_key, value1);
    value1.unwrap_err(); // Error::Timeout

    // Remove the fp.
    fail::remove(fp);

    // It should read an error instead of timeout.
    let value1 = read_on_peer(
        cluster,
        old_leader.clone(),
        old_region.clone(),
        stale_key,
        read_quorum,
        Duration::from_secs(5),
    );
    debug!("stale_key: {:?}, {:?}", stale_key, value1);
    assert!(value1.unwrap().get_header().has_error());

    // Clean up.
    fail::remove(propose_readindex);
}

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
    debug!("stale_key: {:?}, {:?} vs {:?}", key, value1, value2);
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

#[test]
fn test_stale_read_during_merging() {
    let _guard = ::setup();

    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    configure_for_merge(&mut cluster);
    let lease = configure_for_lease_read(&mut cluster, None, None);
    cluster.cfg.raft_store.right_derive_when_split = false;
    cluster.cfg.raft_store.pd_heartbeat_tick_interval =
        cluster.cfg.raft_store.raft_base_tick_interval.clone();
    debug!("max leader lease: {:?}", lease);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    // Write the initial values.
    let key1 = b"k1";
    let v1 = b"v1";
    cluster.must_put(key1, v1);
    let key2 = b"k2";
    let v2 = b"v2";
    cluster.must_put(key2, v2);
    let region = pd_client.get_region(b"k1").unwrap();
    pd_client.must_add_peer(region.get_id(), new_peer(2, 4));
    pd_client.must_add_peer(region.get_id(), new_peer(3, 5));

    cluster.must_split(&region, b"k2");

    let mut region1 = cluster.get_region(key1);
    let mut region1000 = cluster.get_region(key2);
    assert_ne!(region1, region1000);
    assert_eq!(region1.get_id(), 1); // requires disable right_derive.
    let leader1 = region1
        .get_peers()
        .iter()
        .find(|p| p.get_id() == 4)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(region1.get_id(), leader1.clone());

    let leader1000 = region1000
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() != leader1.get_store_id())
        .unwrap()
        .clone();
    cluster.must_transfer_leader(region1000.get_id(), leader1000.clone());
    assert_ne!(leader1.get_store_id(), leader1000.get_store_id());

    //             Merge into
    // region1000 ------------> region1,
    cluster.try_merge(region1000.get_id(), region1.get_id());

    // Pause the apply worker other than peer 4.
    let apply_commit_merge = "apply_before_commit_merge_except_1_4";
    fail::cfg(apply_commit_merge, "pause").unwrap();

    // Wait for commit merge.
    // The TiKVs that have followers of the old region will elected a leader
    // of the new region.
    //             TiKV A  TiKV B  TiKV C
    // Region    1   L       F       F
    // Region 1000   F       L       F
    //           after wait
    // Region    1   L       F       F
    // Region 1000   X       L       F
    // Note: L: leader, F: follower, X: peer is not exist.
    // TODO: what if cluster runs slow and lease is expired.
    // Epoch changed by prepare merge.
    // We can not use `get_region_with` to get the latest info of reigon 1000,
    // because leader1 is not paused, it executes commit merge very fast
    // and reports pd, its range covers region1000.
    //
    // region1000 does prepare merge, it increases ver and conf_ver by 1.
    debug!("before merge: {:?} | {:?}", region1000, region1);
    let region1000_version = region1000.get_region_epoch().get_version() + 1;
    region1000
        .mut_region_epoch()
        .set_version(region1000_version);
    let region1000_conf_version = region1000.get_region_epoch().get_conf_ver() + 1;
    region1000
        .mut_region_epoch()
        .set_conf_ver(region1000_conf_version);

    // Epoch changed by commit merge.
    region1 = cluster.get_region_with(key1, |region| region != &region1);
    debug!("after merge: {:?} | {:?}", region1000, region1);

    // A key that is covered by region 1000 and region 1.
    let stale_key = key2;

    must_not_stale_read(
        &mut cluster,
        stale_key,
        &region1000,
        &leader1000,
        &region1,
        &leader1,
        apply_commit_merge,
    );
}

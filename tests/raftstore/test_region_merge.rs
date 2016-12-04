// Copyright 2016 PingCAP, Inc.
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

use tikv::pd::PdClient;

use super::pd::ALLOC_BASE_ID;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::transport_simulate::*;
use super::util::*;

fn test_simple_region_merge<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;

    cluster.run();

    let pd_client = cluster.pd_client.clone();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    let region = pd_client.get_region(b"k2").unwrap();

    cluster.must_split(&region, b"k2");

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k2").unwrap();
    assert_ne!(r1, r2);

    cluster.must_merge(&r1);

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k2").unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn test_node_simple_region_merge() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_simple_region_merge(&mut cluster);
}

#[test]
fn test_server_simple_region_merge() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_simple_region_merge(&mut cluster);
}

fn test_quorum_region_merge<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();
    cluster.run();
    // Isolate peer 2 from other part of the cluster.
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    let region = pd_client.get_region(b"k2").unwrap();

    cluster.must_split(&region, b"k2");

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k2").unwrap();
    assert_ne!(r1, r2);

    cluster.must_merge(&r1);

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k2").unwrap();
    assert_eq!(r1, r2);

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, b"k1");
    must_get_none(&engine_2, b"k2");
}

#[test]
fn test_node_quorum_region_merge() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_quorum_region_merge(&mut cluster);
}

#[test]
fn test_server_quorum_region_merge() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_quorum_region_merge(&mut cluster);
}

fn test_data_migration_region_merge<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;

    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();

    // Setup the initial cluster with three peers.
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));

    // Ensure the initial cluster works.
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    assert_eq!(cluster.get(b"k1"), Some(b"v1".to_vec()));
    assert_eq!(cluster.get(b"k2"), Some(b"v2".to_vec()));

    // Split the initial region into two regions and separate them into different stores.
    let region = pd_client.get_region(b"k2").unwrap();
    cluster.must_split(&region, b"k2");
    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k2").unwrap();
    assert_ne!(r1, r2);

    // Add peers [(4, _), (5, _), (6, _)].
    // To avoid the conflict of these allocated ids, the newly added peers use value
    // less than ALLOC_BASE_ID as their ids.
    pd_client.must_add_peer(r2.get_id(), new_peer(4, (ALLOC_BASE_ID - 4) as u64));
    pd_client.must_add_peer(r2.get_id(), new_peer(5, (ALLOC_BASE_ID - 5) as u64));
    pd_client.must_add_peer(r2.get_id(), new_peer(6, (ALLOC_BASE_ID - 6) as u64));
    // Ensure the newly add peers have been updated.
    cluster.must_put(b"k3", b"v3");
    // Remove peers [(1, 1), (2, 2), (3, 3)].
    pd_client.must_remove_peer(r2.get_id(), new_peer(1, (ALLOC_BASE_ID + 1) as u64));
    pd_client.must_remove_peer(r2.get_id(), new_peer(2, (ALLOC_BASE_ID + 2) as u64));
    pd_client.must_remove_peer(r2.get_id(), new_peer(3, (ALLOC_BASE_ID + 3) as u64));

    // Ensure the cluster still works after peers are added and removed.
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.get(b"k4"), Some(b"v4".to_vec()));

    // Merge two regions on different stores.
    cluster.must_merge(&r1);

    // Check the region merge is done.
    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k3").unwrap();
    assert_eq!(r1, r2);
}

#[test]
fn test_node_data_migration_region_merge() {
    let count = 6;
    let mut cluster = new_node_cluster(0, count);
    test_data_migration_region_merge(&mut cluster);
}

#[test]
fn test_server_data_migration_region_merge() {
    let count = 6;
    let mut cluster = new_server_cluster(0, count);
    test_data_migration_region_merge(&mut cluster);
}

// TODO add test cases:
// 1. Start merge will hang on one node isolation. No split/conf change is allowed in this state.
//    If the isolation is gone when network recovers, the merge procedure will continue.
// 2. A pending(committed, not committed) conf change or region split happen in
//    1) from region, would cause into region to rollback the region merge
//    2) into region, would cause it cancels the region merge
// 3. the retry of suspend region and commit merge.
// 4. the loss of shutdown region doesn't matters. PD will tell from region to shutdown after
//    region merge is done.
// 5. If the into peer becomes leader after committing merge, and the from peer in the same store
//    is a slow follower which is not in suspended state, both peers will be destroyed.
//    And the region merge procedure will be finished even that.
// 6. The Raft follower message dispatching works for from region, no matter for suspend region or
//    shutdown region.

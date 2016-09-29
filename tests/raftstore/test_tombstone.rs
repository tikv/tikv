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

use std::time::Duration;

use rocksdb::Writable;

use kvproto::raft_serverpb::{self, RegionLocalState, PeerState};
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::transport_simulate::*;
use super::server::new_server_cluster;
use super::util::*;
use tikv::raftstore::store::{keys, Peekable};

fn test_tombstone<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();

    let r1 = cluster.run_conf_change();

    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, b"k1", b"v1");

    // add peer (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");

    // Remove peer (2, 2) from region 1.
    pd_client.must_remove_peer(r1, new_peer(2, 2));

    // After new leader is elected, the change peer must be finished.
    cluster.leader_of_region(r1).unwrap();
    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, b"k1");
    must_get_none(&engine_2, b"k3");

    // Send a stale raft message to peer (2, 2)
    let mut raft_msg = raft_serverpb::RaftMessage::new();

    raft_msg.set_region_id(r1);
    // Use an invalid from peer to ignore gc peer message.
    raft_msg.set_from_peer(new_peer(0, 0));
    raft_msg.set_to_peer(new_peer(2, 2));
    raft_msg.mut_region_epoch().set_conf_ver(0);
    raft_msg.mut_region_epoch().set_version(0);

    cluster.send_raft_msg(raft_msg).unwrap();

    // We must get RegionNotFound error.
    let region_status = new_status_request(r1, new_peer(2, 2), new_region_leader_cmd());
    let resp = cluster.call_command(region_status, Duration::from_secs(5)).unwrap();
    assert!(resp.get_header().get_error().has_region_not_found(),
            format!("region must not found, but got {:?}", resp));
}

#[test]
fn test_node_tombstone() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_tombstone(&mut cluster);
}

#[test]
fn test_server_tombstone() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_tombstone(&mut cluster);
}

fn test_fast_destroy<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();

    // Disable default max peer number check.
    pd_client.disable_default_rule();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");
    // remove peer (3, 3)
    pd_client.must_remove_peer(1, new_peer(3, 3));

    must_get_none(&engine_3, b"k1");

    cluster.stop_node(3);

    let key = keys::region_state_key(1);
    let state: RegionLocalState = engine_3.get_msg(&key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);

    // Force add some dirty data.
    engine_3.put(&keys::data_key(b"k0"), b"v0").unwrap();

    cluster.must_put(b"k2", b"v2");

    // start node again.
    cluster.run_node(3);

    // add new peer in node 3
    pd_client.must_add_peer(1, new_peer(3, 4));

    must_get_equal(&engine_3, b"k2", b"v2");
    // the dirty data must be cleared up.
    must_get_none(&engine_3, b"k0");
}

#[test]
fn test_node_fast_destroy() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_fast_destroy(&mut cluster);
}

#[test]
fn test_server_fast_destroy() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_fast_destroy(&mut cluster);
}

fn test_readd_peer<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();

    let r1 = cluster.run_conf_change();

    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, b"k1", b"v1");

    // add peer (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");

    cluster.add_send_filter(IsolationFilterFactory::new(2));

    // Remove peer (2, 2) from region 1.
    pd_client.must_remove_peer(r1, new_peer(2, 2));

    // After new leader is elected, the change peer must be finished.
    cluster.leader_of_region(r1).unwrap();
    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    pd_client.must_add_peer(r1, new_peer(2, 4));

    cluster.clear_send_filters();
    cluster.must_put(b"k4", b"v4");
    let engine = cluster.get_engine(2);
    must_get_equal(&engine, b"k4", b"v4");
}

#[test]
fn test_node_readd_peer() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_readd_peer(&mut cluster);
}

#[test]
fn test_server_readd_peer() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_readd_peer(&mut cluster);
}

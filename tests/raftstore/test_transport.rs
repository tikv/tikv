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

use std::sync::Arc;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util::{must_get_equal, sleep_ms};
use super::test_multi::wait_until_node_online;
use std::collections::HashSet;

fn split_leader_with_majority(leader: u64, count: u64) -> (HashSet<u64>, HashSet<u64>) {
    if leader <= (count + 1) / 2 {
        ((1..count / 2 + 2).collect(), (count / 2 + 2..count + 1).collect())
    } else {
        (((count + 1) / 2..count + 1).collect(), (1..(count + 1) / 2).collect())
    }
}

fn split_leader_with_minority(leader: u64, count: u64) -> (HashSet<u64>, HashSet<u64>) {
    let mut vec: Vec<u64> = (1..count + 1).collect();
    vec.swap(0, (leader - 1) as usize);
    let ucount = count as usize;
    let s1: HashSet<_> = vec[0..(ucount - 1) / 2].into_iter().cloned().collect();
    let s2: HashSet<_> = vec[(ucount - 1) / 2..ucount].into_iter().cloned().collect();
    (s1, s2)
}

fn test_partition_write<T: Simulator>(cluster: &mut Cluster<T>, count: u64) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    let (key, value) = (b"k1", b"v1");
    cluster.must_delete(key);

    let region_id = cluster.get_region_id(key);
    let peer = cluster.leader_of_region(region_id).unwrap();
    let leader = peer.get_store_id();

    // leader partition with majority doesn't affect write/read
    let (s1, s2) = split_leader_with_majority(leader, count);
    cluster.partition(Arc::new(s1), Arc::new(s2));
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    cluster.reset_transport_hooks();

    // if leader partition with minority, new leader should be elected
    let (s1, s2) = split_leader_with_minority(leader, count);
    cluster.partition(Arc::new(s1), Arc::new(s2));
    cluster.must_put(key, b"v2");
    assert_eq!(cluster.get(key), Some(b"v2".to_vec()));
    cluster.must_delete(key);
    assert_eq!(cluster.get(key), None);

    // when network recover, old leader should sync data
    cluster.reset_transport_hooks();
    wait_until_node_online(cluster, leader);
    must_get_equal(&cluster.get_engine(leader), key, b"v2");
}

#[test]
fn test_node_partition_write() {
    let mut cluster = new_node_cluster(0, 5);
    test_partition_write(&mut cluster, 5);
}

#[test]
fn test_server_partition_write() {
    let mut cluster = new_server_cluster(0, 5);
    test_partition_write(&mut cluster, 5);
}

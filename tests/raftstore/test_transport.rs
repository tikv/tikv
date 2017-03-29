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

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util::{must_get_equal, new_peer};

fn test_partition_write<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    must_get_equal(&cluster.engines[&1], key, value);

    let region_id = cluster.get_region_id(key);

    // transfer leader to (1, 1)
    cluster.must_transfer_leader(region_id, new_peer(1, 1));

    // leader in majority, partition doesn't affect write/read
    cluster.partition(vec![1, 2, 3], vec![4, 5]);
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    cluster.must_transfer_leader(region_id, new_peer(1, 1));
    cluster.clear_send_filters();

    // leader in minority, new leader should be elected
    cluster.partition(vec![1, 2], vec![3, 4, 5]);
    assert_eq!(cluster.must_get(key), Some(value.to_vec()));
    assert_ne!(cluster.leader_of_region(region_id).unwrap().get_id(), 1);
    assert_ne!(cluster.leader_of_region(region_id).unwrap().get_id(), 2);
    cluster.must_put(key, b"changed");
    cluster.clear_send_filters();

    // when network recover, old leader should sync data
    cluster.reset_leader_of_region(region_id);
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), key, b"changed");
}

#[test]
fn test_node_partition_write() {
    let mut cluster = new_node_cluster(0, 5);
    test_partition_write(&mut cluster);
}

#[test]
fn test_server_partition_write() {
    let mut cluster = new_server_cluster(0, 5);
    test_partition_write(&mut cluster);
}

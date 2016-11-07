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

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;

fn test_simple_region_merge<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let pd_client = cluster.pd_client.clone();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    let region = pd_client.get_region(b"k1").unwrap();

    cluster.must_split(&region, b"k2");

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k2").unwrap();
    assert!(r1 != r2);

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

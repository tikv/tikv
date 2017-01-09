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

fn test_put<T: Simulator>(cluster: &mut Cluster<T>) {
    let max_size: usize = 1024;
    cluster.cfg.raft_store.raft_entry_max_size = max_size as u64;

    cluster.run();

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
        let v = cluster.get(key);
        assert_eq!(v, Some(value.to_vec()));
    }

    // construct large raft entry
    let mut large_value: Vec<u8> = Vec::with_capacity(max_size + 1);
    for _ in 0..max_size + 1 {
        large_value.push(b'v');
    }

    let res = cluster.put(b"key", large_value.as_slice());
    assert!(res.as_ref().err().unwrap().has_raft_entry_too_large());
}

#[test]
fn test_node_put_with_large_entry() {
    let mut cluster = new_node_cluster(0, 1);
    test_put(&mut cluster);
}

#[test]
fn test_server_put_with_large_entry() {
    let mut cluster = new_server_cluster(0, 1);
    test_put(&mut cluster);
}

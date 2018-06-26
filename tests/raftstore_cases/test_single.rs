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

use tikv::util::config::*;

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util::*;

// TODO add stale epoch test cases.

fn test_put<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
        let v = cluster.get(key);
        assert_eq!(v, Some(value.to_vec()));
    }
    // value should be overwrited.
    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i + 1));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
        let v = cluster.get(key);
        assert_eq!(v, Some(value.to_vec()));
    }
}

fn test_delete<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
        let v = cluster.get(key);
        assert_eq!(v, Some(value.to_vec()));
    }

    for i in 1..1000 {
        let k = format!("key{}", i);
        let key = k.as_bytes();
        cluster.must_delete(key);
        assert!(cluster.get(key).is_none());
    }
}

fn test_delete_range<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let cf = "lock";

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put_cf(cf, key, value);
        let v = cluster.get_cf(cf, key);
        assert_eq!(v, Some(value.to_vec()));
    }

    cluster.must_delete_range_cf(cf, b"key1", b"key9999");

    for i in 1..1000 {
        let k = format!("key{}", i);
        let key = k.as_bytes();
        assert!(cluster.get_cf(cf, key).is_none());
    }
}

fn test_wrong_store_id<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let (k, v) = (b"k", b"v");
    let mut region = cluster.get_region(k);
    let region_id = region.get_id();
    let cmd = new_put_cmd(k, v);
    let mut req = new_request(region_id, region.take_region_epoch(), vec![cmd], true);
    let mut leader = cluster.leader_of_region(region_id).unwrap();
    // setup wrong store id.
    let store_id = leader.get_store_id();
    leader.set_store_id(store_id + 1);
    req.mut_header().set_peer(leader);
    let result = cluster.call_command_on_node(store_id, req, Duration::from_secs(5));
    assert!(
        !result
            .unwrap()
            .get_header()
            .get_error()
            .get_message()
            .is_empty()
    );
}

fn test_put_large_entry<T: Simulator>(cluster: &mut Cluster<T>) {
    let max_size: usize = 1024;
    cluster.cfg.raft_store.raft_entry_max_size = ReadableSize(max_size as u64);

    cluster.run();

    let large_value = vec![b'v'; max_size + 1];
    let res = cluster.put(b"key", large_value.as_slice());
    assert!(res.as_ref().err().unwrap().has_raft_entry_too_large());
}

#[test]
fn test_node_put() {
    let mut cluster = new_node_cluster(0, 1);
    test_put(&mut cluster);
}

#[test]
fn test_node_delete() {
    let mut cluster = new_node_cluster(0, 1);
    test_delete(&mut cluster);
}

#[test]
fn test_node_delete_range() {
    let mut cluster = new_node_cluster(0, 1);
    test_delete_range(&mut cluster);
}

#[test]
fn test_node_wrong_store_id() {
    let mut cluster = new_node_cluster(0, 1);
    test_wrong_store_id(&mut cluster);
}

#[test]
fn test_server_put() {
    let mut cluster = new_server_cluster(0, 1);
    test_put(&mut cluster);
}

#[test]
fn test_server_delete() {
    let mut cluster = new_server_cluster(0, 1);
    test_delete(&mut cluster);
}

#[test]
fn test_server_delete_range() {
    let mut cluster = new_server_cluster(0, 1);
    test_delete_range(&mut cluster);
}

#[test]
fn test_server_wrong_store_id() {
    let mut cluster = new_server_cluster(0, 1);
    test_wrong_store_id(&mut cluster);
}

#[test]
fn test_node_put_large_entry() {
    let mut cluster = new_node_cluster(0, 1);
    test_put_large_entry(&mut cluster);
}

#[test]
fn test_server_put_large_entry() {
    let mut cluster = new_server_cluster(0, 1);
    test_put_large_entry(&mut cluster);
}

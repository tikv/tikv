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
use tikv::raftstore::store::*;
use super::node::new_node_cluster;
use super::server::new_server_cluster;

// TODO add stale epoch test cases.

fn test_put<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.put(key, value);
        let v = cluster.get(key);
        assert_eq!(v, Some(value.to_vec()));
    }
    // value should be overwrited.
    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i + 1));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.put(key, value);
        let v = cluster.get(key);
        assert_eq!(v, Some(value.to_vec()));
    }
}

fn test_delete<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.put(key, value);
        let v = cluster.get(key);
        assert_eq!(v, Some(value.to_vec()));
    }

    for i in 1..1000 {
        let k = format!("key{}", i);
        let key = k.as_bytes();
        cluster.delete(key);
        assert!(cluster.get(key).is_none());
    }
}

fn test_seek<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    for i in 100..200 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.put(key, value);
    }

    for i in 0..100 {
        let k = format!("key{:03}", i);
        let key = k.as_bytes();
        let (k, v) = cluster.seek(key).unwrap();
        assert_eq!(k, b"key100");
        assert_eq!(v, b"value100");
    }

    for i in 100..200 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        let (sk, sv) = cluster.seek(key).unwrap();
        assert_eq!(sk, key);
        assert_eq!(sv, value);
    }

    for i in 200..300 {
        let k = format!("key{}", i);
        let key = k.as_bytes();
        assert!(cluster.seek(key).is_none());
    }

    assert!(cluster.seek(b"key2").is_none(),
            "seek should follow binary order");
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
fn test_node_seek() {
    let mut cluster = new_node_cluster(0, 1);
    test_seek(&mut cluster);
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
fn test_server_seek() {
    let mut cluster = new_server_cluster(0, 1);
    test_seek(&mut cluster);
}

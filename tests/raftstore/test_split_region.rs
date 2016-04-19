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
use std::thread;
use std::cmp;
use rand::{self, Rng};

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util;
use tikv::pd::PdClient;
use tikv::util::HandyRwLock;
use tikv::raftstore::store::keys::data_key;
use tikv::raftstore::store::engine::Iterable;
use tikv::util::codec::bytes;

pub const REGION_MAX_SIZE: u64 = 50000;
pub const REGION_SPLIT_SIZE: u64 = 30000;

fn test_base_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_log();

    cluster.bootstrap_region().expect("");
    cluster.start();

    let pd_client = cluster.pd_client.clone();
    let cluster_id = cluster.id();

    let region = pd_client.rl().get_region(cluster_id, b"").unwrap();

    let a1 = bytes::encode_bytes(b"a1");
    cluster.put(&a1, b"v1");

    let a3 = bytes::encode_bytes(b"a3");
    cluster.put(&a3, b"v3");

    let a2 = bytes::encode_bytes(b"a2");
    // Split with a2, so a1 must in left, and a3 in right.
    cluster.split_region(region.get_id(), Some(a2.clone()));

    let left = pd_client.rl().get_region(cluster_id, &a1).unwrap();
    let right = pd_client.rl().get_region(cluster_id, &a3).unwrap();

    assert_eq!(region.get_id(), left.get_id());
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(left.get_end_key(), right.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());

    cluster.put(&a1, b"vv1");
    assert_eq!(cluster.get(&a1).unwrap(), b"vv1".to_vec());

    cluster.put(&a3, b"vv3");
    assert_eq!(cluster.get(&a3).unwrap(), b"vv3".to_vec());

    let epoch = left.get_region_epoch().clone();
    let get = util::new_request(left.get_id(), epoch, vec![util::new_get_cmd(&a3)]);
    let region_id = left.get_id();
    let resp = cluster.call_command_on_leader(region_id, get, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error());
    assert!(resp.get_header().get_error().has_key_not_in_region());
}

#[test]
fn test_node_base_split_region() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_base_split_region(&mut cluster);
}

#[test]
fn test_server_base_split_region() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_base_split_region(&mut cluster);
}

/// Keep puting random kvs until specified size limit is reached.
fn put_till_size<T: Simulator>(cluster: &mut Cluster<T>,
                               limit: u64,
                               range: &mut Iterator<Item = u64>)
                               -> Vec<u8> {
    let mut len = 0;
    let mut rng = rand::thread_rng();
    let mut max_key = vec![];
    while len < limit {
        let key = bytes::encode_bytes(&range.next().unwrap().to_string().into_bytes());
        let mut value = vec![0; 64];
        rng.fill_bytes(&mut value);
        cluster.put(&key, &value);
        // plus 1 for the extra encoding prefix
        len += key.len() as u64 + 1;
        len += value.len() as u64;
        if max_key < key {
            max_key = key;
        }
    }
    max_key
}

fn test_auto_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.store_cfg.split_region_check_tick_interval = 100;
    cluster.cfg.store_cfg.region_max_size = REGION_MAX_SIZE;
    cluster.cfg.store_cfg.region_split_size = REGION_SPLIT_SIZE;

    let check_size_diff = cluster.cfg.store_cfg.region_check_size_diff;
    let mut range = 1..;

    cluster.bootstrap_region().expect("");
    cluster.start();

    let pd_client = cluster.pd_client.clone();
    let cluster_id = cluster.id();

    let region = pd_client.rl().get_region(cluster_id, b"").unwrap();

    let last_key = put_till_size(cluster, REGION_SPLIT_SIZE, &mut range);

    // it should be finished in millis if split.
    thread::sleep(Duration::from_secs(1));

    let target = pd_client.rl().get_region(cluster_id, &last_key).unwrap();

    assert_eq!(region, target);

    let final_key = put_till_size(cluster,
                                  REGION_MAX_SIZE - REGION_SPLIT_SIZE + check_size_diff,
                                  &mut range);
    let max_key = cmp::max(last_key, final_key);

    thread::sleep(Duration::from_secs(1));

    let left = pd_client.rl().get_region(cluster_id, b"").unwrap();
    let right = pd_client.rl().get_region(cluster_id, &max_key).unwrap();

    assert!(left != right);
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(right.get_start_key(), left.get_end_key());
    assert_eq!(region.get_end_key(), right.get_end_key());
    assert_eq!(pd_client.rl().get_region(cluster_id, &max_key).unwrap(),
               right);
    assert_eq!(pd_client.rl().get_region(cluster_id, left.get_end_key()).unwrap(),
               right);

    let middle_key = left.get_end_key();
    let leader = cluster.leader_of_region(left.get_id()).unwrap();
    let store_id = leader.get_store_id();
    let mut size = 0;
    cluster.engines[&store_id]
        .scan(&data_key(b""),
              &data_key(middle_key),
              &mut |k, v| {
                  size += k.len() as u64;
                  size += v.len() as u64;
                  Ok(true)
              })
        .expect("");
    assert!(size <= REGION_SPLIT_SIZE);
    // although size may be smaller than util::REGION_SPLIT_SIZE, but the diff should
    // be small.
    assert!(size > REGION_SPLIT_SIZE - 1000);

    let epoch = left.get_region_epoch().clone();
    let get = util::new_request(left.get_id(), epoch, vec![util::new_get_cmd(&max_key)]);
    let region_id = left.get_id();
    let resp = cluster.call_command_on_leader(region_id, get, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error());
    assert!(resp.get_header().get_error().has_key_not_in_region());
}

#[test]
fn test_node_auto_split_region() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

#[test]
fn test_server_auto_split_region() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

fn test_delay_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    // We use three nodes for this test.
    cluster.bootstrap_region().expect("");
    cluster.start();

    let pd_client = cluster.pd_client.clone();
    let cluster_id = cluster.id();

    let region = pd_client.rl().get_region(cluster_id, b"").unwrap();

    let a1 = bytes::encode_bytes(b"a1");
    cluster.put(&a1, b"v1");

    let a3 = bytes::encode_bytes(b"a3");
    cluster.put(&a3, b"v3");

    // check all nodes apply the logs.
    for i in 0..3 {
        let engine = cluster.get_engine(i + 1);
        util::must_get_equal(&engine, &a1, b"v1");
        util::must_get_equal(&engine, &a3, b"v3");
    }

    let leader = cluster.leader_of_region(region.get_id()).unwrap();

    // Stop a not leader peer
    let index = (1..4).find(|&x| x != leader.get_store_id()).unwrap();
    cluster.stop_node(index);

    let a2 = bytes::encode_bytes(b"a20");
    cluster.split_region(region.get_id(), Some(a2.clone()));

    // When the node starts, the region will try to join the raft group first,
    // so most of case, the new leader's heartbeat for split region may arrive
    // before applying the log.
    cluster.run_node(index);

    // Wait a long time to guarantee node joined.
    // TODO: we should think a better to check instead of sleep.
    util::sleep_ms(3000);

    let a4 = bytes::encode_bytes(b"a4");
    cluster.put(&a4, b"v4");

    assert_eq!(cluster.get(&a4).unwrap(), b"v4".to_vec());

    let engine = cluster.get_engine(index);
    util::must_get_equal(&engine, &a4, b"v4");
}

#[test]
fn test_node_delay_split_region() {
    let mut cluster = new_node_cluster(0, 3);
    test_delay_split_region(&mut cluster);
}

#[test]
fn test_server_delay_split_region() {
    let mut cluster = new_server_cluster(0, 3);
    test_delay_split_region(&mut cluster);
}

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
use std::sync::atomic::{AtomicUsize, Ordering};

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util;
use super::transport_simulate::*;
use tikv::pd::PdClient;
use tikv::raftstore::store::keys::data_key;
use tikv::raftstore::store::engine::Iterable;
use kvproto::raftpb;
use kvproto::raft_serverpb::RaftMessage;

pub const REGION_MAX_SIZE: u64 = 50000;
pub const REGION_SPLIT_SIZE: u64 = 30000;

fn test_base_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_log();

    cluster.bootstrap_region().expect("");
    cluster.start();

    let pd_client = cluster.pd_client.clone();

    let tbls = vec![(b"a22", b"a11", b"a33"), (b"a11", b"a00", b"a11"), (b"a33", b"a22", b"a33")];

    for (split_key, left_key, right_key) in tbls {
        cluster.must_put(left_key, b"v1");
        cluster.must_put(right_key, b"v3");

        // Left and right key must be in same region before split.
        let region = pd_client.get_region(left_key).unwrap();
        let region2 = pd_client.get_region(right_key).unwrap();
        assert_eq!(region.get_id(), region2.get_id());

        // Split with split_key, so left_key must in left, and right_key in right.
        cluster.must_split(&region, split_key);

        let left = pd_client.get_region(left_key).unwrap();
        let right = pd_client.get_region(right_key).unwrap();

        assert_eq!(region.get_id(), left.get_id());
        assert_eq!(region.get_start_key(), left.get_start_key());
        assert_eq!(left.get_end_key(), right.get_start_key());
        assert_eq!(region.get_end_key(), right.get_end_key());

        cluster.must_put(left_key, b"vv1");
        assert_eq!(cluster.get(left_key).unwrap(), b"vv1".to_vec());

        cluster.must_put(right_key, b"vv3");
        assert_eq!(cluster.get(right_key).unwrap(), b"vv3".to_vec());

        let epoch = left.get_region_epoch().clone();
        let get = util::new_request(left.get_id(), epoch, vec![util::new_get_cmd(right_key)]);
        let resp = cluster.call_command_on_leader(get, Duration::from_secs(3)).unwrap();
        assert!(resp.get_header().has_error());
        assert!(resp.get_header().get_error().has_key_not_in_region(),
                format!("{:?}", resp));
    }
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
        let key = range.next().unwrap().to_string().into_bytes();
        let mut value = vec![0; 64];
        rng.fill_bytes(&mut value);
        cluster.must_put(&key, &value);
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

    let region = pd_client.get_region(b"").unwrap();

    let last_key = put_till_size(cluster, REGION_SPLIT_SIZE, &mut range);

    // it should be finished in millis if split.
    thread::sleep(Duration::from_secs(1));

    let target = pd_client.get_region(&last_key).unwrap();

    assert_eq!(region, target);

    let final_key = put_till_size(cluster,
                                  REGION_MAX_SIZE - REGION_SPLIT_SIZE + check_size_diff,
                                  &mut range);
    let max_key = cmp::max(last_key, final_key);

    thread::sleep(Duration::from_secs(1));

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();

    assert!(left != right);
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(right.get_start_key(), left.get_end_key());
    assert_eq!(region.get_end_key(), right.get_end_key());
    assert_eq!(pd_client.get_region(&max_key).unwrap(), right);
    assert_eq!(pd_client.get_region(left.get_end_key()).unwrap(), right);

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
    let resp = cluster.call_command_on_leader(get, Duration::from_secs(3)).unwrap();
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

    let region = pd_client.get_region(b"").unwrap();

    let a1 = b"a1";
    cluster.must_put(a1, b"v1");

    let a3 = b"a3";
    cluster.must_put(a3, b"v3");

    // check all nodes apply the logs.
    for i in 0..3 {
        let engine = cluster.get_engine(i + 1);
        util::must_get_equal(&engine, a1, b"v1");
        util::must_get_equal(&engine, a3, b"v3");
    }

    let leader = cluster.leader_of_region(region.get_id()).unwrap();

    // Stop a not leader peer
    let index = (1..4).find(|&x| x != leader.get_store_id()).unwrap();
    cluster.stop_node(index);

    let a2 = b"a20";
    cluster.must_split(&region, a2);

    // When the node starts, the region will try to join the raft group first,
    // so most of case, the new leader's heartbeat for split region may arrive
    // before applying the log.
    cluster.run_node(index);

    // Wait a long time to guarantee node joined.
    // TODO: we should think a better to check instead of sleep.
    util::sleep_ms(3000);

    let a4 = b"a4";
    cluster.must_put(a4, b"v4");

    assert_eq!(cluster.get(a4).unwrap(), b"v4".to_vec());

    let engine = cluster.get_engine(index);
    util::must_get_equal(&engine, a4, b"v4");
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

struct DropSnapshotFailure {
    count: AtomicUsize,
}

impl Filter for DropSnapshotFailure {
    fn before(&self, m: &RaftMessage) -> bool {
        if m.get_message().get_msg_type() != raftpb::MessageType::MsgSnapStatus {
            return false;
        }

        // ignore the first snapshot failure message
        self.count.fetch_add(1, Ordering::SeqCst) < 1
    }
}

fn test_split_overlap_snapshot<T: Simulator>(cluster: &mut Cluster<T>,
                                             drop_snapshot_failure: bool) {
    // We use three nodes 1, 2, 3 for this test.
    cluster.bootstrap_region().expect("");
    cluster.start();

    // guarantee 1 is leader
    cluster.transfer_leader(1, util::new_peer(1, 1));
    cluster.must_put(b"k0", b"v0");
    assert_eq!(cluster.leader_of_region(1), Some(util::new_peer(1, 1)));

    let pd_client = cluster.pd_client.clone();

    // isolate 3 for region 1.
    cluster.hook_transport(IsolateRegionStore::new(1, 3));
    cluster.must_put(b"k1", b"v1");

    if drop_snapshot_failure {
        // when region 1 split, node 1 will send snapshot to node 3 for region 2,
        // but node 3 will returns a snapshot failure message. We should drop
        // the first one to guarantee node 1 sends heartbeat to node 3, and then
        // we can return snapshot failure message again.
        cluster.hook_node_transport(3,
                                    vec![box FilterRegionPacket {
                                             region_id: 1,
                                             store_id: 3,
                                         },
                                         box DropSnapshotFailure { count: AtomicUsize::new(0) }]);
    }

    let region = pd_client.get_region(b"").unwrap();

    // split (-inf, +inf) -> (-inf, k2), [k2, +inf]
    cluster.must_split(&region, b"k2");

    cluster.must_put(b"k2", b"v2");

    // node 1 and node 2 must have k2, but node 3 must not.
    for i in 1..3 {
        let engine = cluster.get_engine(i);
        util::must_get_equal(&engine, b"k2", b"v2");
    }

    let engine = cluster.get_engine(3);
    util::must_get_none(&engine, b"k2");

    cluster.reset_transport_hooks();
    cluster.must_put(b"k3", b"v3");

    util::sleep_ms(3000);
    // node 3 must have k3.
    util::must_get_equal(&engine, b"k3", b"v3");
}

#[test]
fn test_node_split_overlap_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    test_split_overlap_snapshot(&mut cluster, false);
}

#[test]
fn test_server_split_overlap_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    test_split_overlap_snapshot(&mut cluster, false);
}

#[test]
fn test_node_split_overlap_snapshot_with_drop() {
    let mut cluster = new_node_cluster(0, 3);
    test_split_overlap_snapshot(&mut cluster, true);
}

#[test]
fn test_server_split_overlap_snapshot_with_drop() {
    let mut cluster = new_server_cluster(0, 3);
    test_split_overlap_snapshot(&mut cluster, true);
}

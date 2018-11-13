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
use std::time::Duration;
use std::{thread, time};

use test_raftstore::*;
use tikv::pd::PdClient;
use tikv::storage::CF_WRITE;
use tikv::util::config::*;

fn flush<T: Simulator>(cluster: &mut Cluster<T>) {
    for engines in cluster.engines.values() {
        engines.kv.flush(true).unwrap();
    }
}

fn test_update_regoin_size<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.region_split_check_diff = ReadableSize::kb(1);
    cluster
        .cfg
        .rocksdb
        .defaultcf
        .level0_file_num_compaction_trigger = 10;
    cluster.start();

    for _ in 0..2 {
        for i in 0..1000 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }
        flush(cluster);
        for i in 1000..2000 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }
        flush(cluster);
        for i in 2000..3000 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            cluster.must_put(k.as_bytes(), v.as_bytes());
        }
        flush(cluster);
    }

    // Make sure there are multiple regions, so it will cover all cases of
    // function `raftstore.on_compaction_finished`.
    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"").unwrap();
    cluster.must_split(&region, b"k2000");

    thread::sleep(time::Duration::from_millis(300));
    let region_id = cluster.get_region_id(b"");
    let old_region_size = cluster
        .pd_client
        .get_region_approximate_size(region_id)
        .unwrap();

    cluster.compact_data();

    thread::sleep(time::Duration::from_millis(300));
    let new_region_size = cluster
        .pd_client
        .get_region_approximate_size(region_id)
        .unwrap();

    assert_ne!(old_region_size, new_region_size);
}

#[test]
fn test_server_update_region_size() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    test_update_regoin_size(&mut cluster);
}

/// Test whether approximate size and keys are updated after transfer leader
#[test]
fn test_transfer_leader_approximate_size_and_keys() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.region_split_check_diff = ReadableSize(100);
    cluster.run();

    let mut range = 1..;
    put_cf_till_size(&mut cluster, CF_WRITE, 100, &mut range);

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"").unwrap();

    // make sure all peer's approximate size is not None.
    cluster.must_transfer_leader(region.get_id(), region.get_peers()[0].clone());
    thread::sleep(Duration::from_millis(100));
    cluster.must_transfer_leader(region.get_id(), region.get_peers()[1].clone());
    thread::sleep(Duration::from_millis(100));
    cluster.must_transfer_leader(region.get_id(), region.get_peers()[2].clone());
    thread::sleep(Duration::from_millis(100));

    let old_size = pd_client
        .get_region_approximate_size(region.get_id())
        .unwrap();
    assert_ne!(old_size, 0);

    cluster.must_transfer_leader(region.get_id(), region.get_peers()[0].clone());

    // make sure split check is invoked so size and keys are updated.
    put_cf_till_size(&mut cluster, CF_WRITE, 100, &mut range);
    thread::sleep(Duration::from_millis(100));
    let new_size = pd_client
        .get_region_approximate_size(region.get_id())
        .unwrap();
    assert_ne!(old_size, new_size);

    // make sure split check is invoked so size and keys are updated.
    cluster.must_transfer_leader(region.get_id(), region.get_peers()[2].clone());
    thread::sleep(Duration::from_millis(100));

    // size and keys should be updated
    let new_size = pd_client
        .get_region_approximate_size(region.get_id())
        .unwrap();
    assert_ne!(old_size, new_size);
}

/// Test whether approximate size and keys are updated after merge
#[test]
fn test_merge_approximate_size_and_keys() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(20);
    cluster.run();

    let mut range = 1..;
    let middle_key = put_cf_till_size(&mut cluster, CF_WRITE, 100, &mut range);
    let max_key = put_cf_till_size(&mut cluster, CF_WRITE, 100, &mut range);

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"").unwrap();

    cluster.must_split(&region, &middle_key);
    // make sure split check is invoked so size and keys are updated.
    thread::sleep(Duration::from_millis(100));

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();
    assert_ne!(left, right);

    // make sure all peer's approximate size is not None.
    cluster.must_transfer_leader(right.get_id(), right.get_peers()[0].clone());
    thread::sleep(Duration::from_millis(100));
    cluster.must_transfer_leader(right.get_id(), right.get_peers()[1].clone());
    thread::sleep(Duration::from_millis(100));
    cluster.must_transfer_leader(right.get_id(), right.get_peers()[2].clone());
    thread::sleep(Duration::from_millis(100));

    let size = pd_client
        .get_region_approximate_size(right.get_id())
        .unwrap();
    assert_ne!(size, 0);
    let keys = pd_client
        .get_region_approximate_keys(right.get_id())
        .unwrap();
    assert_ne!(keys, 0);

    pd_client.must_merge(left.get_id(), right.get_id());
    // make sure split check is invoked so size and keys are updated.
    thread::sleep(Duration::from_millis(100));

    let region = pd_client.get_region(b"").unwrap();
    // size and keys should be updated.
    assert_ne!(
        pd_client
            .get_region_approximate_size(region.get_id())
            .unwrap(),
        size
    );
    assert_ne!(
        pd_client
            .get_region_approximate_keys(region.get_id())
            .unwrap(),
        keys
    );

    // after merge and then transfer leader, if not update new leader's approximate size, it maybe be stale.
    cluster.must_transfer_leader(region.get_id(), region.get_peers()[0].clone());
    // make sure split check is invoked
    thread::sleep(Duration::from_millis(100));
    assert_ne!(
        pd_client
            .get_region_approximate_size(region.get_id())
            .unwrap(),
        size
    );
    assert_ne!(
        pd_client
            .get_region_approximate_keys(region.get_id())
            .unwrap(),
        keys
    );
}

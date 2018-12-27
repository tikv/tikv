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
use std::{thread, time};

use test_raftstore::*;
use tikv::pd::PdClient;
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

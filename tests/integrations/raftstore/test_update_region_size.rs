// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time};

use engine_traits::MiscExt;
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::config::*;

fn flush<T: Simulator>(cluster: &mut Cluster<T>) {
    for engines in cluster.engines.values() {
        engines.kv.flush(true).unwrap();
    }
}

fn test_update_region_size<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.region_split_check_diff = ReadableSize::kb(1);
    cluster
        .cfg
        .rocksdb
        .defaultcf
        .level0_file_num_compaction_trigger = 10;
    cluster.start().unwrap();

    let batch_put = |cluster: &mut Cluster<T>, mut start, end| {
        while start < end {
            let next = std::cmp::min(end, start + 50);
            let requests = (start..next)
                .map(|i| {
                    new_put_cmd(
                        &format!("k{}", i).into_bytes(),
                        &format!("value{}", i).into_bytes(),
                    )
                })
                .collect();
            cluster
                .batch_put(&format!("k{}", start).into_bytes(), requests)
                .unwrap();
            start = next;
        }
    };

    for _ in 0..2 {
        batch_put(cluster, 0, 1000);
        flush(cluster);
        batch_put(cluster, 1000, 2000);
        flush(cluster);
        batch_put(cluster, 2000, 3000);
        flush(cluster);
    }

    // Make sure there are multiple regions, so it will cover all cases of
    // function `raftstore.on_compaction_finished`.
    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"").unwrap();
    cluster.must_split(&region, b"k2000");

    thread::sleep(time::Duration::from_millis(500));
    let region_id = cluster.get_region_id(b"");
    let old_region_size = cluster
        .pd_client
        .get_region_approximate_size(region_id)
        .unwrap();

    cluster.compact_data();

    thread::sleep(time::Duration::from_millis(500));
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
    test_update_region_size(&mut cluster);
}

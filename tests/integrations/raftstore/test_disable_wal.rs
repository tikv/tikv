// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{MiscExt, CF_DEFAULT};
use test_raftstore::*;

#[test]
fn test_disable_wal_recovery_basic() {
    let mut cluster = new_server_cluster(0, 1);
    // Initialize the cluster.
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.disable_kv_wal = true;
    cluster.run();
    let region = cluster.get_region(b"");
    let leader = region.get_peers().iter().find(|p| p.store_id == 1).unwrap();
    cluster.must_transfer_leader(region.get_id(), leader.clone());

    cluster.must_put(b"k1", b"v1");
    cluster.get_engine(1).flush_cfs(true).unwrap();
    cluster.must_put(b"k2", b"v2");

    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");

    cluster.must_split(&region, b"k3");
    cluster.must_put(b"k4", b"v4");
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k4", b"v4");
    let region = cluster.get_region(b"k4");
    cluster.must_split(&region, b"k5");
    cluster.must_put(b"k5", b"v5");
}

#[test]
fn test_disable_wal_recovery_split_merge() {
    let mut cluster = new_server_cluster(0, 3);
    // Initialize the cluster.
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.disable_kv_wal = true;
    cluster.run();
    cluster.must_put(b"k1", b"v1");

    cluster.must_split(&cluster.get_region(b""), b"k5");
    cluster.get_engine(1).flush_cfs(true).unwrap();
    cluster.must_split(&cluster.get_region(b""), b"k3");
    cluster
        .pd_client
        .must_merge(cluster.get_region_id(b"k3"), cluster.get_region_id(b"k5"));
    cluster
        .pd_client
        .must_merge(cluster.get_region_id(b""), cluster.get_region_id(b"k5"));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    cluster.get_engine(1).flush_cf(CF_DEFAULT, true).unwrap();
    sleep_ms(100);
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    assert_eq!(cluster.get_region_id(b"k3"), cluster.get_region_id(b"k5"));
    assert_eq!(cluster.get_region_id(b""), cluster.get_region_id(b"k5"));
    sleep_ms(100);
    cluster.must_split(&cluster.get_region(b""), b"k3");
}

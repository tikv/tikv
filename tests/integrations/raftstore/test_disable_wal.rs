// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{MiscExt, Mutable, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT};
use raftstore::coprocessor::ConsistencyCheckMethod;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

#[test]
fn test_disable_wal_recovery_basic() {
    let mut cluster = new_server_cluster(0, 1);
    // Initialize the cluster.
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.disable_kv_wal = true;
    cluster.run();
    let engine = cluster.get_engine(1);

    cluster.must_put(b"k1", b"v1");
    engine.flush_cfs(true).unwrap();
    cluster.must_put(b"k2", b"v2");

    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");

    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k3");
    cluster.must_put(b"k4", b"v4");
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k4", b"v4");
    let region = cluster.get_region(b"k4");
    cluster.must_split(&region, b"k5");
    cluster.must_put(b"k5", b"v5");
    let mut write_opts = WriteOptions::default();
    write_opts.set_disable_wal(true);
    let mut wb = engine.write_batch();
    wb.put(b"k6", b"v6").unwrap();
    wb.write_opt(&write_opts).unwrap();
    engine.flush_cfs(true).unwrap();
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

    cluster.must_split(&cluster.get_region(b""), b"k1");
    cluster.must_split(&cluster.get_region(b"k1"), b"k2");
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
    cluster
        .pd_client
        .must_merge(cluster.get_region_id(b"k1"), cluster.get_region_id(b"k2"));
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    cluster
        .pd_client
        .must_merge(cluster.get_region_id(b""), cluster.get_region_id(b"k1"));
    assert_eq!(cluster.get_region_id(b"k1"), cluster.get_region_id(b"k2"));
    assert_eq!(cluster.get_region_id(b""), cluster.get_region_id(b"k1"));
}

#[test]
fn test_disable_wal_recovery_target_region_recreated() {
    let mut cluster = new_server_cluster(0, 3);
    // Initialize the cluster.
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.disable_kv_wal = true;
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");

    cluster.must_split(&cluster.get_region(b""), b"k3");
    cluster.get_engine(1).flush_cfs(true).unwrap();
    cluster.must_put(b"k3", b"v3");
    cluster
        .pd_client
        .must_merge(cluster.get_region_id(b"k3"), cluster.get_region_id(b""));
    cluster.must_split(&cluster.get_region(b""), b"k5");
    let peer = cluster
        .get_region(b"k5")
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 1)
        .cloned()
        .unwrap();
    cluster
        .pd_client
        .must_remove_peer(cluster.get_region_id(b"k5"), peer);
    cluster
        .pd_client
        .must_add_peer(cluster.get_region_id(b"k5"), new_peer(1, 10001));
    cluster.must_put(b"k5", b"v5");
    must_get_equal(&cluster.get_engine(1), b"k5", b"v5");
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k5", b"v5");
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
}

#[test]
fn test_disable_wal_recovery_consistency_check() {
    let mut cluster = new_server_cluster(0, 3);
    // Initialize the cluster.
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.disable_kv_wal = true;
    cluster.cfg.raft_store.consistency_check_interval = ReadableDuration::millis(50);
    cluster.cfg.coprocessor.consistency_check_method = ConsistencyCheckMethod::Raw;
    cluster.run();
    cluster.must_put(b"k1", b"v1");
    cluster.must_split(&cluster.get_region(b""), b"k3");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
    sleep_ms(100);
    cluster
        .pd_client
        .must_merge(cluster.get_region_id(b""), cluster.get_region_id(b"k3"));
    cluster.must_put(b"k4", b"v4");
    cluster.must_put(b"k5", b"v5");
    must_get_equal(&cluster.get_engine(1), b"k4", b"v4");
    must_get_equal(&cluster.get_engine(1), b"k5", b"v5");
    cluster.get_engine(1).flush_cf(CF_DEFAULT, true).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    sleep_ms(100);
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    cluster.must_put(b"k6", b"v6");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k4", b"v4");
    must_get_equal(&cluster.get_engine(1), b"k5", b"v5");
    must_get_equal(&cluster.get_engine(1), b"k6", b"v6");
}

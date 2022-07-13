// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use test_cloud_server::ServerCluster;
use tikv_util::config::{ReadableDuration, ReadableSize};

use crate::alloc_node_id;

#[test]
fn test_engine_auto_switch() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.rocksdb.writecf.write_buffer_size = ReadableSize::kb(256);
    });
    cluster.put_kv(0..100, i_to_key, i_to_val);
    cluster.put_kv(100..200, i_to_key, i_to_val);
    cluster.put_kv(200..300, i_to_key, i_to_val);
    let region_id = cluster.get_region_id(&[]);
    let engine = cluster.get_kvengine(node_id);
    let stats = engine.get_shard_stat(region_id);
    assert!(stats.mem_table_count + stats.l0_table_count > 1);
    cluster.stop();
}

fn i_to_key(i: usize) -> Vec<u8> {
    format!("key_{:03}", i).into_bytes()
}

fn i_to_val(i: usize) -> Vec<u8> {
    format!("val_{:03}", i).into_bytes().repeat(100)
}

#[test]
fn test_split_by_key() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.rocksdb.writecf.write_buffer_size = ReadableSize::kb(16);
        conf.rocksdb.writecf.target_file_size_base = ReadableSize::kb(16);
        conf.coprocessor.region_split_size = ReadableSize::kb(64);
        conf.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
        conf.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(20);
        conf.raft_store.split_region_check_tick_interval = ReadableDuration::millis(20);
    });

    cluster.put_kv(0..300, i_to_key, i_to_key);
    cluster.put_kv(300..600, i_to_key, i_to_key);
    cluster.put_kv(600..1000, i_to_key, i_to_key);
    // The split max keys should be 64 * 3 / 2 * 1024 / 100 ~= 983
    let engine = cluster.get_kvengine(node_id);
    for _ in 0..10 {
        if engine.get_all_shard_id_vers().len() > 1 {
            break;
        }
        sleep();
    }
    let shard_stats = engine.get_all_shard_stats();
    assert!(shard_stats.len() > 1);
    let total_size: u64 = shard_stats.iter().map(|s| s.total_size).sum();
    assert!(total_size < 64 * 1024);
    cluster.stop();
}

#[test]
fn test_remove_peer() {
    test_util::init_log_for_test();
    let node_ids = vec![alloc_node_id(), alloc_node_id(), alloc_node_id()];
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, _| {});
    cluster.wait_region_replicated(&[], 3);
    cluster.split(&i_to_key(5));
    // Wait for region heartbeat to update region epoch in PD.
    sleep();
    cluster.put_kv(0..10, i_to_key, i_to_key);
    let pd = cluster.get_pd_client();
    for _ in 0..100 {
        if pd.get_regions_number() == 2 {
            break;
        }
        sleep();
    }
    pd.disable_default_operator();
    cluster.remove_node_peers(*node_ids.first().unwrap());
    // After one store has removed peer, the cluster is still available.
    cluster.put_kv(0..10, i_to_key, i_to_key);
    cluster.stop();
}

fn sleep() {
    std::thread::sleep(Duration::from_millis(100));
}

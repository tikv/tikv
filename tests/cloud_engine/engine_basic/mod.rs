// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use pd_client::PdClient;
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
        std::thread::sleep(Duration::from_millis(100));
    }
    let shard_stats = engine.get_all_shard_stats();
    assert!(shard_stats.len() > 1);
    let total_size: u64 = shard_stats.iter().map(|s| s.total_size).sum();
    assert!(total_size < 64 * 1024);
    cluster.stop();
    // wait for compaction jobs done.
    // TODO(x): make kvengine stop every threads.
    std::thread::sleep(Duration::from_secs(1));
}

#[test]
fn test_remove_peer() {
    test_util::init_log_for_test();
    let node_ids = vec![alloc_node_id(), alloc_node_id(), alloc_node_id()];
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, _| {});
    cluster.split(&i_to_key(5));
    cluster.put_kv(0..10, i_to_key, i_to_key);
    let pd = cluster.get_pd_client();
    for _ in 0..100 {
        if pd.get_regions_number() == 2 {
            break;
        }
        sleep();
    }
    let stores = pd.get_stores().unwrap();
    for _ in 0..100 {
        let region_1 = pd.get_region(&[]).unwrap();
        let region_2 = pd.get_region(&i_to_key(5)).unwrap();
        if region_1.get_peers().len() == 3 && region_2.get_peers().len() == 3 {
            let store_1_peer = raftstore::store::util::find_peer(&region_1, stores[0].id).unwrap();
            let store_2_peer = raftstore::store::util::find_peer(&region_2, stores[1].id).unwrap();
            pd.remove_peer(region_1.id, store_1_peer.clone());
            pd.remove_peer(region_2.id, store_2_peer.clone());
            break;
        }
        sleep();
    }
    let mut removed = false;
    for _ in 0..100 {
        let region_1 = pd.get_region(&[]).unwrap();
        let region_2 = pd.get_region(&i_to_key(5)).unwrap();
        if region_1.get_peers().len() == 2 && region_2.get_peers().len() == 2 {
            removed = true;
            break;
        }
        sleep();
    }
    assert!(removed);
    // After two store has removed peer, the cluster is still available.
    cluster.put_kv(0..10, i_to_key, i_to_key);
    cluster.stop();
}

fn sleep() {
    std::thread::sleep(Duration::from_millis(100));
}

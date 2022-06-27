// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use test_cloud_server::ServerCluster;
use tikv_util::config::ReadableSize;

use crate::alloc_node_id;

#[test]
fn test_engine_auto_switch() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let cluster = ServerCluster::new(vec![node_id], |_, conf| {
        conf.rocksdb.writecf.write_buffer_size = ReadableSize::kb(256);
    });
    cluster.put_kv(0..100, i_to_key, i_to_val);
    cluster.put_kv(100..200, i_to_key, i_to_val);
    cluster.put_kv(200..300, i_to_key, i_to_val);
    let region_id = cluster.get_region_id(&[]);
    let engine = cluster.get_kvengine(node_id);
    let stats = engine.get_shard_stat(region_id);
    assert!(stats.mem_table_count + stats.l0_table_count > 1);
}

fn i_to_key(i: usize) -> Vec<u8> {
    format!("key_{:03}", i).into_bytes()
}

fn i_to_val(i: usize) -> Vec<u8> {
    format!("val_{:03}", i).into_bytes().repeat(100)
}

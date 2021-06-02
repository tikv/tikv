// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use test_raftstore::*;

// Test even if memory usage reaches high water, committed entries can still get applied slowly.
#[test]
fn test_memory_usage_reaches_high_water() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    cluster.run_conf_change();

    fail::cfg("memory_usage_reaches_high_water", "return").unwrap();
    for i in 0..10 {
        let k = format!("k{:02}", i).into_bytes();
        cluster.must_put(&k, b"value");
        must_get_equal(&cluster.get_engine(1), &k, b"value");
    }
    fail::cfg("memory_usage_reaches_high_water", "off").unwrap();
}

#[test]
fn test_evict_entry_cache() {
    // let mut cluster = new_node_cluster(0, 3);
    // TODO: finish it.
}

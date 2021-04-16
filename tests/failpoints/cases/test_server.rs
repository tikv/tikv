// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use test_raftstore::*;

#[test]
fn test_mismatch_store_node() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    fail::cfg("mock_store_refresh_interval_secs", "return(0)").unwrap();
    cluster.start().unwrap();
    let node_ids = cluster.get_node_ids();
    let mut iter = node_ids.iter();
    let node1_id = *iter.next().unwrap();
    let node2_id = *iter.next().unwrap();
    let pd_client = cluster.pd_client.clone();
    let mut store1 = pd_client.get_store(node1_id).unwrap().clone();
    let mut store2 = pd_client.get_store(node2_id).unwrap().clone();
    let node1_addr = store1.get_address().to_string();
    let node2_addr = store2.get_address().to_string();
    cluster.stop_node(node1_id);
    cluster.stop_node(node2_id);
    // make router error
    store1.set_address(node2_addr.clone());
    store2.set_address(node1_addr.clone());
    pd_client.put_store(store1.clone()).unwrap();
    pd_client.put_store(store2.clone()).unwrap();
    fail::cfg("skip_put_store", "return()").unwrap();
    cluster.run_node(node1_id).unwrap();
    cluster.run_node(node2_id).unwrap();
    fail::remove("skip_put_store");
    // update store_info in pd
    store1.set_address(node1_addr.clone());
    store2.set_address(node2_addr.clone());
    pd_client.put_store(store1.clone()).unwrap();
    pd_client.put_store(store2.clone()).unwrap();
    // wait address refresh
    sleep_ms(600);
    cluster.must_put(b"k4", b"k5");
    fail::remove("mock_store_refresh_interval_secs");
}

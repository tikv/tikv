// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use test_raftstore::*;
use tikv_util::config::*;

#[test]
fn test_store_heartbeat_report_hotspots() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(10);
    cluster.run();
    fail::cfg("mock_hotspot_threshold", "return(0)").unwrap();
    sleep_ms(100);
    let region_id = cluster.get_region_id(b"");
    let store_id = 1;
    let hot_peers = cluster.pd_client.get_store_hotspots(store_id).unwrap();
    let peer_stat = hot_peers.get(&region_id).unwrap();
    assert_eq!(peer_stat.get_region_id(), region_id);
    fail::remove("mock_hotspot_threshold");
}

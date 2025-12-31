// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv_util::config::ReadableDuration;

#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_gc_peer_on_tombstone_store() {
    let mut cluster = new_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.gc_peer_check_interval = ReadableDuration::millis(500);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");

    let peer_on_store1 = find_peer(&region, 1).unwrap().clone();
    let peer_on_store3 = find_peer(&region, 3).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    pd_client.must_remove_peer(region.get_id(), peer_on_store3);

    // Immediately invalidate store address cache.
    fail::cfg("mock_store_refresh_interval_secs", "return(0)").unwrap();

    // Shutdown store 3 and wait for gc peer ticks.
    cluster.stop_node(3);
    cluster.clear_send_filters();
    sleep_ms(3 * cluster.cfg.raft_store.gc_peer_check_interval.as_millis());

    cluster.must_empty_region_removed_records(region.get_id());
}

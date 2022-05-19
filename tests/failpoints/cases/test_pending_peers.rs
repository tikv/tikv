// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use test_raftstore::*;
use tikv_util::{config::*, time::Instant};

#[test]
fn test_pending_peers() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);

    let region_worker_fp = "region_apply_snap";

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));

    // To ensure peer 2 is not pending.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    fail::cfg(region_worker_fp, "sleep(2000)").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    sleep_ms(1000);
    let pending_peers = pd_client.get_pending_peers();
    // Region worker is not started, snapshot should not be applied yet.
    assert_eq!(pending_peers[&3], new_peer(3, 3));
    // But it will be applied finally.
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    sleep_ms(100);
    let pending_peers = pd_client.get_pending_peers();
    assert!(pending_peers.is_empty());
}

// Tests if raftstore and apply worker write truncated_state concurrently could lead to
// dirty write.
#[test]
fn test_pending_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    let election_timeout = configure_for_lease_read(&mut cluster, None, Some(15));
    let gc_limit = cluster.cfg.raft_store.raft_log_gc_count_limit();
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);

    let handle_snapshot_fp = "apply_on_handle_snapshot_1_1";
    let handle_snapshot_finish_fp = "apply_on_handle_snapshot_finish_1_1";
    fail::cfg("apply_on_handle_snapshot_sync", "return").unwrap();

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    cluster.must_transfer_leader(region_id, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");

    fail::cfg(handle_snapshot_fp, "pause").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    // Give some time for peer 3 to request snapshot.
    sleep_ms(100);

    // Isolate peer 1 from rest of the cluster.
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    sleep_ms((election_timeout.as_millis() * 2) as _);
    cluster.reset_leader_of_region(region_id);
    // Compact logs to force requesting snapshot after clearing send filters.
    let state2 = cluster.truncated_state(1, 2);
    for i in 1..gc_limit * 10 {
        let k = i.to_string().into_bytes();
        cluster.must_put(&k, &k.clone());
    }
    cluster.wait_log_truncated(1, 2, state2.get_index() + 5 * gc_limit);

    // Make sure peer 1 has applied snapshot.
    cluster.clear_send_filters();
    let start = Instant::now();
    loop {
        if cluster.pd_client.get_pending_peers().get(&1).is_none()
            || start.saturating_elapsed() > election_timeout * 10
        {
            break;
        }
        sleep_ms(50);
    }
    let state1 = cluster.truncated_state(1, 1);

    // Peer 2 continues to handle snapshot.
    fail::cfg(handle_snapshot_finish_fp, "pause").unwrap();
    fail::remove(handle_snapshot_fp);
    sleep_ms(200);
    let state2 = cluster.truncated_state(1, 1);
    fail::remove(handle_snapshot_finish_fp);
    assert!(
        state1.get_term() <= state2.get_term(),
        "{:?} {:?}",
        state1,
        state2
    );
    assert!(
        state1.get_index() <= state2.get_index(),
        "{:?} {:?}",
        state1,
        state2
    );
}

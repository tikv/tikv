// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use pd_client::PdClient;
use test_raftstore::*;

// Test if the entries can be committed and applied on followers even when
// leader's io is paused.
#[test]
fn test_async_io_commit_without_leader_persist() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.store_io_pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let region = pd_client.get_region(b"k1").unwrap();
    let peer_1 = find_peer(&region, 1).cloned().unwrap();

    cluster.must_put(b"k1", b"v1");
    cluster.must_transfer_leader(region.get_id(), peer_1);

    let raft_before_save_on_store_1_fp = "raft_before_save_on_store_1";
    fail::cfg(raft_before_save_on_store_1_fp, "pause").unwrap();

    for i in 2..10 {
        cluster
            .async_put(format!("k{}", i).as_bytes(), b"v1")
            .unwrap();
    }

    // Although leader can not persist entries, these entries can be committed
    must_get_equal(&cluster.get_engine(2), b"k9", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k9", b"v1");
    // For now, entries must be applied after persisting
    must_get_none(&cluster.get_engine(1), b"k9");

    fail::remove(raft_before_save_on_store_1_fp);
    must_get_equal(&cluster.get_engine(3), b"k9", b"v1");
}

/// Test if the leader delays its destroy after applying conf change to
/// remove itself.
#[test]
fn test_delay_destroy_after_conf_change() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.store_io_pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k1", b"v1");

    fail::cfg("on_handle_apply", "pause").unwrap();

    // Remove leader itself.
    pd_client.remove_peer(r1, new_peer(1, 1));
    // Wait for sending the conf change to other peers
    sleep_ms(100);
    // Peer 1 can not be removed because the conf change can not apply
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    fail::cfg("raft_before_save_on_store_1", "pause").unwrap();

    for i in 2..10 {
        cluster
            .async_put(format!("k{}", i).as_bytes(), b"v")
            .unwrap();
    }

    fail::remove("on_handle_apply");
    // Wait for applying conf change
    sleep_ms(100);
    // Peer 1 should not be destroyed now due to delay destroy
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    fail::remove("raft_before_save_on_store_1");
    // Peer 1 should be destroyed as expected
    must_get_none(&cluster.get_engine(1), b"k1");
}

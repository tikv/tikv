// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use futures::executor::block_on;
use pd_client::PdClient;
use raftstore::store::util::find_peer;
use test_raftstore::*;
use tikv_util::{config::ReadableDuration, mpsc};

#[test]
fn test_unsafe_recover_send_report() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    configure_for_lease_read(&mut cluster, None, None);

    // Makes the leadership definite.
    let store2_peer = find_peer(&region, nodes[1]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store2_peer);
    cluster.put(b"random_key1", b"random_val1").unwrap();

    // Blocks the raft apply process on store 1 entirely .
    let (apply_triggered_tx, apply_triggered_rx) = mpsc::bounded::<()>(1);
    let (apply_released_tx, apply_released_rx) = mpsc::bounded::<()>(1);
    fail::cfg_callback("on_handle_apply_store_1", move || {
        let _ = apply_triggered_tx.send(());
        let _ = apply_released_rx.recv();
    })
    .unwrap();

    // Mannually makes an update, and wait for the apply to be triggered, to simulate "some entries are commited but not applied" scenario.
    cluster.put(b"random_key2", b"random_val2").unwrap();
    apply_triggered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    // Makes the group lose its quorum.
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);

    // Triggers the unsafe recovery store reporting process.
    pd_client.must_set_require_report(true);
    cluster.must_send_store_heartbeat(nodes[0]);

    // No store report is sent, since there are peers have unapplied entries.
    for _ in 0..20 {
        assert_eq!(pd_client.must_get_store_reported(&nodes[0]), 0);
        sleep_ms(100);
    }

    // Unblocks the apply process.
    drop(apply_released_tx);

    // Store reports are sent once the entries are applied.
    let mut reported = false;
    for _ in 0..20 {
        if pd_client.must_get_store_reported(&nodes[0]) > 0 {
            reported = true;
            break;
        }
        sleep_ms(100);
    }
    assert_eq!(reported, true);
    fail::remove("on_handle_apply_store_1");
}

#[test]
fn test_unsafe_recover_wait_for_snapshot_apply() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 8;
    cluster.cfg.raft_store.merge_max_log_gap = 3;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    configure_for_lease_read(&mut cluster, None, None);

    // Makes the leadership definite.
    let store2_peer = find_peer(&region, nodes[1]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store2_peer);
    cluster.stop_node(nodes[1]);
    let (raft_gc_triggered_tx, raft_gc_triggered_rx) = mpsc::bounded::<()>(1);
    let (raft_gc_finished_tx, raft_gc_finished_rx) = mpsc::bounded::<()>(1);
    fail::cfg_callback("worker_gc_raft_log", move || {
        let _ = raft_gc_triggered_rx.recv();
    })
    .unwrap();
    fail::cfg_callback("worker_gc_raft_log_finished", move || {
        let _ = raft_gc_finished_tx.send(());
    })
    .unwrap();
    // Add at least 4m data
    (0..10).for_each(|_| cluster.must_put(b"random_k", b"random_v"));
    // Unblock raft log GC.
    drop(raft_gc_triggered_tx);
    // Wait until logs are GCed.
    raft_gc_finished_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    // Makes the group lose its quorum.
    cluster.stop_node(nodes[2]);

    // Blocks the raft snap apply process.
    let (apply_triggered_tx, apply_triggered_rx) = mpsc::bounded::<()>(1);
    let (apply_released_tx, apply_released_rx) = mpsc::bounded::<()>(1);
    fail::cfg_callback("region_apply_snap", move || {
        let _ = apply_triggered_tx.send(());
        let _ = apply_released_rx.recv();
    })
    .unwrap();

    cluster.run_node(nodes[1]).unwrap();

    apply_triggered_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();

    // Triggers the unsafe recovery store reporting process.
    pd_client.must_set_require_report(true);
    cluster.must_send_store_heartbeat(nodes[1]);

    // No store report is sent, since there are peers have unapplied entries.
    for _ in 0..20 {
        assert_eq!(pd_client.must_get_store_reported(&nodes[1]), 0);
        sleep_ms(100);
    }

    // Unblocks the snap apply process.
    drop(apply_released_tx);

    // Store reports are sent once the entries are applied.
    let mut reported = false;
    for _ in 0..20 {
        if pd_client.must_get_store_reported(&nodes[1]) > 0 {
            reported = true;
            break;
        }
        sleep_ms(100);
    }
    assert_eq!(reported, true);
    fail::remove("worker_gc_raft_log");
    fail::remove("worker_gc_raft_log_finished");
    fail::remove("raft_before_apply_snap_callback");
}

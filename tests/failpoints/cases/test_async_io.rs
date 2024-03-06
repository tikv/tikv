// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::AtomicBool, mpsc, Arc, Mutex},
    time::Duration,
};

use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv_util::HandyRwLock;

// Test if the entries can be committed and applied on followers even when
// leader's io is paused.
#[test]
fn test_async_io_commit_without_leader_persist() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.cmd_batch_concurrent_ready_max_count = 0;
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
fn test_async_io_delay_destroy_after_conf_change() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.store_io_pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k1", b"v1");

    let on_handle_apply_fp = "on_handle_apply";
    fail::cfg(on_handle_apply_fp, "pause").unwrap();

    // Remove leader itself.
    pd_client.remove_peer(r1, new_peer(1, 1));
    // Wait for sending the conf change to other peers
    sleep_ms(100);
    // Peer 1 can not be removed because the conf change can not apply
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    let raft_before_save_on_store_1_fp = "raft_before_save_on_store_1";
    fail::cfg(raft_before_save_on_store_1_fp, "pause").unwrap();

    for i in 2..10 {
        cluster
            .async_put(format!("k{}", i).as_bytes(), b"v")
            .unwrap();
    }

    fail::remove(on_handle_apply_fp);
    // Wait for applying conf change
    sleep_ms(100);
    // Peer 1 should not be destroyed now due to delay destroy
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    fail::remove(raft_before_save_on_store_1_fp);
    // Peer 1 should be destroyed as expected
    must_get_none(&cluster.get_engine(1), b"k1");
}

/// Test if the peer can be destroyed when it receives a tombstone msg and
/// its snapshot is persisting.
#[test]
fn test_async_io_cannot_destroy_when_persist_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.store_io_pool_size = 2;
    configure_for_snapshot(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let region = pd_client.get_region(b"k1").unwrap();
    let peer_1 = find_peer(&region, 1).cloned().unwrap();
    let peer_3 = find_peer(&region, 3).cloned().unwrap();

    cluster.must_transfer_leader(region.get_id(), peer_1);

    cluster.must_put(b"k", b"v");
    // Make sure peer 3 exists
    must_get_equal(&cluster.get_engine(3), b"k", b"v");

    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let send_filter = Box::new(
        RegionPacketFilter::new(region.get_id(), 3)
            .direction(Direction::Send)
            .msg_type(MessageType::MsgAppendResponse)
            .reserve_dropped(Arc::clone(&dropped_msgs)),
    );
    cluster.sim.wl().add_send_filter(3, send_filter);

    cluster.must_put(b"k1", b"v1");

    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.sim.wl().clear_send_filters(3);

    cluster.add_send_filter(IsolationFilterFactory::new(3));

    for i in 2..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    let raft_before_save_kv_on_store_3_fp = "raft_before_save_kv_on_store_3";
    fail::cfg(raft_before_save_kv_on_store_3_fp, "pause").unwrap();

    let (notify_tx, notify_rx) = mpsc::channel();
    cluster.sim.wl().add_recv_filter(
        3,
        Box::new(MessageTypeNotifier::new(
            MessageType::MsgSnapshot,
            notify_tx,
            Arc::new(AtomicBool::new(true)),
        )),
    );

    cluster.clear_send_filters();

    // Wait for leader sending snapshot to peer 3
    notify_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // Wait for peer 3 handling snapshot
    sleep_ms(100);

    pd_client.must_remove_peer(region.get_id(), peer_3);

    // Trigger leader sending tombstone msg to peer 3
    let router = cluster.sim.wl().get_router(1).unwrap();
    for raft_msg in dropped_msgs.lock().unwrap().drain(..).rev() {
        if raft_msg.get_to_peer().get_store_id() == 1 {
            router.send_raft_message(raft_msg).unwrap();
            break;
        }
    }
    // Wait for peer 3 handling tombstone msg
    sleep_ms(100);

    // Peer 3 should not be destroyed because its snapshot is persisting
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    fail::remove(raft_before_save_kv_on_store_3_fp);

    must_get_none(&cluster.get_engine(3), b"k1");
}

/// Test if the peer can handle ready when its snapshot is persisting.
#[test]
fn test_async_io_cannot_handle_ready_when_persist_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.store_io_pool_size = 2;
    configure_for_snapshot(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k1", b"v1");

    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.add_send_filter(IsolationFilterFactory::new(3));

    for i in 2..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    must_get_equal(&cluster.get_engine(2), b"k9", b"v1");

    let raft_before_save_kv_on_store_3_fp = "raft_before_save_kv_on_store_3";
    fail::cfg(raft_before_save_kv_on_store_3_fp, "pause").unwrap();

    let (notify_tx, notify_rx) = mpsc::channel();
    cluster.sim.wl().add_recv_filter(
        3,
        Box::new(MessageTypeNotifier::new(
            MessageType::MsgSnapshot,
            notify_tx,
            Arc::new(AtomicBool::new(true)),
        )),
    );

    cluster.clear_send_filters();

    // Wait for leader sending snapshot to peer 3
    notify_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // Wait for peer 3 handling snapshot
    sleep_ms(100);

    let panic_if_handle_ready_3_fp = "panic_if_handle_ready_3";
    fail::cfg(panic_if_handle_ready_3_fp, "return").unwrap();

    for i in 10..20 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    cluster.must_transfer_leader(r1, new_peer(2, 2));

    for i in 20..30 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    fail::remove(panic_if_handle_ready_3_fp);
    fail::remove(raft_before_save_kv_on_store_3_fp);

    must_get_equal(&cluster.get_engine(3), b"k29", b"v1");
}

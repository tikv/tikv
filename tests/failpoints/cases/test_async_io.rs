// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::AtomicBool, mpsc, Arc, Mutex},
    time::Duration,
};

use engine_traits::{Peekable, RaftEngineReadOnly, CF_RAFT};
use kvproto::raft_serverpb::RaftApplyState;
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use test_raftstore_macro::test_case;
use test_util::eventually;
use tikv_util::{config::ReadableDuration, HandyRwLock};

// Test if the entries can be committed and applied on followers even when
// leader's io is paused.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_async_io_commit_without_leader_persist() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.cmd_batch_concurrent_ready_max_count = 0;
    cluster.cfg.raft_store.store_io_pool_size = 2;
    cluster.cfg.raft_store.max_apply_unpersisted_log_limit = 0;
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
        let _ = cluster
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

#[test]
fn test_async_io_apply_without_leader_persist() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.store_io_pool_size = 1;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let region = pd_client.get_region(b"k1").unwrap();
    let peer_1 = find_peer(&region, 1).cloned().unwrap();

    cluster.must_transfer_leader(region.get_id(), peer_1);
    cluster.must_put(b"k1", b"v1");

    let raft_before_save_on_store_1_fp = "raft_before_persist_on_store_1";
    // Skip persisting to simulate raft log persist lag but not block node restart.
    fail::cfg(raft_before_save_on_store_1_fp, "return").unwrap();

    for i in 2..10 {
        let _ = cluster
            .async_put(format!("k{}", i).as_bytes(), b"v1")
            .unwrap();
    }

    // All node can apply these entries.
    for i in 1..=3 {
        must_get_equal(&cluster.get_engine(i), b"k9", b"v1");
    }

    cluster.stop_node(1);
    fail::remove(raft_before_save_on_store_1_fp);

    // Node 1 can recover successfully.
    cluster.run_node(1).unwrap();

    cluster.must_put(b"k1", b"v2");
    sleep_ms(100);
    for i in 1..=3 {
        must_get_equal(&cluster.get_engine(i), b"k1", b"v2");
    }
}

#[test]
fn test_async_io_apply_conf_change_without_leader_persist() {
    let mut cluster = new_node_cluster(0, 4);
    cluster.cfg.raft_store.store_io_pool_size = 1;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k1", b"v1");

    let raft_before_save_on_store_1_fp = "raft_before_persist_on_store_1";
    // Skip persisting to simulate raft log persist lag but not block node restart.
    fail::cfg(raft_before_save_on_store_1_fp, "return").unwrap();

    for i in 2..10 {
        let _ = cluster
            .async_put(format!("k{}", i).as_bytes(), b"v1")
            .unwrap();
    }
    must_get_equal(&cluster.get_engine(1), b"k9", b"v1");

    pd_client.must_add_peer(r1, new_peer(4, 4));
    pd_client.remove_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k1", b"v2");
    for i in [1, 2, 4] {
        eventually_get_equal(&cluster.get_engine(i), b"k1", b"v2");
    }
    must_get_none(&cluster.get_engine(3), b"k1");

    cluster.stop_node(1);
    fail::remove(raft_before_save_on_store_1_fp);

    // Node 1 can recover successfully.
    cluster.run_node(1).unwrap();

    cluster.must_put(b"k1", b"v3");
    eventually_get_equal(&cluster.get_engine(1), b"k1", b"v3");
}

/// Test when enables "apply before persistence" and the leader's applied index
/// is higher than persisted index when restart. And the new leader has already
/// GCed some raft logs the former leader is not persisted. In this case, the
/// old leader need to recover by snapshot.
#[test]
fn test_async_io_apply_before_persist_apply_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.store_io_pool_size = 2;
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    configure_for_snapshot(&mut cluster.cfg);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let region = pd_client.get_region(b"k1").unwrap();
    let peer_1 = find_peer(&region, 1).cloned().unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_1);
    cluster.must_put(b"k1", b"v1");

    let raft_before_save_on_store_1_fp = "raft_before_persist_on_store_1";
    // Skip persisting to simulate raft log persist lag but not block node restart.
    fail::cfg(raft_before_save_on_store_1_fp, "return").unwrap();
    let only_apply_on_store_1_fp = "only_apply_for_region_1_on_store_1";
    // Pause applying raft log on all stores but store 1.
    fail::cfg(only_apply_on_store_1_fp, "pause").unwrap();

    for i in 2..10 {
        let _ = cluster
            .async_put(format!("k{}", i).as_bytes(), b"v1")
            .unwrap();
    }
    must_get_equal(&cluster.get_engine(1), b"k9", b"v1");
    must_get_none(&cluster.get_engine(2), b"k9");
    must_get_none(&cluster.get_engine(3), b"k9");

    let region = pd_client.get_region(b"k2").unwrap();
    assert_eq!(region.id, 1);

    // test unpersisted split won't cause problem when recover via snapshot.
    cluster.must_split(&region, b"k2");
    for i in 2..=10 {
        cluster.must_put(b"k2", format!("v{}", i).as_bytes());
    }
    must_get_equal(&cluster.get_engine(1), b"k2", b"v10");

    cluster.stop_node(1);

    let raft_state1 = cluster
        .get_raft_engine(1)
        .get_raft_state(1)
        .unwrap()
        .unwrap();

    fail::remove(raft_before_save_on_store_1_fp);
    fail::remove(only_apply_on_store_1_fp);

    cluster.must_put(b"k2", b"v11");

    let leader = cluster.leader_of_region(1).unwrap();
    assert!(leader.store_id != 1);
    // wait leader gc raft log.
    eventually(
        Duration::from_millis(100),
        Duration::from_millis(1000),
        || {
            let apply_state: RaftApplyState = cluster
                .get_engine(leader.store_id)
                .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
                .unwrap()
                .unwrap();
            apply_state.get_truncated_state().get_index() > raft_state1.last_index
        },
    );

    // stop the follower, thus there will be only 2 peer and the follower need to
    // sync snapshot.
    for i in 2..=3 {
        if i != leader.store_id {
            cluster.stop_node(i);
        }
    }
    cluster.run_node(1).unwrap();

    eventually_get_equal(&cluster.get_engine(1), b"k2", b"v11");
    // test new write
    cluster.must_put(b"k1", b"v12");
    cluster.must_put(b"k2", b"v12");
    eventually_get_equal(&cluster.get_engine(1), b"k1", b"v12");
    eventually_get_equal(&cluster.get_engine(1), b"k2", b"v12");
}

/// Test if the leader delays its destroy after applying conf change to
/// remove itself.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_async_io_delay_destroy_after_conf_change() {
    let mut cluster = new_cluster(0, 3);
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
        let _ = cluster
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
///
/// Note: snapshot flow is changed, so partitioend-raft-kv does not support this
/// test.
#[test]
fn test_async_io_cannot_destroy_when_persist_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.store_io_pool_size = 2;
    configure_for_snapshot(&mut cluster.cfg);
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
///
/// Note: snapshot flow is changed, so partitioend-raft-kv does not support this
/// test.
#[test]
fn test_async_io_cannot_handle_ready_when_persist_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.store_io_pool_size = 2;
    configure_for_snapshot(&mut cluster.cfg);
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

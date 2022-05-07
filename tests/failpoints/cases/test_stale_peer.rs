// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::AtomicBool, mpsc, Arc},
    thread,
    time::Duration,
};

use engine_traits::RaftEngineReadOnly;
use futures::executor::block_on;
use kvproto::raft_serverpb::{PeerState, RaftLocalState, RaftMessage};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv_util::{config::ReadableDuration, time::Instant, HandyRwLock};

#[test]
fn test_one_node_leader_missing() {
    let mut cluster = new_server_cluster(0, 1);

    // 50ms election timeout.
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 5;
    let base_tick_interval = cluster.cfg.raft_store.raft_base_tick_interval.0;
    let election_timeout = base_tick_interval * 5;
    cluster.cfg.raft_store.raft_store_max_leader_lease =
        ReadableDuration(election_timeout - base_tick_interval);
    // Use large peer check interval, abnormal and max leader missing duration to make a valid config,
    // that is election timeout x 2 < peer stale state check < abnormal < max leader missing duration.
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration(election_timeout * 3);
    cluster.cfg.raft_store.abnormal_leader_missing_duration =
        ReadableDuration(election_timeout * 4);
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration(election_timeout * 7);

    // Panic if the cluster does not has a valid stale state.
    let check_stale_state = "peer_check_stale_state";
    fail::cfg(check_stale_state, "panic").unwrap();

    cluster.start().unwrap();

    // Check stale state 3 times,
    thread::sleep(cluster.cfg.raft_store.peer_stale_state_check_interval.0 * 3);
    fail::remove(check_stale_state);
}

#[test]
fn test_node_update_localreader_after_removed() {
    let mut cluster = new_node_cluster(0, 6);
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    // Add 4 peers.
    for i in 2..6 {
        pd_client.must_add_peer(r1, new_peer(i, i));
    }

    // Make sure peer 1 leads the region.
    cluster.must_transfer_leader(r1, new_peer(1, 1));
    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // Make sure peer 2 is initialized.
    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, key, value);

    // Pause peer 2 apply worker if it executes AddNode.
    let add_node_fp = "apply_on_add_node_1_2";
    fail::cfg(add_node_fp, "pause").unwrap();

    // Add peer 6.
    pd_client.must_add_peer(r1, new_peer(6, 6));

    // Isolate peer 2 from rest of the cluster.
    cluster.add_send_filter(IsolationFilterFactory::new(2));

    // Remove peer 2, so it will receive a gc msssage
    // after max_leader_missing_duration timeout.
    pd_client.must_remove_peer(r1, new_peer(2, 2));
    thread::sleep(cluster.cfg.raft_store.max_leader_missing_duration.0 * 2);

    // Continue peer 2 apply worker, so that peer 2 tries to
    // update region to its read delegate.
    fail::remove(add_node_fp);

    // Make sure peer 2 is removed in node 2.
    cluster.must_region_not_exist(r1, 2);
}

#[test]
fn test_stale_learner_restart() {
    let mut cluster = new_node_cluster(0, 2);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.raft_log_gc_threshold = 10;
    let r = cluster.run_conf_change();
    cluster
        .pd_client
        .must_add_peer(r, new_learner_peer(2, 1003));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    // Simulates slow apply.
    fail::cfg("on_handle_apply_1003", "return").unwrap();
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    let mut state: RaftLocalState = cluster
        .get_raft_engine(1)
        .get_raft_state(r)
        .unwrap()
        .unwrap();
    let last_index = state.get_last_index();
    let timer = Instant::now();
    while timer.saturating_elapsed() < Duration::from_secs(5) {
        state = cluster
            .get_raft_engine(2)
            .get_raft_state(r)
            .unwrap()
            .unwrap();
        if last_index <= state.get_hard_state().get_commit() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    if state.last_index != last_index {
        panic!("store 2 has not catched up logs after 5 secs.");
    }
    cluster.shutdown();
    must_get_none(&cluster.get_engine(2), b"k2");
    fail::remove("on_handle_apply_1003");
    cluster.run_node(2).unwrap();
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
}

/// Test if a peer can be destroyed through tombstone msg when applying snapshot.
#[test]
fn test_stale_peer_destroy_when_apply_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");

    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.add_send_filter(IsolationFilterFactory::new(3));

    for i in 2..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    let region_apply_snap_fp = "region_apply_snap";
    fail::cfg(region_apply_snap_fp, "pause").unwrap();

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
    let region = pd_client.get_region(b"k1").unwrap();
    // Wait for peer 3 handling snapshot
    let timer = Instant::now();
    loop {
        let local_state = cluster.region_local_state(region.get_id(), 3);
        if local_state.get_state() == PeerState::Applying {
            break;
        }
        if timer.saturating_elapsed() > Duration::from_secs(3) {
            panic!("not become applying state after 3 seconds.");
        }
        sleep_ms(10);
    }

    pd_client.must_remove_peer(region.get_id(), new_peer(3, 3));

    let region = pd_client.get_region(b"k1").unwrap();
    let mut tombstone_msg = RaftMessage::default();
    tombstone_msg.set_region_id(region.get_id());
    tombstone_msg.set_from_peer(new_peer(3, 3));
    tombstone_msg.set_to_peer(new_peer(3, 3));
    tombstone_msg.set_region_epoch(region.get_region_epoch().clone());
    tombstone_msg.set_is_tombstone(true);
    cluster
        .sim
        .wl()
        .send_raft_msg(tombstone_msg.clone())
        .unwrap();
    // Wait for cancelling snapshot
    sleep_ms(100);
    fail::remove(region_apply_snap_fp);
    // Wait for peer 3 changing `SnapState`
    sleep_ms(100);
    cluster.sim.wl().send_raft_msg(tombstone_msg).unwrap();

    must_get_none(&cluster.get_engine(3), b"k1");
}

/// Test if destroy a uninitialized peer through tombstone msg would allow a staled peer be created again.
#[test]
fn test_destroy_uninitialized_peer_when_there_exists_old_peer() {
    // 4 stores cluster.
    let mut cluster = new_node_cluster(0, 4);
    cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.hibernate_regions = false;

    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // Now region 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    // add peer (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let epoch = pd_client.get_region_epoch(r1);

    // Conf version must change.
    assert!(epoch.get_conf_ver() > 2);

    // Transfer leader to peer (2, 2).
    cluster.must_transfer_leader(r1, new_peer(2, 2));

    // Isolate node 1
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    cluster.must_put(format!("k{}", 2).as_bytes(), b"v1");

    // Remove 3 and add 4
    pd_client.must_add_peer(r1, new_learner_peer(4, 4));
    pd_client.must_add_peer(r1, new_peer(4, 4));
    pd_client.must_remove_peer(r1, new_peer(3, 3));

    cluster.must_put(format!("k{}", 3).as_bytes(), b"v1");

    // Ensure 5 drops all snapshot
    let (notify_tx, _notify_rx) = mpsc::channel();
    cluster
        .sim
        .wl()
        .add_recv_filter(3, Box::new(DropSnapshotFilter::new(notify_tx)));

    // Add learner 5 on store 3
    pd_client.must_add_peer(r1, new_learner_peer(3, 5));

    cluster.must_put(format!("k{}", 4).as_bytes(), b"v1");

    // Remove and destroy the uninitialized 5
    let peer_5 = new_learner_peer(3, 5);
    pd_client.must_remove_peer(r1, peer_5.clone());
    cluster.must_gc_peer(r1, 3, peer_5);

    let region = block_on(pd_client.get_region_by_id(r1)).unwrap();
    must_region_cleared(&cluster.get_all_engines(3), &region.unwrap());

    // Unisolate 1 and try wakeup 3
    cluster.clear_send_filters();
    cluster.sim.wl().clear_recv_filters(3);
    cluster.partition(vec![1, 3], vec![2, 4]);

    sleep_until_election_triggered(&cluster.cfg);

    let region = block_on(pd_client.get_region_by_id(r1)).unwrap();
    must_region_cleared(&cluster.get_all_engines(3), &region.unwrap());
}

/// Logs scan are now moved to raftlog gc threads. The case is to test if logs
/// are still cleaned up when there is stale logs before first index during destroy.
#[test]
fn test_destroy_clean_up_logs_with_unfinished_log_gc() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 15;
    cluster.cfg.raft_store.raft_log_gc_threshold = 15;
    let pd_client = cluster.pd_client.clone();

    // Disable default max peer number check.
    pd_client.disable_default_operator();
    cluster.run();
    // Simulate raft log gc are pending in queue.
    let fp = "worker_gc_raft_log";
    fail::cfg(fp, "return(0)").unwrap();

    let state = cluster.truncated_state(1, 3);
    for i in 0..30 {
        let b = format!("k{}", i).into_bytes();
        cluster.must_put(&b, &b);
    }
    must_get_equal(&cluster.get_engine(3), b"k29", b"k29");
    cluster.wait_log_truncated(1, 3, state.get_index() + 1);
    cluster.stop_node(3);
    let truncated_index = cluster.truncated_state(1, 3).get_index();
    let raft_engine = cluster.engines[&3].raft.clone();
    // Make sure there are stale logs.
    raft_engine.get_entry(1, truncated_index).unwrap().unwrap();

    pd_client.must_remove_peer(1, new_peer(3, 3));
    cluster.must_put(b"k30", b"v30");
    must_get_equal(&cluster.get_engine(1), b"k30", b"v30");

    fail::remove(fp);
    // So peer (3, 3) will be destroyed by gc message. And all stale logs before first
    // index should be cleaned up.
    cluster.run_node(3).unwrap();
    must_get_none(&cluster.get_engine(3), b"k29");

    let mut dest = vec![];
    raft_engine.get_all_entries_to(1, &mut dest).unwrap();
    // All logs should be deleted.
    assert!(dest.is_empty(), "{:?}", dest);
}

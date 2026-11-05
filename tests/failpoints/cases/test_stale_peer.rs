// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex, atomic::AtomicBool, mpsc},
    thread,
    time::Duration,
};

use engine_traits::{RaftEngine, RaftEngineDebug, RaftEngineReadOnly};
use futures::executor::block_on;
use kvproto::raft_serverpb::{PeerState, RaftLocalState, RaftMessage};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use raftstore::store::{
    MergeResultKind, PeerMsg, SignificantMsg, SignificantRouter, UnsafeRecoveryExecutePlanSyncer,
    fsm::ApplyTaskRes,
};
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv_util::{HandyRwLock, config::ReadableDuration, time::Instant};

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
    // Use large peer check interval, abnormal and max leader missing duration to
    // make a valid config, that is election timeout x 2 < peer stale state
    // check < abnormal < max leader missing duration.
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
fn test_clean_stale_peer() {
    let validate_data_files = |dir: &std::path::Path| -> bool {
        let mut data_file = 0_usize;
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() && path.to_str().unwrap().ends_with(".log") {
                data_file += 1;
            }
        }
        data_file > 0
    };
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.snap_mgr_gc_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    // Disable raft log gc in this test case.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();

    // Manually set store 3 is offlined.
    fail::cfg("manually_set_store_offline", "return").unwrap();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));

    (0..10).for_each(|_| cluster.must_put(b"k1", b"v1"));
    // Send heartbeats to pd to ensure that heartbeat responses have been received
    // by node 3.
    cluster.must_send_store_heartbeat(3);
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    pd_client.must_remove_peer(region_id, new_peer(3, 3));
    // Since the node 3 is marked with NodeState::Removing, its `offlined == true`
    // in SnapManager, and no need to clear the corresponding data.
    sleep_ms(500);
    let engine = cluster.get_engine(3);
    let engine_path = Path::new(engine.get_engine_path());
    assert!(validate_data_files(engine_path));
    fail::remove("manually_set_store_offline");
    // After the state is cleared, the store should be marked with
    // NodeState::Serving. And after adding a new peer to store 3,
    // all previous data should be synced to it.
    cluster.must_send_store_heartbeat(3);
    cluster.must_put(b"k10", b"v10");
    let engine = cluster.get_engine(3);
    let (key, value) = (b"k10", b"v10");
    must_get_none(&engine, key);
    pd_client.must_add_peer(region_id, new_peer(3, 4));
    must_get_equal(&engine, key, value);
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    // Remove the peer 4.
    let (notify_tx, notify_rx) = mpsc::channel();
    let notifier_fp = "raftstore_set_region_after_change_peer";
    fail::cfg_callback(notifier_fp, move || {
        let _ = notify_tx.send(notifier_fp);
    })
    .unwrap();
    pd_client.must_remove_peer(region_id, new_peer(3, 4));
    assert_eq!(
        notify_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        notifier_fp
    );
    sleep_ms(500);
    must_get_none(&engine, key);
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
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

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
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

/// Test if a peer can be destroyed through tombstone msg when applying
/// snapshot.
//#[test_case(test_raftstore_v2::new_node_cluster)] // unstable test case
#[test_case(test_raftstore::new_node_cluster)]
fn test_stale_peer_destroy_when_apply_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_snapshot(&mut cluster.cfg);
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

    // we expect the peer would be destroyed after applying the snapshot without
    // another message trigger
    must_get_none(&cluster.get_engine(3), b"k1");
}

/// Test if destroy a uninitialized peer through tombstone msg would allow a
/// staled peer be created again.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
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
/// are still cleaned up when there is stale logs before first index during
/// destroy.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_destroy_clean_up_logs_with_unfinished_log_gc() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(15);
    cluster.cfg.raft_store.raft_log_gc_threshold = 15;
    let pd_client = cluster.pd_client.clone();

    // Disable default max peer number check.
    pd_client.disable_default_operator();
    cluster.run();
    // Simulate raft log gc tasks are lost during shutdown.
    let fp = "worker_gc_raft_log";
    fail::cfg(fp, "return").unwrap();

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
    // So peer (3, 3) will be destroyed by gc message. And all stale logs before
    // first index should be cleaned up.
    cluster.run_node(3).unwrap();
    must_get_none(&cluster.get_engine(3), b"k29");

    let mut dest = vec![];
    raft_engine.get_all_entries_to(1, &mut dest).unwrap();
    // All logs should be deleted.
    assert!(dest.is_empty(), "{:?}", dest);
}

#[test_case(test_raftstore::new_node_cluster)]
fn test_async_destroy_peer_delayed() {
    let mut cluster = new_node_cluster(0, 3);
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

    // Pause destroy peer
    let (tx, rx) = mpsc::sync_channel(1);
    let pause_destroy_peer_fp = "destroy_peer_after_pending_move";
    fail::cfg_callback(pause_destroy_peer_fp, move || {
        tx.send(pause_destroy_peer_fp).unwrap();
    })
    .unwrap();

    // Remove the peer 3 and the relevant destroying handling should be
    // paused
    pd_client.must_remove_peer(r1, new_peer(3, 3));
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        pause_destroy_peer_fp
    );

    // Add a new peer 4 to store 3, and remove the failpoint to enable
    // the racing condition that the pending region has been added
    // a new peer while the destroying task of the old peer is executing
    // concurrentl.
    pd_client.must_add_peer(r1, new_peer(3, 4));
    fail::remove(pause_destroy_peer_fp);

    // Again, remove the peer, and check the data should be cleared.
    pd_client.must_remove_peer(r1, new_peer(3, 4));
    sleep_ms(100); // wait for clearing data asynchronously.
    let region = block_on(pd_client.get_region_by_id(r1)).unwrap();
    must_region_cleared(&cluster.get_all_engines(3), &region.unwrap());
}

#[test]
fn test_async_destroy_initialized_peer_reentry() {
    check_async_destroy_peer_reentry(true);
}

#[test]
fn test_async_destroy_uninitialized_peer_reentry() {
    check_async_destroy_peer_reentry(false);
}

#[test]
fn test_async_destroy_peer_completion_before_snapshot_step1() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.pd_client.must_add_peer(region_id, new_peer(2, 2));
    let peer = new_peer(3, 3);
    cluster.pd_client.must_add_peer(region_id, peer.clone());
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let (cleanup_tx, cleanup_rx) = mpsc::channel();
    fail::cfg_callback("before_clear_peer_meta", move || {
        cleanup_tx.send(()).unwrap();
    })
    .unwrap();

    // Pause the real worker's completion in the source FSM before it locks
    // StoreMeta. Step1 queued below must be handled after this completion.
    let (completed_tx, completed_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel::<()>();
    let resume_rx = Mutex::new(resume_rx);
    fail::cfg_callback("raft_store_after_destroy_peer", move || {
        completed_tx.send(()).unwrap();
        let _ = resume_rx.lock().unwrap().recv();
    })
    .unwrap();
    cluster.pd_client.must_remove_peer(region_id, peer);
    completed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    cleanup_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    // Inject the dependency that a late target snapshot registers. Registration
    // and sending Step1 hold the same lock, as in destroy_regions_for_snapshot.
    let target_region_id = region_id + 1;
    let router = cluster.get_router(3).unwrap();
    {
        let mut meta = cluster.store_metas[&3].lock().unwrap();
        assert!(meta.regions.contains_key(&region_id));
        meta.targets_map.insert(region_id, target_region_id);
        meta.destroyed_region_for_snap.insert(region_id, true);
        meta.atomic_snap_regions
            .entry(target_region_id)
            .or_default()
            .insert(region_id, false);
        router
            .significant_send(
                region_id,
                SignificantMsg::MergeResult {
                    target_region_id,
                    target: new_peer(3, 4),
                    result: MergeResultKind::FromTargetSnapshotStep1,
                },
            )
            .unwrap();
    }

    drop(resume_tx);
    cluster.must_remove_region(3, region_id);
    fail::remove("raft_store_after_destroy_peer");
    {
        let meta = cluster.store_metas[&3].lock().unwrap();
        assert!(!meta.regions.contains_key(&region_id));
        assert!(!meta.targets_map.contains_key(&region_id));
        assert!(!meta.destroyed_region_for_snap.contains_key(&region_id));
        assert!(meta.atomic_snap_regions[&target_region_id].is_empty());
    }

    // A replacement peer must still install its snapshot and read the data.
    // This also drains the old peer's region-worker tasks.
    cluster.pd_client.must_add_peer(region_id, new_peer(3, 4));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    assert_eq!(cleanup_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    fail::remove("before_clear_peer_meta");
}

fn check_async_destroy_peer_reentry(initialized: bool) {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.pd_client.must_add_peer(region_id, new_peer(2, 2));

    let peer = if initialized {
        let peer = new_peer(3, 3);
        cluster.pd_client.must_add_peer(region_id, peer.clone());
        must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
        peer
    } else {
        let (tx, rx) = mpsc::channel();
        cluster
            .sim
            .wl()
            .add_recv_filter(3, Box::new(DropSnapshotFilter::new(tx)));
        let peer = new_learner_peer(3, 3);
        cluster.pd_client.must_add_peer(region_id, peer.clone());
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        peer
    };

    // Hold cleanup on the region worker while the old peer FSM still accepts
    // messages. Each invocation represents an actual metadata cleanup task.
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel::<()>();
    let resume_rx = Mutex::new(resume_rx);
    fail::cfg_callback("before_clear_peer_meta", move || {
        started_tx.send(()).unwrap();
        let _ = resume_rx.lock().unwrap().recv();
    })
    .unwrap();

    let (gc_started_tx, gc_started_rx) = mpsc::channel();
    let (gc_resume_tx, gc_resume_rx) = mpsc::channel::<()>();
    if initialized {
        let gc_resume_rx = Mutex::new(gc_resume_rx);
        fail::cfg_callback("destroy_region_before_gc_flush", move || {
            gc_started_tx.send(()).unwrap();
            let _ = gc_resume_rx.lock().unwrap().recv();
        })
        .unwrap();
    }
    cluster.pd_client.must_remove_peer(region_id, peer.clone());
    let router = cluster.get_router(3).unwrap();
    if initialized {
        // Re-enter while the first destroy is still waiting for log GC. It must
        // keep waiting without scheduling another flush or panicking.
        gc_started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        router
            .force_send(
                region_id,
                PeerMsg::ApplyRes(Box::new(ApplyTaskRes::Destroy {
                    region_id,
                    peer_id: peer.get_id(),
                    merge_from_snapshot: false,
                })),
            )
            .unwrap();
        cluster
            .call_command_on_node(
                3,
                new_status_request(region_id, peer.clone(), new_region_leader_cmd()),
                Duration::from_secs(5),
            )
            .unwrap();
    } else {
        cluster.gc_peer(region_id, 3, peer.clone()).unwrap();
    }
    drop(gc_resume_tx);
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    if initialized {
        assert_eq!(gc_started_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        fail::remove("destroy_region_before_gc_flush");
    }

    // Unsafe recovery bypasses Peer::maybe_destroy. This used to schedule a
    // second cleanup for an uninitialized peer.
    let syncer = UnsafeRecoveryExecutePlanSyncer::new(1, Arc::new(Mutex::new(router.clone())));
    syncer.abort();
    router
        .significant_send(region_id, SignificantMsg::UnsafeRecoveryDestroy(syncer))
        .unwrap();
    if initialized {
        // A target snapshot records its dependency before notifying the source.
        // It must wait for the source's existing cleanup rather than switching
        // the source back to the atomic-merge destruction protocol.
        let mut meta = cluster.store_metas[&3].lock().unwrap();
        meta.targets_map.insert(region_id, region_id + 1);
        meta.destroyed_region_for_snap.insert(region_id, true);
        meta.atomic_snap_regions
            .entry(region_id + 1)
            .or_default()
            .insert(region_id, false);
        drop(meta);
        for result in [
            MergeResultKind::FromTargetSnapshotStep1,
            MergeResultKind::FromTargetSnapshotStep2,
        ] {
            router
                .significant_send(
                    region_id,
                    SignificantMsg::MergeResult {
                        target_region_id: region_id + 1,
                        target: new_peer(3, 4),
                        result,
                    },
                )
                .unwrap();
        }
    }
    // Also exercise a direct destroy retry.
    router
        .force_send(
            region_id,
            PeerMsg::ApplyRes(Box::new(ApplyTaskRes::Destroy {
                region_id,
                peer_id: peer.get_id(),
                merge_from_snapshot: false,
            })),
        )
        .unwrap();
    if initialized {
        router
            .significant_send(region_id, SignificantMsg::RaftLogGcFlushed)
            .unwrap();
    }
    // This response is a FIFO barrier: all messages above have been processed
    // before cleanup is released.
    let request = new_status_request(region_id, peer, new_region_leader_cmd());
    cluster
        .call_command_on_node(3, request, Duration::from_secs(5))
        .unwrap();
    if initialized {
        let meta = cluster.store_metas[&3].lock().unwrap();
        assert_eq!(meta.destroyed_region_for_snap.get(&region_id), Some(&false));
        assert_eq!(
            meta.atomic_snap_regions[&(region_id + 1)].get(&region_id),
            Some(&false)
        );
    }

    drop(resume_tx);
    cluster.must_remove_region(3, region_id);
    cluster.sim.wl().clear_recv_filters(3);
    cluster.pd_client.must_add_peer(region_id, new_peer(3, 4));
    // Installing the new peer's snapshot drains earlier region-worker tasks.
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    assert_eq!(started_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    if initialized {
        let meta = cluster.store_metas[&3].lock().unwrap();
        assert!(!meta.destroyed_region_for_snap.contains_key(&region_id));
        assert!(meta.atomic_snap_regions[&(region_id + 1)].is_empty());
    }
    fail::remove("before_clear_peer_meta");
}

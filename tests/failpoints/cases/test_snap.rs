// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Mutex};
use std::time::*;
use std::{fs, io, mem, thread};

use raft::eraftpb::MessageType;

use raftstore::store::*;
use test_raftstore::*;
use tikv_util::config::*;
use tikv_util::HandyRwLock;

#[test]
fn test_overlap_cleanup() {
    let mut cluster = new_node_cluster(0, 3);
    // Disable raft log gc in this test case.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

    let gen_snapshot_fp = "region_gen_snap";

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.must_transfer_leader(region_id, new_peer(2, 2));
    // This will only pause the bootstrapped region, so the split region
    // can still work as expected.
    fail::cfg(gen_snapshot_fp, "pause").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    cluster.must_put(b"k3", b"v3");
    assert_snapshot(&cluster.get_snap_dir(2), region_id, true);
    let region1 = cluster.get_region(b"k1");
    cluster.must_split(&region1, b"k2");
    // Wait till the snapshot of split region is applied, whose range is ["", "k2").
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    // Resume the fail point and pause it again. So only the paused snapshot is generated.
    // And the paused snapshot's range is ["", ""), hence overlap.
    fail::cfg(gen_snapshot_fp, "pause").unwrap();
    // Overlap snapshot should be deleted.
    assert_snapshot(&cluster.get_snap_dir(3), region_id, false);
    fail::remove(gen_snapshot_fp);
}

// When resolving remote address, all messages will be dropped and
// report unreachable. However unreachable won't reset follower's
// progress if it's in Snapshot state. So trying to send a snapshot
// when the address is being resolved will leave follower's progress
// stay in Snapshot forever.
#[test]
fn test_server_snapshot_on_resolve_failure() {
    let mut cluster = new_server_cluster(1, 2);
    configure_for_snapshot(&mut cluster);

    let on_send_store_fp = "transport_on_send_snapshot";

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");

    let ready_notify = Arc::default();
    let (notify_tx, notify_rx) = mpsc::channel();
    cluster.sim.write().unwrap().add_send_filter(
        1,
        Box::new(MessageTypeNotifier::new(
            MessageType::MsgSnapshot,
            notify_tx,
            Arc::clone(&ready_notify),
        )),
    );

    // "return(2)" those failure occurs if TiKV resolves or sends to store 2.
    fail::cfg(on_send_store_fp, "return(2)").unwrap();
    pd_client.add_peer(1, new_learner_peer(2, 2));

    // We are ready to recv notify.
    ready_notify.store(true, Ordering::SeqCst);
    notify_rx.recv_timeout(Duration::from_secs(3)).unwrap();

    let engine2 = cluster.get_engine(2);
    must_get_none(&engine2, b"k1");

    // If snapshot status is reported correctly, sending snapshot should be retried.
    notify_rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[test]
fn test_generate_snapshot() {
    let mut cluster = new_server_cluster(1, 5);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 8;
    cluster.cfg.raft_store.merge_max_log_gap = 3;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.stop_node(4);
    cluster.stop_node(5);
    (0..10).for_each(|_| cluster.must_put(b"k2", b"v2"));
    // Sleep for a while to ensure all logs are compacted.
    thread::sleep(Duration::from_millis(100));

    fail::cfg("snapshot_delete_after_send", "pause").unwrap();

    // Let store 4 inform leader to generate a snapshot.
    cluster.run_node(4).unwrap();
    must_get_equal(&cluster.get_engine(4), b"k2", b"v2");

    fail::cfg("snapshot_enter_do_build", "pause").unwrap();
    cluster.run_node(5).unwrap();
    thread::sleep(Duration::from_millis(100));

    fail::cfg("snapshot_delete_after_send", "off").unwrap();
    must_empty_dir(cluster.get_snap_dir(1));

    // The task is droped so that we can't get the snapshot on store 5.
    fail::cfg("snapshot_enter_do_build", "pause").unwrap();
    must_get_none(&cluster.get_engine(5), b"k2");

    fail::cfg("snapshot_enter_do_build", "off").unwrap();
    must_get_equal(&cluster.get_engine(5), b"k2", b"v2");

    fail::remove("snapshot_enter_do_build");
    fail::remove("snapshot_delete_after_send");
}

fn must_empty_dir(path: String) {
    for _ in 0..500 {
        thread::sleep(Duration::from_millis(10));
        if fs::read_dir(&path).unwrap().count() == 0 {
            return;
        }
    }

    let entries = fs::read_dir(&path)
        .and_then(|dir| dir.collect::<io::Result<Vec<_>>>())
        .unwrap();
    if !entries.is_empty() {
        panic!(
            "the directory {:?} should be empty, but has entries: {:?}",
            path, entries
        );
    }
}

fn assert_snapshot(snap_dir: &str, region_id: u64, exist: bool) {
    let region_id = format!("{}", region_id);
    let timer = Instant::now();
    loop {
        for p in fs::read_dir(&snap_dir).unwrap() {
            let name = p.unwrap().file_name().into_string().unwrap();
            let mut parts = name.split('_');
            parts.next();
            if parts.next().unwrap() == region_id && exist {
                return;
            }
        }
        if !exist {
            return;
        }

        if timer.elapsed() < Duration::from_secs(6) {
            thread::sleep(Duration::from_millis(20));
        } else {
            panic!(
                "assert snapshot [exist: {}, region: {}] fail",
                exist, region_id
            );
        }
    }
}

#[test]
fn test_node_request_snapshot_on_split() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_request_snapshot(&mut cluster);
    cluster.run();

    let region = cluster.get_region(b"");
    // Make sure peer 2 does not in the pending state.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    for _ in 0..100 {
        cluster.must_put(&[7; 100], &[7; 100]);
    }
    cluster.must_transfer_leader(1, new_peer(3, 3));

    let split_fp = "apply_before_split_1_3";
    fail::cfg(split_fp, "pause").unwrap();
    let (split_tx, split_rx) = mpsc::channel();
    cluster.split_region(
        &region,
        b"k1",
        Callback::Write(Box::new(move |_| {
            split_tx.send(()).unwrap();
        })),
    );
    // Split is stopped on peer3.
    split_rx
        .recv_timeout(Duration::from_millis(100))
        .unwrap_err();

    // Request snapshot.
    let committed_index = cluster.must_request_snapshot(2, region.get_id());

    // Install snapshot filter after requesting snapshot.
    let (tx, rx) = mpsc::channel();
    let notifier = Mutex::new(Some(tx));
    cluster.sim.wl().add_recv_filter(
        2,
        Box::new(RecvSnapshotFilter {
            notifier,
            region_id: region.get_id(),
        }),
    );
    // There is no snapshot as long as we pause the split.
    rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

    // Continue split.
    fail::remove(split_fp);
    split_rx.recv().unwrap();
    let mut m = rx.recv().unwrap();
    let snapshot = m.take_message().take_snapshot();

    // Requested snapshot_index >= committed_index.
    assert!(
        snapshot.get_metadata().get_index() >= committed_index,
        "{:?} | {}",
        m,
        committed_index
    );
}

// A peer on store 3 is isolated and is applying snapshot. (add failpoint so it's always pending)
// Then two conf change happens, this peer is removed and a new peer is added on store 3.
// Then isolation clear, this peer will be destroyed because of a bigger peer id in msg.
// In previous implementation, peer fsm can be destroyed synchronously because snapshot state is
// pending and can be canceled, but panic may happen if the applyfsm runs very slow.
#[test]
fn test_destroy_peer_on_pending_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k1", b"v1");
    // Ensure peer 3 is initialized.
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.add_send_filter(IsolationFilterFactory::new(3));

    for i in 0..20 {
        cluster.must_put(format!("k1{}", i).as_bytes(), b"v1");
    }

    let apply_snapshot_fp = "apply_pending_snapshot";
    fail::cfg(apply_snapshot_fp, "return()").unwrap();

    cluster.clear_send_filters();
    // Wait for leader send snapshot.
    sleep_ms(100);

    cluster.add_send_filter(IsolationFilterFactory::new(3));
    // Don't send check stale msg to PD
    let peer_check_stale_state_fp = "peer_check_stale_state";
    fail::cfg(peer_check_stale_state_fp, "return()").unwrap();

    pd_client.must_remove_peer(r1, new_peer(3, 3));
    pd_client.must_add_peer(r1, new_peer(3, 4));

    let before_handle_normal_3_fp = "before_handle_normal_3";
    fail::cfg(before_handle_normal_3_fp, "pause").unwrap();

    cluster.clear_send_filters();
    // Wait for leader send msg to peer 3.
    // Then destroy peer 3 and create peer 4.
    sleep_ms(100);

    fail::remove(apply_snapshot_fp);

    fail::remove(before_handle_normal_3_fp);

    cluster.must_put(b"k120", b"v1");
    // After peer 4 has applied snapshot, data should be got.
    must_get_equal(&cluster.get_engine(3), b"k120", b"v1");
}

#[test]
fn test_shutdown_when_snap_gc() {
    let mut cluster = new_node_cluster(0, 2);
    // So that batch system can handle a snap_gc event before shutting down.
    cluster.cfg.raft_store.store_batch_system.max_batch_size = 1;
    cluster.cfg.raft_store.snap_mgr_gc_tick_interval = ReadableDuration::millis(20);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    // Only save a snapshot on peer 2, but do not apply it really.
    fail::cfg("skip_schedule_applying_snapshot", "return").unwrap();
    pd_client.must_add_peer(r1, new_learner_peer(2, 2));

    // Snapshot directory on store 2 shouldn't be empty.
    let snap_dir = cluster.get_snap_dir(2);
    for i in 0..=100 {
        if i == 100 {
            panic!("store 2 snap dir must not be empty");
        }
        let dir = fs::read_dir(&snap_dir).unwrap();
        if dir.count() > 0 {
            break;
        }
        sleep_ms(10);
    }

    fail::cfg("peer_2_handle_snap_mgr_gc", "pause").unwrap();
    std::thread::spawn(|| {
        // Sleep a while to wait snap_gc event to reach batch system.
        sleep_ms(500);
        fail::cfg("peer_2_handle_snap_mgr_gc", "off").unwrap();
    });

    sleep_ms(100);
    cluster.stop_node(2);

    let snap_dir = cluster.get_snap_dir(2);
    let dir = fs::read_dir(&snap_dir).unwrap();
    if dir.count() == 0 {
        panic!("store 2 snap dir must not be empty");
    }
}

// Test if a peer handle the old snapshot properly.
#[test]
fn test_receive_old_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    // Bypass the snapshot gc because the snapshot may be used twice.
    let peer_2_handle_snap_mgr_gc_fp = "peer_2_handle_snap_mgr_gc";
    fail::cfg(peer_2_handle_snap_mgr_gc_fp, "return()").unwrap();

    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_transfer_leader(r1, new_peer(1, 1));

    cluster.must_put(b"k00", b"v1");
    // Ensure peer 2 is initialized.
    must_get_equal(&cluster.get_engine(2), b"k00", b"v1");

    cluster.add_send_filter(IsolationFilterFactory::new(2));

    for i in 0..20 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let recv_filter = Box::new(
        RegionPacketFilter::new(r1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgSnapshot)
            .reserve_dropped(Arc::clone(&dropped_msgs)),
    );
    cluster.sim.wl().add_recv_filter(2, recv_filter);

    cluster.clear_send_filters();

    for _ in 0..20 {
        let guard = dropped_msgs.lock().unwrap();
        if !guard.is_empty() {
            break;
        }
        drop(guard);
        sleep_ms(10);
    }
    let msgs = {
        let mut guard = dropped_msgs.lock().unwrap();
        if guard.is_empty() {
            drop(guard);
            panic!("do not receive snapshot msg in 200ms");
        }
        mem::replace(guard.as_mut(), vec![])
    };

    cluster.sim.wl().clear_recv_filters(2);

    for i in 20..40 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }
    must_get_equal(&cluster.get_engine(2), b"k39", b"v1");

    let router = cluster.sim.wl().get_router(2).unwrap();
    // Send the old snapshot
    for raft_msg in msgs {
        router.send_raft_message(raft_msg).unwrap();
    }

    cluster.must_put(b"k40", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k40", b"v1");

    pd_client.must_remove_peer(r1, new_peer(2, 2));

    must_get_none(&cluster.get_engine(2), b"k40");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k5");

    let left = cluster.get_region(b"k1");
    pd_client.must_add_peer(left.get_id(), new_peer(2, 4));

    cluster.must_put(b"k11", b"v1");
    // If peer 2 handles previous old snapshot properly and does not leave over metadata
    // in `pending_snapshot_regions`, peer 4 should be created normally.
    must_get_equal(&cluster.get_engine(2), b"k11", b"v1");

    fail::remove(peer_2_handle_snap_mgr_gc_fp);
}

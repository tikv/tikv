// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::*;

use fail;
use raft::eraftpb::MessageType;

use test_raftstore::*;
use tikv::raftstore::store::*;
use tikv_util::config::*;
use tikv_util::HandyRwLock;

#[test]
fn test_overlap_cleanup() {
    let _guard = crate::setup();
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
    let _guard = crate::setup();
    let mut cluster = new_server_cluster(1, 4);
    configure_for_snapshot(&mut cluster);

    let on_resolve_fp = "transport_snapshot_on_resolve";
    let on_send_store_fp = "transport_on_send_store";

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    pd_client.must_remove_peer(1, new_peer(4, 4));
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

    let (drop_snapshot_tx, drop_snapshot_rx) = mpsc::channel();
    cluster
        .sim
        .write()
        .unwrap()
        .add_recv_filter(4, Box::new(DropSnapshotFilter::new(drop_snapshot_tx)));

    pd_client.add_peer(1, new_peer(4, 5));

    // The leader is trying to send snapshots, but the filter drops snapshots.
    drop_snapshot_rx
        .recv_timeout(Duration::from_secs(3))
        .unwrap();

    // "return(4)" those failure occurs if TiKV resolves or sends to store 4.
    fail::cfg(on_resolve_fp, "return(4)").unwrap();
    fail::cfg(on_send_store_fp, "return(4)").unwrap();

    // We are ready to recv notify.
    ready_notify.store(true, Ordering::SeqCst);
    notify_rx.recv_timeout(Duration::from_secs(3)).unwrap();

    let engine4 = cluster.get_engine(4);
    must_get_none(&engine4, b"k1");
    cluster.sim.write().unwrap().clear_recv_filters(4);

    // Remove the on_send_store_fp.
    // Now it will resolve the store 4's address via heartbeat messages,
    // so snapshots works fine.
    //
    // But keep the on_resolve_fp.
    // Any snapshot messages that has been sent before will meet the
    // injected resolve failure eventually.
    // It perverts a race condition, remove the on_resolve_fp before snapshot
    // messages meet the failpoint, that fails the test.
    fail::remove(on_send_store_fp);

    notify_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&engine4, b"k1", b"v1");
    must_get_equal(&engine4, b"k2", b"v2");

    // Clean up.
    fail::remove(on_resolve_fp);
}

#[test]
fn test_generate_snapshot() {
    let _guard = crate::setup();

    let mut cluster = new_server_cluster(1, 5);
    configure_for_snapshot(&mut cluster);
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
    let _guard = crate::setup();

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
// Peerfsm can be destroyed synchronously because snapshot state is pending and can be canceled.
// I.e. async_remove is false.
#[test]
fn test_destroy_peer_on_pending_snapshot() {
    let _guard = crate::setup();

    let mut cluster = new_server_cluster(0, 4);
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

    pd_client.must_remove_peer(r1, new_peer(3, 3));
    pd_client.must_add_peer(r1, new_peer(4, 4));

    pd_client.must_remove_peer(r1, new_peer(4, 4));
    pd_client.must_add_peer(r1, new_peer(3, 5));

    let destroy_peer_fp = "destroy_peer";
    fail::cfg(destroy_peer_fp, "pause").unwrap();
    cluster.clear_send_filters();
    // Wait for leader send msg to peer 3.
    // Then destroy peer 3 and create peer 5.
    sleep_ms(100);
    fail::remove(destroy_peer_fp);

    fail::remove(apply_snapshot_fp);
    // After peer 5 has applied snapshot, data should be got.
    must_get_equal(&cluster.get_engine(3), b"k119", b"v1");
}

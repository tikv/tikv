// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::*;

use fail;
use raft::eraftpb::MessageType;

use test_raftstore::*;
use tikv::util::config::*;

#[test]
fn test_overlap_cleanup() {
    let _guard = ::setup();
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

    // This will only pause the bootstrapped region, so the split region
    // can still work as expected.
    fail::cfg(gen_snapshot_fp, "pause").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    cluster.must_put(b"k3", b"v3");
    let region1 = cluster.get_region(b"k1");
    cluster.must_split(&region1, b"k2");
    // Wait till the snapshot of split region is applied, whose range is ["", "k2").
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    // Resume the fail point and pause it again. So only the paused snapshot is generated.
    // And the paused snapshot's range is ["", ""), hence overlap.
    fail::cfg(gen_snapshot_fp, "pause").unwrap();
    // Wait a little bit for the message being sent out.
    thread::sleep(Duration::from_secs(1));
    // Overlap snapshot should be deleted.
    let snap_dir = cluster.get_snap_dir(3);
    for p in fs::read_dir(&snap_dir).unwrap() {
        let name = p.unwrap().file_name().into_string().unwrap();
        let mut parts = name.split('_');
        parts.next();
        if parts.next().unwrap() == "1" {
            panic!("snapshot of region 1 should be deleted.");
        }
    }
    fail::remove(gen_snapshot_fp);
}

// When resolving remote address, all messages will be dropped and
// report unreachable. However unreachable won't reset follower's
// progress if it's in Snapshot state. So trying to send a snapshot
// when the address is being resolved will leave follower's progress
// stay in Snapshot forever.
#[test]
fn test_server_snapshot_on_resolve_failure() {
    let _guard = ::setup();
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
        box MessageTypeNotifier::new(
            MessageType::MsgSnapshot,
            notify_tx,
            Arc::clone(&ready_notify),
        ),
    );

    let (drop_snapshot_tx, drop_snapshot_rx) = mpsc::channel();
    cluster
        .sim
        .write()
        .unwrap()
        .add_recv_filter(4, box DropSnapshotFilter::new(drop_snapshot_tx));

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
    let _guard = ::setup();

    let mut cluster = new_server_cluster(1, 5);
    configure_for_snapshot(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();
    cluster.stop_node(4);
    cluster.stop_node(5);
    (0..10).for_each(|_| cluster.must_put(b"k2", b"v2"));
    // Sleep for a while to ensure all logs are compacted.
    thread::sleep(Duration::from_millis(100));

    fail::cfg("snapshot_delete_after_send", "pause").unwrap();

    // Let store 4 inform leader to generate a snapshot.
    cluster.run_node(4);
    must_get_equal(&cluster.get_engine(4), b"k2", b"v2");

    fail::cfg("snapshot_enter_do_build", "pause").unwrap();
    cluster.run_node(5);
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

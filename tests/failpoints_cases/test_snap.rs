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

use std::*;
use std::time::*;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};

use fail;
use tikv::util::config::*;
use tikv::raftstore::Result;
use raft::eraftpb::MessageType;
use kvproto::raft_serverpb::RaftMessage;

use raftstore::cluster::Simulator;
use raftstore::transport_simulate::*;
use raftstore::node::new_node_cluster;
use raftstore::server::new_server_cluster;
use raftstore::util::*;

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

#[test]
fn test_snapshot_between_save() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();

    pd_client.must_add_peer(region_id, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // Add peer on store 3 so that it will be shutdown by fail point.
    fail::cfg("raft_snapshot_between_save", "return").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    cluster.stop_node(3);

    // Reset fail point and restart store 3.
    fail::cfg("raft_snapshot_between_save", "off").unwrap();
    cluster.run_node(3);

    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    fail::remove("raft_snapshot_between_save");
}

#[test]
fn test_snapshot_apply_validate_fail() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();

    pd_client.must_add_peer(region_id, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    fail::cfg("raft_snapshot_validate", "return").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k1");

    fail::cfg("raft_snapshot_validate", "off").unwrap();
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    fail::remove("raft_snapshot_validate");
}

pub struct SnapshotNotifier {
    notifier: Mutex<Sender<()>>,
    pending_notify: AtomicUsize,
    ready_notify: Arc<AtomicBool>,
}

impl SnapshotNotifier {
    pub fn new(notifier: Sender<()>, ready_notify: Arc<AtomicBool>) -> SnapshotNotifier {
        SnapshotNotifier {
            notifier: Mutex::new(notifier),
            ready_notify: ready_notify,
            pending_notify: AtomicUsize::new(0),
        }
    }
}

impl Filter<RaftMessage> for SnapshotNotifier {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        for msg in msgs.iter() {
            if msg.get_message().get_msg_type() == MessageType::MsgSnapshot
                && self.ready_notify.load(Ordering::SeqCst)
            {
                self.pending_notify.fetch_add(1, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    fn after(&self, _: Result<()>) -> Result<()> {
        while self.pending_notify.load(Ordering::SeqCst) > 0 {
            debug!("notify snapshot");
            self.pending_notify.fetch_sub(1, Ordering::SeqCst);
            let _ = self.notifier.lock().unwrap().send(());
        }
        Ok(())
    }
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
        box SnapshotNotifier::new(notify_tx, Arc::clone(&ready_notify)),
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

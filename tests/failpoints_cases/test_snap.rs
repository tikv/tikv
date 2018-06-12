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

use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::time::*;
use std::*;

use fail;
use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::MessageType;
use tikv::raftstore::Result;
use tikv::util::config::*;

use raftstore::cluster::Simulator;
use raftstore::node::new_node_cluster;
use raftstore::server::new_server_cluster;
use raftstore::transport_simulate::*;
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

// TODO: maybe merge with MessageTypeNotifier
pub struct SnapshotNotifier {
    notifier: Mutex<Sender<()>>,
}

impl SnapshotNotifier {
    pub fn new(notifier: Sender<()>) -> SnapshotNotifier {
        SnapshotNotifier {
            notifier: Mutex::new(notifier),
        }
    }
}

impl Filter<RaftMessage> for SnapshotNotifier {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        for msg in msgs.iter() {
            if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
                let _ = self.notifier.lock().unwrap().send(());
            }
        }

        Ok(())
    }
}

// It should retry when fail to send snapshot.
#[test]
fn test_server_snapshot_failure() {
    let _guard = ::setup();
    let mut cluster = new_server_cluster(1, 4);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    pd_client.must_remove_peer(1, new_peer(4, 4));
    cluster.must_put(b"k1", b"v1");

    let (notify_tx, notify_rx) = mpsc::channel();
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(1, box SnapshotNotifier::new(notify_tx));

    fail::cfg("snapshot_before_send", "return(failure)").unwrap();

    pd_client.add_peer(1, new_peer(4, 5));

    // First try should fail.
    notify_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    // Second try means retry is working.
    notify_rx.recv_timeout(Duration::from_secs(3)).unwrap();

    fail::remove("snapshot_before_send");

    cluster.must_put(b"k2", b"v2");
    let engine4 = cluster.get_engine(4);
    must_get_equal(&engine4, b"k1", b"v1");
    must_get_equal(&engine4, b"k2", b"v2");
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

// Copyright 2016 PingCAP, Inc.
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
use std::time::{Duration, Instant};
use std::sync::{Arc, RwLock, Mutex};
use std::sync::mpsc::{self, Sender};
use std::sync::atomic::{AtomicBool, Ordering};

use tikv::raftstore::Result;
use tikv::raftstore::store::Msg;
use tikv::util::HandyRwLock;
use kvproto::eraftpb::{Message, MessageType};
use kvproto::raft_serverpb::RaftMessage;

use super::transport_simulate::*;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util::*;


fn test_huge_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_log();
    cluster.cfg.raft_store.raft_log_gc_count_limit = 1000;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = 10;
    cluster.cfg.raft_store.snap_apply_batch_size = 500;
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();

    let r1 = cluster.run_conf_change();

    // at least 4m data
    for i in 0..2 * 1024 {
        let key = format!("{:01024}", i);
        let value = format!("{:01024}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, &format!("{:01024}", 0).into_bytes());
    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    must_get_equal(&engine_2, key, value);

    // now snapshot must be applied on peer 2;
    let key = format!("{:01024}", 0);
    let value = format!("{:01024}", 0);
    must_get_equal(&engine_2, key.as_bytes(), value.as_bytes());
    let stale = Arc::new(AtomicBool::new(false));
    cluster.sim.wl().add_recv_filter(3, box LeadingDuplicatedSnapshotFilter::new(stale.clone()));
    pd_client.must_add_peer(r1, new_peer(3, 3));
    let mut i = 2 * 1024;
    loop {
        i += 1;
        let key = format!("{:01024}", i);
        let value = format!("{:01024}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
        if stale.load(Ordering::Relaxed) {
            break;
        }
        if i > 10 * 1024 {
            panic!("snapshot should be sent twice after {} kvs", i);
        }
    }
    cluster.must_put(b"k3", b"v3");
    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k3", b"v3");

    // TODO: add more tests.
}

#[test]
fn test_node_huge_snapshot() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_huge_snapshot(&mut cluster);
}

#[test]
fn test_server_huge_snapshot() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_huge_snapshot(&mut cluster);
}

fn test_snap_gc<T: Simulator>(cluster: &mut Cluster<T>) {
    // truncate the log quickly so that we can force sending snapshot.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = 20;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 2;
    cluster.cfg.raft_store.snap_mgr_gc_tick_interval = 50;
    cluster.cfg.raft_store.snap_gc_timeout = 2;

    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    let (tx, rx) = mpsc::channel();
    // drop all the snapshot so we can detect stale snapfile.
    cluster.sim.wl().add_recv_filter(3, box DropSnapshotFilter::new(tx));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let first_snap_idx = rx.recv_timeout(Duration::from_secs(3)).unwrap();

    cluster.must_put(b"k2", b"v2");

    // node 1 and node 2 must have k2, but node 3 must not.
    for i in 1..3 {
        let engine = cluster.get_engine(i);
        must_get_equal(&engine, b"k2", b"v2");
    }

    let engine3 = cluster.get_engine(3);
    must_get_none(&engine3, b"k2");

    for _ in 0..30 {
        // write many logs to force log GC for region 1 and region 2.
        // and trigger snapshot more than one time.
        cluster.must_put(b"k1", b"v1");
        cluster.must_put(b"k2", b"v2");
    }

    let mut now = Instant::now();
    loop {
        let snap_index = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        if snap_index != first_snap_idx {
            break;
        }
        if now.elapsed() >= Duration::from_secs(5) {
            panic!("can't get any snap after {}", first_snap_idx);
        }
    }

    let snap_dir = cluster.get_snap_dir(3);
    // it must have more than 2 snaps.
    let snapfiles: Vec<_> = fs::read_dir(snap_dir).unwrap().map(|p| p.unwrap().path()).collect();
    assert!(snapfiles.len() >= 2);

    cluster.sim.wl().clear_recv_filters(3);
    debug!("filters cleared.");

    // node 3 must have k1, k2.
    must_get_equal(&engine3, b"k1", b"v1");
    must_get_equal(&engine3, b"k2", b"v2");

    now = Instant::now();
    loop {
        let mut snap_files = vec![];
        for i in 1..4 {
            let snap_dir = cluster.get_snap_dir(i);
            // snapfiles should be gc.
            snap_files.extend(fs::read_dir(snap_dir).unwrap().map(|p| p.unwrap().path()));
        }
        if snap_files.is_empty() {
            return;
        }
        if now.elapsed() > Duration::from_secs(10) {
            panic!("snap files is still not empty: {:?}", snap_files);
        }
        // trigger log compaction.
        cluster.must_put(b"k2", b"v2");
        sleep_ms(20);
    }
}

#[test]
fn test_node_snap_gc() {
    let mut cluster = new_node_cluster(0, 3);
    test_snap_gc(&mut cluster);
}

#[test]
fn test_server_snap_gc() {
    let mut cluster = new_server_cluster(0, 3);
    test_snap_gc(&mut cluster);
}

fn test_concurrent_snap<T: Simulator>(cluster: &mut Cluster<T>) {
    // util::init_log();
    // disable raft log gc.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = 60000;

    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(r1, new_peer(2, 2));
    // peer 2 can't step to leader.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(r1, 2)
        .msg_type(MessageType::MsgRequestVote)
        .direction(Direction::Send)));
    cluster.must_transfer_leader(r1, new_peer(1, 1));
    cluster.must_put(b"k3", b"v3");
    // Now peer 1 can't send snapshot to peer 2
    cluster.sim.wl().add_recv_filter(3, box PauseFirstSnapshotFilter::new());
    cluster.sim.wl().add_recv_filter(1, box PauseFirstSnapshotFilter::new());
    pd_client.must_add_peer(r1, new_peer(3, 3));
    let region = cluster.get_region(b"k1");
    // TODO: should check whether snapshot has been sent before split.
    cluster.must_split(&region, b"k2");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
}

#[test]
fn test_node_concurrent_snap() {
    let mut cluster = new_node_cluster(0, 3);
    test_concurrent_snap(&mut cluster);
}

#[test]
fn test_server_concurrent_snap() {
    let mut cluster = new_server_cluster(0, 3);
    test_concurrent_snap(&mut cluster);
}

fn test_cf_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    // truncate the log quickly so that we can force sending snapshot.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = 20;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 2;
    cluster.cfg.raft_store.snap_mgr_gc_tick_interval = 50;
    cluster.cfg.raft_store.snap_gc_timeout = 2;

    cluster.run();
    let cf = "lock";
    cluster.must_put_cf(cf, b"k1", b"v1");
    cluster.must_put_cf(cf, b"k2", b"v2");
    let engine1 = cluster.get_engine(1);
    must_get_cf_equal(&engine1, cf, b"k1", b"v1");
    must_get_cf_equal(&engine1, cf, b"k2", b"v2");

    // Isolate node 1.
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    // Write some data to trigger snapshot.
    for i in 100..110 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        cluster.must_put_cf(cf, key.as_bytes(), value.as_bytes());
    }

    cluster.must_delete_cf(cf, b"k2");

    // Add node 1 back.
    cluster.clear_send_filters();

    // Now snapshot must be applied on node 1.
    must_get_cf_equal(&engine1, cf, b"k1", b"v1");
    must_get_cf_none(&engine1, cf, b"k2");

    // test if node can be safely restarted without losing any data.
    cluster.stop_node(1);
    cluster.run_node(1);

    cluster.must_put_cf(cf, b"k3", b"v3");
    must_get_cf_equal(&engine1, cf, b"k3", b"v3");
}

#[test]
fn test_node_cf_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    test_cf_snapshot(&mut cluster);
}

#[test]
fn test_server_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    test_cf_snapshot(&mut cluster);
}

// replace content of all the snapshots with the first snapshot it received.
struct StaleSnap {
    first_snap: RwLock<Option<Message>>,
    sent: Mutex<Sender<()>>,
}

impl Filter<RaftMessage> for Arc<StaleSnap> {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut res = Vec::with_capacity(msgs.len());
        for mut m in msgs.drain(..) {
            if m.get_message().get_msg_type() == MessageType::MsgSnapshot &&
               m.get_to_peer().get_store_id() == 3 {
                if self.first_snap.rl().is_none() {
                    *self.first_snap.wl() = Some(m.take_message());
                    continue;
                } else {
                    let from = m.get_message().get_from();
                    let to = m.get_message().get_to();
                    m.set_message(self.first_snap.rl().as_ref().unwrap().clone());
                    m.mut_message().set_from(from);
                    m.mut_message().set_to(to);
                    let _ = self.sent.lock().unwrap().send(());
                }
            }
            res.push(m);
        }
        *msgs = res;
        check_messages(msgs)
    }
}

#[test]
fn test_node_stale_snap() {
    let mut cluster = new_node_cluster(0, 3);
    // disable compact log to make snapshot only be sent when peer is first added.
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 1000;

    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();

    let r1 = cluster.run_conf_change();
    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let (tx, rx) = mpsc::channel();
    let filter = StaleSnap {
        first_snap: RwLock::default(),
        sent: Mutex::new(tx),
    };
    cluster.add_send_filter(CloneFilterFactory(Arc::new(filter)));
    pd_client.must_add_peer(r1, new_peer(3, 3));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    pd_client.must_remove_peer(r1, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k2");
    pd_client.must_add_peer(r1, new_peer(3, 4));

    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(2), b"k3", b"v3");
    rx.recv().unwrap();
    sleep_ms(2000);
    must_get_none(&cluster.get_engine(3), b"k3");
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
}

/// Pause Snap and wait till first append message arrives.
pub struct SnapshotAppendFilter {
    stale: AtomicBool,
    pending_msg: Mutex<Vec<Msg>>,
    notifier: Mutex<Sender<()>>,
}

impl SnapshotAppendFilter {
    pub fn new(notifier: Sender<()>) -> SnapshotAppendFilter {
        SnapshotAppendFilter {
            stale: AtomicBool::new(false),
            pending_msg: Mutex::new(vec![]),
            notifier: Mutex::new(notifier),
        }
    }
}

impl Filter<Msg> for SnapshotAppendFilter {
    fn before(&self, msgs: &mut Vec<Msg>) -> Result<()> {
        if self.stale.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut to_send = vec![];
        let mut pending_msg = self.pending_msg.lock().unwrap();
        let mut stale = false;
        for m in msgs.drain(..) {
            let mut should_collect = false;
            if let Msg::RaftMessage(ref msg) = m {
                should_collect = !stale &&
                                 msg.get_message().get_msg_type() == MessageType::MsgSnapshot;
                stale = !pending_msg.is_empty() &&
                        msg.get_message().get_msg_type() == MessageType::MsgAppend;
            }
            if should_collect {
                pending_msg.push(m);
                self.notifier.lock().unwrap().send(()).unwrap();
            } else {
                if stale {
                    to_send.extend(pending_msg.drain(..));
                }
                to_send.push(m);
            }
        }
        self.stale.store(stale, Ordering::SeqCst);
        msgs.extend(to_send);
        Ok(())
    }
}

fn test_snapshot_with_append<T: Simulator>(cluster: &mut Cluster<T>) {
    // truncate the log quickly so that we can force sending snapshot.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = 20;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 2;
    cluster.cfg.raft_store.snap_mgr_gc_tick_interval = 50;
    cluster.cfg.raft_store.snap_gc_timeout = 2;

    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();
    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let (tx, rx) = mpsc::channel();
    cluster.sim.wl().add_recv_filter(4, box SnapshotAppendFilter::new(tx));
    pd_client.add_peer(r1, new_peer(4, 4));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    let engine4 = cluster.get_engine(4);
    must_get_equal(&engine4, b"k1", b"v1");
    must_get_equal(&engine4, b"k2", b"v2");
}

#[test]
fn test_node_snapshot_with_append() {
    let mut cluster = new_node_cluster(0, 4);
    test_snapshot_with_append(&mut cluster);
}

#[test]
fn test_server_snapshot_with_append() {
    let mut cluster = new_server_cluster(0, 4);
    test_snapshot_with_append(&mut cluster);
}

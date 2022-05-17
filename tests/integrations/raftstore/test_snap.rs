// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Sender},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use engine_traits::{KvEngine, RaftEngineReadOnly};
use file_system::{IOOp, IOType};
use futures::executor::block_on;
use grpcio::Environment;
use kvproto::raft_serverpb::*;
use raft::eraftpb::{Message, MessageType, Snapshot};
use raftstore::{store::*, Result};
use rand::Rng;
use security::SecurityManager;
use test_raftstore::*;
use tikv::server::snap::send_snap;
use tikv_util::{config::*, time::Instant, HandyRwLock};

fn test_huge_snapshot<T: Simulator>(cluster: &mut Cluster<T>, max_snapshot_file_size: u64) {
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(500);
    cluster.cfg.raft_store.max_snapshot_file_raw_size = ReadableSize(max_snapshot_file_size);
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    let first_value = vec![0; 10240];
    // at least 4m data
    for i in 0..400 {
        let key = format!("{:03}", i);
        cluster.must_put(key.as_bytes(), &first_value);
    }
    let first_key: &[u8] = b"000";

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, first_key);
    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    must_get_equal(&engine_2, key, value);

    // now snapshot must be applied on peer 2;
    must_get_equal(&engine_2, first_key, &first_value);
    let stale = Arc::new(AtomicBool::new(false));
    cluster.sim.wl().add_recv_filter(
        3,
        Box::new(LeadingDuplicatedSnapshotFilter::new(
            Arc::clone(&stale),
            false,
        )),
    );
    pd_client.must_add_peer(r1, new_peer(3, 3));
    let mut i = 400;
    loop {
        i += 1;
        let key = format!("{:03}", i);
        cluster.must_put(key.as_bytes(), &first_value);
        if stale.load(Ordering::Relaxed) {
            break;
        }
        if i > 1000 {
            panic!("snapshot should be sent twice after {} kvs", i);
        }
    }
    cluster.must_put(b"k3", b"v3");
    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k3", b"v3");

    // TODO: add more tests.
}

#[test]
fn test_server_huge_snapshot() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_huge_snapshot(&mut cluster, u64::MAX);
}

#[test]
fn test_server_huge_snapshot_multi_files() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_huge_snapshot(&mut cluster, 1024 * 1024);
}

fn test_server_snap_gc_internal(version: &str) {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    cluster.pd_client.reset_version(version);
    cluster.cfg.raft_store.snap_gc_timeout = ReadableDuration::millis(300);
    cluster.cfg.raft_store.max_snapshot_file_raw_size = ReadableSize::mb(100);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let (tx, rx) = mpsc::channel();
    // drop all the snapshot so we can detect stale snapfile.
    cluster
        .sim
        .wl()
        .add_recv_filter(3, Box::new(DropSnapshotFilter::new(tx)));
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
        if now.saturating_elapsed() >= Duration::from_secs(5) {
            panic!("can't get any snap after {}", first_snap_idx);
        }
    }

    let snap_dir = cluster.get_snap_dir(3);
    // it must have more than 2 snaps.

    assert!(
        fs::read_dir(snap_dir)
            .unwrap()
            .filter(|p| p.is_ok())
            .count()
            >= 2
    );

    let actual_max_per_file_size = cluster.get_snap_mgr(1).get_actual_max_per_file_size(true);

    // version > 6.0.0 should enable multi_snapshot_file feature, which means actual max_per_file_size equals the config
    if version == "6.5.0" {
        assert!(actual_max_per_file_size == cluster.cfg.raft_store.max_snapshot_file_raw_size.0);
    } else {
        // the feature is disabled, and the actual_max_per_file_size should be u64::MAX (so that only one file is generated)
        assert!(actual_max_per_file_size == u64::MAX);
    }

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
        if now.saturating_elapsed() > Duration::from_secs(10) {
            panic!("snap files is still not empty: {:?}", snap_files);
        }
        sleep_ms(20);
    }
}

#[test]
fn test_server_snap_gc_with_multi_snapshot_files() {
    test_server_snap_gc_internal("6.5.0");
}

#[test]
fn test_server_snap_gc() {
    test_server_snap_gc_internal("5.1.0");
}

/// A helper function for testing the handling of snapshot is correct
/// when there are multiple snapshots which have overlapped region ranges
/// arrive at the same raftstore.
fn test_concurrent_snap<T: Simulator>(cluster: &mut Cluster<T>) {
    // Disable raft log gc in this test case.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(r1, new_peer(2, 2));
    // Force peer 2 to be followers all the way.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1, 2)
            .msg_type(MessageType::MsgRequestVote)
            .direction(Direction::Send),
    ));
    cluster.must_transfer_leader(r1, new_peer(1, 1));
    cluster.must_put(b"k3", b"v3");
    // Pile up snapshots of overlapped region ranges and deliver them all at once.
    let (tx, rx) = mpsc::channel();
    cluster
        .sim
        .wl()
        .add_recv_filter(3, Box::new(CollectSnapshotFilter::new(tx)));
    pd_client.must_add_peer(r1, new_peer(3, 3));
    let region = cluster.get_region(b"k1");
    // Ensure the snapshot of range ("", "") is sent and piled in filter.
    if let Err(e) = rx.recv_timeout(Duration::from_secs(1)) {
        panic!("the snapshot is not sent before split, e: {:?}", e);
    }
    // Split the region range and then there should be another snapshot for the split ranges.
    cluster.must_split(&region, b"k2");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
    // Ensure the regions work after split.
    cluster.must_put(b"k11", b"v11");
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
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
    configure_for_snapshot(cluster);

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
    cluster.run_node(1).unwrap();

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
#[derive(Clone)]
struct StaleSnap {
    inner: Arc<StaleSnapInner>,
}

struct StaleSnapInner {
    first_snap: RwLock<Option<Message>>,
    sent: Mutex<Sender<()>>,
}

impl Filter for StaleSnap {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut res = Vec::with_capacity(msgs.len());
        for mut m in msgs.drain(..) {
            if m.get_message().get_msg_type() == MessageType::MsgSnapshot
                && m.get_to_peer().get_store_id() == 3
            {
                if self.inner.first_snap.rl().is_none() {
                    *self.inner.first_snap.wl() = Some(m.take_message());
                    continue;
                } else {
                    let from = m.get_message().get_from();
                    let to = m.get_message().get_to();
                    m.set_message(self.inner.first_snap.rl().as_ref().unwrap().clone());
                    m.mut_message().set_from(from);
                    m.mut_message().set_to(to);
                    let _ = self.inner.sent.lock().unwrap().send(());
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
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let (tx, rx) = mpsc::channel();
    let filter = StaleSnap {
        inner: Arc::new(StaleSnapInner {
            first_snap: RwLock::default(),
            sent: Mutex::new(tx),
        }),
    };
    cluster.add_send_filter(CloneFilterFactory(filter));
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
    pending_msg: Mutex<Vec<RaftMessage>>,
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

impl Filter for SnapshotAppendFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        if self.stale.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut to_send = vec![];
        let mut pending_msg = self.pending_msg.lock().unwrap();
        let mut stale = false;
        for msg in msgs.drain(..) {
            let should_collect =
                !stale && msg.get_message().get_msg_type() == MessageType::MsgSnapshot;
            stale = !pending_msg.is_empty()
                && msg.get_message().get_msg_type() == MessageType::MsgAppend;
            if should_collect {
                pending_msg.push(msg);
                self.notifier.lock().unwrap().send(()).unwrap();
            } else {
                if stale {
                    to_send.extend(pending_msg.drain(..));
                }
                to_send.push(msg);
            }
        }
        self.stale.store(stale, Ordering::SeqCst);
        msgs.extend(to_send);
        Ok(())
    }
}

fn test_snapshot_with_append<T: Simulator>(cluster: &mut Cluster<T>) {
    configure_for_snapshot(cluster);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    cluster.run();

    // In case of removing leader, let's transfer leader to some node first.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    pd_client.must_remove_peer(1, new_peer(4, 4));

    let (tx, rx) = mpsc::channel();
    cluster
        .sim
        .wl()
        .add_recv_filter(4, Box::new(SnapshotAppendFilter::new(tx)));
    pd_client.add_peer(1, new_peer(4, 5));
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

#[test]
fn test_inspected_snapshot() {
    let mut cluster = new_server_cluster(1, 3);
    cluster.cfg.prefer_mem = false;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(8);
    cluster.cfg.raft_store.merge_max_log_gap = 3;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.stop_node(3);
    (0..10).for_each(|_| cluster.must_put(b"k2", b"v2"));
    // Sleep for a while to ensure all logs are compacted.
    sleep_ms(100);

    let stats = cluster
        .io_rate_limiter
        .as_ref()
        .unwrap()
        .statistics()
        .unwrap();
    assert_eq!(stats.fetch(IOType::Replication, IOOp::Read), 0);
    assert_eq!(stats.fetch(IOType::Replication, IOOp::Write), 0);
    // Make sure snapshot read hits disk
    cluster.flush_data();
    // Let store 3 inform leader to generate a snapshot.
    cluster.run_node(3).unwrap();
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    assert_ne!(stats.fetch(IOType::Replication, IOOp::Read), 0);
    assert_ne!(stats.fetch(IOType::Replication, IOOp::Write), 0);

    pd_client.must_remove_peer(1, new_peer(2, 2));
    assert_eq!(stats.fetch(IOType::LoadBalance, IOOp::Read), 0);
    assert_eq!(stats.fetch(IOType::LoadBalance, IOOp::Write), 0);
    pd_client.must_add_peer(1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    assert_ne!(stats.fetch(IOType::LoadBalance, IOOp::Read), 0);
    assert_ne!(stats.fetch(IOType::LoadBalance, IOOp::Write), 0);
}

// Test snapshot generating and receiving can share one I/O limiter fairly.
// 1. Bootstrap a 1 Region, 1 replica cluster;
// 2. Add a peer on store 2 for the Region, so that there is a snapshot received on store 2;
// 3. Rename the received snapshot on store 2, and then keep sending it back to store 1;
// 4. Add another peer for the Region, so store 1 will generate a new snapshot;
// 5. Test the generating can success while the store keeps receiving snapshots from store 2.
#[test]
fn test_gen_during_heavy_recv() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.server.snap_max_write_bytes_per_sec = ReadableSize(5 * 1024 * 1024);
    cluster.cfg.raft_store.snap_mgr_gc_tick_interval = ReadableDuration(Duration::from_secs(100));

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    // 1M random value to ensure the region snapshot is large enough.
    cluster.must_put(b"key-0000", b"value");
    for i in 1..1024 {
        let key = format!("key-{:04}", i).into_bytes();
        cluster.must_put(&key, &random_long_vec(1024));
    }
    // Another 1M random value because the region will split later.
    cluster.must_put(b"zzz-0000", b"value");
    for i in 1..1024 {
        let key = format!("zzz-{:04}", i).into_bytes();
        cluster.must_put(&key, &random_long_vec(1024));
    }

    pd_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"key-0000", b"value");

    // The new region can be used to keep sending snapshots to store 1.
    let r2 = {
        let r = cluster.get_region(b"key");
        cluster.must_split(&r, b"zzz");
        cluster.get_region(b"key").get_id()
    };
    cluster.must_transfer_leader(r2, new_peer(2, 1002));

    let snap_mgr = cluster.get_snap_mgr(2);
    let engine = cluster.engines[&2].kv.clone();
    let snap_term = cluster.raft_local_state(r2, 2).get_hard_state().term;
    let snap_apply_state = cluster.apply_state(r2, 2);
    let mut snap_index = snap_apply_state.applied_index;

    let snap = do_snapshot(
        snap_mgr.clone(),
        &engine,
        engine.snapshot(),
        r2,
        snap_term,
        snap_apply_state,
        true,
        true,
    )
    .unwrap();

    // Keep sending snapshots to store 1.
    let s1_addr = cluster.sim.rl().get_addr(1);
    let sec_mgr = cluster.sim.rl().security_mgr.clone();
    let snap_dir = cluster.sim.rl().get_snap_dir(2);
    let th = std::thread::spawn(move || {
        loop {
            for suffix in &[".meta", "_default.sst"] {
                let f = format!("gen_{}_{}_{}{}", r2, snap_term, snap_index, suffix);
                let mut src = PathBuf::from(&snap_dir);
                src.push(&f);

                let f = format!("gen_{}_{}_{}{}", r2, snap_term, snap_index + 1, suffix);
                let mut dst = PathBuf::from(&snap_dir);
                dst.push(&f);

                fs::hard_link(&src, &dst).unwrap();
            }

            let snap_mgr = snap_mgr.clone();
            let sec_mgr = sec_mgr.clone();
            let s = snap.clone();
            if let Err(e) =
                send_a_large_snapshot(snap_mgr, sec_mgr, &s1_addr, r2, s, snap_index, snap_term)
            {
                info!("send_a_large_snapshot fail: {}", e);
                break;
            }
            snap_index += 1;
        }
    });

    // While store 1 keeps receiving snapshots, it should still can generate a snapshot on time.
    pd_client.must_add_peer(r1, new_learner_peer(3, 3));
    sleep_ms(500);
    must_get_equal(&cluster.get_engine(3), b"zzz-0000", b"value");
    assert_eq!(cluster.get_snap_mgr(1).stats().sending_count, 0);
    assert_eq!(cluster.get_snap_mgr(2).stats().receiving_count, 0);
    drop(cluster);
    let _ = th.join();
}

fn send_a_large_snapshot(
    mgr: SnapManager,
    security_mgr: Arc<SecurityManager>,
    addr: &str,
    region_id: u64,
    mut snap: Snapshot,
    index: u64,
    term: u64,
) -> std::result::Result<(), String> {
    snap.mut_metadata().term = term;
    snap.mut_metadata().index = index;

    // Construct a raft message.
    let mut msg = RaftMessage::default();
    msg.region_id = region_id;
    msg.mut_message().set_snapshot(snap);

    let env = Arc::new(Environment::new(1));
    let cfg = tikv::server::Config::default();
    block_on(async {
        send_snap(env, mgr, security_mgr, &cfg, addr, msg)
            .unwrap()
            .await
            .map(|_| ())
            .map_err(|e| format!("{:?}", e))
    })
}

fn random_long_vec(length: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut value = Vec::with_capacity(1024);
    (0..length).for_each(|_| value.push(rng.gen::<u8>()));
    value
}

/// Snapshot is generated using apply term from apply thread, which should be set
/// correctly otherwise lead to unconsistency.
#[test]
fn test_correct_snapshot_term() {
    // Use five replicas so leader can send a snapshot to a new peer without
    // committing extra logs.
    let mut cluster = new_server_cluster(0, 5);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    // Use run conf change to make new node be initialized with term 0.
    let r = cluster.run_conf_change();

    // 5 will catch up logs with just a snapshot.
    cluster.add_send_filter(IsolationFilterFactory::new(5));
    // 4 will catch up logs using snapshot from 5.
    cluster.add_send_filter(IsolationFilterFactory::new(4));

    pd_client.must_add_peer(r, new_peer(2, 2));
    pd_client.must_add_peer(r, new_peer(3, 3));
    pd_client.must_add_peer(r, new_peer(4, 4));
    pd_client.must_add_peer(r, new_peer(5, 5));
    cluster.must_put(b"k0", b"v0");

    cluster.clear_send_filters();
    // So peer 4 will not apply snapshot from current leader.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 4)
            .msg_type(MessageType::MsgSnapshot)
            .direction(Direction::Recv),
    ));
    // So no new log will be applied. When peer 5 becomes leader, it won't have
    // chance to update apply index in apply worker.
    for i in 1..=3 {
        cluster.add_send_filter(CloneFilterFactory(
            RegionPacketFilter::new(1, i)
                .msg_type(MessageType::MsgAppendResponse)
                .direction(Direction::Send),
        ));
    }
    cluster.must_transfer_leader(1, new_peer(5, 5));
    // Clears send filters so peer 4 can accept snapshot from peer 5. If peer 5
    // didn't set apply index correctly using snapshot in apply worker, the snapshot
    // will be generated as term 0. Raft consider term of missing index as 0, so
    // peer 4 will accept the snapshot and think it has already applied it, hence fast
    // forward it then panic.
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(4), b"k0", b"v0");
    cluster.clear_send_filters();
    cluster.must_put(b"k1", b"v1");
    // If peer 4 panicks, it won't be able to apply new writes.
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");
}

/// Test when applying a snapshot, old logs should be cleaned up.
#[test]
fn test_snapshot_clean_up_logs_with_log_gc() {
    let mut cluster = new_node_cluster(0, 4);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(50);
    cluster.cfg.raft_store.raft_log_gc_threshold = 50;
    // Speed up log gc.
    cluster.cfg.raft_store.raft_log_compact_sync_interval = ReadableDuration::millis(1);
    let pd_client = cluster.pd_client.clone();

    // Disable default max peer number check.
    pd_client.disable_default_operator();
    let r = cluster.run_conf_change();
    pd_client.must_add_peer(r, new_peer(2, 2));
    pd_client.must_add_peer(r, new_peer(3, 3));
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    pd_client.must_add_peer(r, new_peer(4, 4));
    pd_client.must_remove_peer(r, new_peer(3, 3));
    cluster.must_transfer_leader(r, new_peer(4, 4));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");
    cluster.clear_send_filters();
    cluster.add_send_filter(IsolationFilterFactory::new(1));
    // Peer (4, 4) must become leader at the end and send snapshot to 2.
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let raft_engine = cluster.engines[&2].raft.clone();
    let mut dest = vec![];
    raft_engine.get_all_entries_to(1, &mut dest).unwrap();
    // No new log is proposed, so there should be no log at all.
    assert!(dest.is_empty(), "{:?}", dest);
}

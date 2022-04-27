// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use engine_traits::CF_WRITE;
use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::{Mutation, Op, PessimisticLockRequest, PrewriteRequest};
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use raftstore::store::config::Config as RaftstoreConfig;
use raftstore::store::util::is_vote_msg;
use raftstore::store::Callback;
use raftstore::Result;
use tikv_util::HandyRwLock;

use collections::HashMap;
use test_raftstore::*;
use tikv::storage::kv::SnapshotExt;
use tikv::storage::Snapshot;
use tikv_util::config::{ReadableDuration, ReadableSize};
use txn_types::{Key, PessimisticLock};

#[test]
fn test_follower_slow_split() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.run();
    let region = cluster.get_region(b"");

    // Only need peer 1 and 3. Stop node 2 to avoid extra vote messages.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    pd_client.must_remove_peer(1, new_peer(2, 2));
    cluster.stop_node(2);

    // Use a channel to retrieve start_key and end_key in pre-vote messages.
    let (range_tx, range_rx) = mpsc::channel();
    let prevote_filter = PrevoteRangeFilter {
        // Only send 1 pre-vote message to peer 3 so if peer 3 drops it,
        // it needs to start a new election.
        filter: RegionPacketFilter::new(1000, 1) // new region id is 1000
            .msg_type(MessageType::MsgRequestPreVote)
            .direction(Direction::Send)
            .allow(1),
        before: Some(Mutex::new(range_tx)),
        after: None,
    };
    cluster
        .sim
        .wl()
        .add_send_filter(1, Box::new(prevote_filter));

    // Ensure pre-vote response is really sended.
    let (tx, rx) = mpsc::channel();
    let prevote_resp_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx,
        Arc::from(AtomicBool::new(true)),
    ));
    cluster.sim.wl().add_send_filter(3, prevote_resp_notifier);

    // After split, pre-vote message should be sent to peer 2.
    fail::cfg("apply_before_split_1_3", "pause").unwrap();
    cluster.must_split(&region, b"k2");
    let range = range_rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_eq!(range.0, b"");
    assert_eq!(range.1, b"k2");

    // After the follower split success, it will response to the pending vote.
    fail::cfg("apply_before_split_1_3", "off").unwrap();
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_ok());
}

#[test]
fn test_split_lost_request_vote() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.run();
    let region = cluster.get_region(b"");

    // Only need peer 1 and 3. Stop node 2 to avoid extra vote messages.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    pd_client.must_remove_peer(1, new_peer(2, 2));
    cluster.stop_node(2);

    // Use a channel to retrieve start_key and end_key in pre-vote messages.
    let (range_tx, range_rx) = mpsc::channel();
    let (after_sent_tx, after_sent_rx) = mpsc::channel();
    let prevote_filter = PrevoteRangeFilter {
        // Only send 1 pre-vote message to peer 3 so if peer 3 drops it,
        // it needs to start a new election.
        filter: RegionPacketFilter::new(1000, 1) // new region id is 1000
            .msg_type(MessageType::MsgRequestPreVote)
            .direction(Direction::Send)
            .allow(1),
        before: Some(Mutex::new(range_tx)),
        after: Some(Mutex::new(after_sent_tx)),
    };
    cluster
        .sim
        .wl()
        .add_send_filter(1, Box::new(prevote_filter));

    // Ensure pre-vote response is really sent.
    let (tx, rx) = mpsc::channel();
    let prevote_resp_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx,
        Arc::from(AtomicBool::new(true)),
    ));
    cluster.sim.wl().add_send_filter(3, prevote_resp_notifier);

    // After split, pre-vote message should be sent to peer 3.
    fail::cfg("apply_after_split_1_3", "pause").unwrap();
    cluster.must_split(&region, b"k2");
    let range = range_rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_eq!(range.0, b"");
    assert_eq!(range.1, b"k2");

    // Make sure the message has sent to peer 3.
    let _sent = after_sent_rx
        .recv_timeout(Duration::from_millis(100))
        .unwrap();

    // Make sure pre-vote is handled.
    let new_region = cluster.pd_client.get_region(b"").unwrap();
    let pending_create_peer = new_region
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 3)
        .unwrap()
        .to_owned();
    let _ = read_on_peer(
        &mut cluster,
        pending_create_peer,
        region,
        b"k1",
        false,
        Duration::from_millis(100),
    );

    // Make sure pre-vote is cached in pending votes.
    {
        let store_meta = cluster.store_metas.get(&3).unwrap();
        let meta = store_meta.lock().unwrap();
        assert!(meta.pending_msgs.iter().any(|m| {
            m.region_id == new_region.id
                && raftstore::store::util::is_first_message(m.get_message())
        }));
    }

    // After the follower split success, it will response to the pending vote.
    fail::cfg("apply_after_split_1_3", "off").unwrap();
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_ok());
}

fn gen_split_region() -> (Region, Region, Region) {
    let mut cluster = new_server_cluster(0, 2);
    let region_max_size = 50000;
    let region_split_size = 30000;
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.coprocessor.region_max_size = ReadableSize(region_max_size);
    cluster.cfg.coprocessor.region_split_size = ReadableSize(region_split_size);

    let mut range = 1..;
    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"").unwrap();
    let last_key = put_till_size(&mut cluster, region_split_size, &mut range);
    let target = pd_client.get_region(&last_key).unwrap();

    assert_eq!(region, target);

    let max_key = put_cf_till_size(&mut cluster, CF_WRITE, region_max_size, &mut range);

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();
    if left == right {
        cluster.wait_region_split_max_cnt(&region, 20, 10, false);
    }

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();

    (region, left, right)
}

#[test]
fn test_pause_split_when_snap_gen_will_split() {
    let is_generating_snapshot = "is_generating_snapshot";
    fail::cfg(is_generating_snapshot, "return()").unwrap();

    let (region, left, right) = gen_split_region();

    assert_ne!(left, right);
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());

    fail::remove(is_generating_snapshot);
}

#[test]
fn test_pause_split_when_snap_gen_never_split() {
    let is_generating_snapshot = "is_generating_snapshot";
    let region_split_skip_max_count = "region_split_skip_max_count";
    fail::cfg(region_split_skip_max_count, "return()").unwrap();
    fail::cfg(is_generating_snapshot, "return()").unwrap();

    let (region, left, right) = gen_split_region();

    assert_eq!(region, left);
    assert_eq!(left, right);

    fail::remove(is_generating_snapshot);
    fail::remove(region_split_skip_max_count);
}

type FilterSender<T> = Mutex<mpsc::Sender<T>>;

// Filter prevote message and record the range.
struct PrevoteRangeFilter {
    filter: RegionPacketFilter,
    before: Option<FilterSender<(Vec<u8>, Vec<u8>)>>,
    after: Option<FilterSender<()>>,
}

impl Filter for PrevoteRangeFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        self.filter.before(msgs)?;
        if let Some(msg) = msgs.iter().filter(|m| is_vote_msg(m.get_message())).last() {
            let start_key = msg.get_start_key().to_owned();
            let end_key = msg.get_end_key().to_owned();
            if let Some(before) = self.before.as_ref() {
                let tx = before.lock().unwrap();
                let _ = tx.send((start_key, end_key));
            }
        }
        Ok(())
    }
    fn after(&self, _: Result<()>) -> Result<()> {
        if let Some(after) = self.after.as_ref() {
            let tx = after.lock().unwrap();
            let _ = tx.send(());
        }
        Ok(())
    }
}

// Test if a peer is created from splitting when another initialized peer with the same
// region id has already existed. In previous implementation, it can be created and panic
// will happen because there are two initialized peer with the same region id.
#[test]
fn test_split_not_to_split_existing_region() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let mut region_a = pd_client.get_region(b"k1").unwrap();
    // [-∞, k2), [k2, +∞)
    //    b         a
    cluster.must_split(&region_a, b"k2");

    cluster.put(b"k0", b"v0").unwrap();
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    let region_b = pd_client.get_region(b"k0").unwrap();
    let peer_b_1 = find_peer(&region_b, 1).cloned().unwrap();
    cluster.must_transfer_leader(region_b.get_id(), peer_b_1);

    let peer_b_3 = find_peer(&region_b, 3).cloned().unwrap();
    assert_eq!(peer_b_3.get_id(), 1003);
    let on_handle_apply_1003_fp = "on_handle_apply_1003";
    fail::cfg(on_handle_apply_1003_fp, "pause").unwrap();
    // [-∞, k1), [k1, k2), [k2, +∞)
    //    c         b          a
    cluster.must_split(&region_b, b"k1");

    pd_client.must_remove_peer(region_b.get_id(), peer_b_3);
    pd_client.must_add_peer(region_b.get_id(), new_peer(4, 4));

    let mut region_c = pd_client.get_region(b"k0").unwrap();
    let peer_c_3 = find_peer(&region_c, 3).cloned().unwrap();
    pd_client.must_remove_peer(region_c.get_id(), peer_c_3);
    pd_client.must_add_peer(region_c.get_id(), new_peer(4, 5));
    // [-∞, k2), [k2, +∞)
    //     c        a
    pd_client.must_merge(region_b.get_id(), region_c.get_id());

    region_a = pd_client.get_region(b"k2").unwrap();
    let peer_a_3 = find_peer(&region_a, 3).cloned().unwrap();
    pd_client.must_remove_peer(region_a.get_id(), peer_a_3);
    pd_client.must_add_peer(region_a.get_id(), new_peer(4, 6));
    // [-∞, +∞)
    //    c
    pd_client.must_merge(region_a.get_id(), region_c.get_id());

    region_c = pd_client.get_region(b"k1").unwrap();
    // [-∞, k2), [k2, +∞)
    //     d        c
    cluster.must_split(&region_c, b"k2");

    let peer_c_4 = find_peer(&region_c, 4).cloned().unwrap();
    pd_client.must_remove_peer(region_c.get_id(), peer_c_4);
    pd_client.must_add_peer(region_c.get_id(), new_peer(3, 7));

    cluster.put(b"k2", b"v2").unwrap();
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");

    fail::remove(on_handle_apply_1003_fp);

    // If peer_c_3 is created, `must_get_none` will fail.
    must_get_none(&cluster.get_engine(3), b"k0");
}

// Test if a peer is created from splitting when another initialized peer with the same
// region id existed before and has been destroyed now.
#[test]
fn test_split_not_to_split_existing_tombstone_region() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    fail::cfg("on_raft_gc_log_tick", "return()").unwrap();
    let r1 = cluster.run_conf_change();

    pd_client.must_add_peer(r1, new_peer(3, 3));

    assert_eq!(r1, 1);
    let before_check_snapshot_1_2_fp = "before_check_snapshot_1_2";
    fail::cfg(before_check_snapshot_1_2_fp, "pause").unwrap();
    pd_client.must_add_peer(r1, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    cluster.must_put(b"k22", b"v22");

    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let left = pd_client.get_region(b"k1").unwrap();
    let left_peer_2 = find_peer(&left, 2).cloned().unwrap();
    pd_client.must_remove_peer(left.get_id(), left_peer_2);
    must_get_none(&cluster.get_engine(2), b"k1");

    let on_handle_apply_2_fp = "on_handle_apply_2";
    fail::cfg("on_handle_apply_2", "pause").unwrap();

    fail::remove(before_check_snapshot_1_2_fp);

    // Wait for the logs
    sleep_ms(100);

    // If left_peer_2 can be created, dropping all msg to make it exist.
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    // Also don't send check stale msg to PD
    let peer_check_stale_state_fp = "peer_check_stale_state";
    fail::cfg(peer_check_stale_state_fp, "return()").unwrap();

    fail::remove(on_handle_apply_2_fp);

    // If value of `k22` is equal to `v22`, the previous split log must be applied.
    must_get_equal(&cluster.get_engine(2), b"k22", b"v22");

    // If left_peer_2 is created, `must_get_none` will fail.
    must_get_none(&cluster.get_engine(2), b"k1");

    cluster.clear_send_filters();

    pd_client.must_add_peer(left.get_id(), new_peer(2, 4));

    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
}

// TiKV uses memory lock to control the order between spliting and creating
// new peer. This case test if tikv continues split if the peer is destroyed after
// memory lock check.
#[test]
fn test_split_continue_when_destroy_peer_after_mem_check() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    fail::cfg("on_raft_gc_log_tick", "return()").unwrap();
    let r1 = cluster.run_conf_change();

    pd_client.must_add_peer(r1, new_peer(3, 3));

    assert_eq!(r1, 1);
    let before_check_snapshot_1_2_fp = "before_check_snapshot_1_2";
    fail::cfg(before_check_snapshot_1_2_fp, "pause").unwrap();
    let before_check_snapshot_1000_2_fp = "before_check_snapshot_1000_2";
    fail::cfg(before_check_snapshot_1000_2_fp, "pause").unwrap();
    pd_client.must_add_peer(r1, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    cluster.must_put(b"k22", b"v22");

    must_get_none(&cluster.get_engine(2), b"k1");

    let left = pd_client.get_region(b"k1").unwrap();
    let left_peer_2 = find_peer(&left, 2).cloned().unwrap();
    pd_client.must_remove_peer(left.get_id(), left_peer_2);

    // Make sure it finish mem check before destorying.
    let (mem_check_tx, mem_check_rx) = crossbeam::channel::bounded(0);
    let on_handle_apply_split_2_fp = "on_handle_apply_split_2_after_mem_check";
    fail::cfg_callback(on_handle_apply_split_2_fp, move || {
        let _ = mem_check_tx.send(());
        let _ = mem_check_tx.send(());
    })
    .unwrap();

    // So region 1 will start apply snapshot and split.
    fail::remove(before_check_snapshot_1_2_fp);

    // Wait for split mem check
    mem_check_rx.recv_timeout(Duration::from_secs(3)).unwrap();

    let (destroy_tx, destroy_rx) = crossbeam::channel::bounded(0);
    fail::cfg_callback("raft_store_finish_destroy_peer", move || {
        let _ = destroy_tx.send(());
    })
    .unwrap();

    // Resum region 1000 processing and wait till it's destroyed.
    fail::remove(before_check_snapshot_1000_2_fp);
    destroy_rx.recv_timeout(Duration::from_secs(3)).unwrap();

    // If left_peer_2 can be created, dropping all msg to make it exist.
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    // Also don't send check stale msg to PD
    let peer_check_stale_state_fp = "peer_check_stale_state";
    fail::cfg(peer_check_stale_state_fp, "return()").unwrap();

    // Resume split.
    fail::remove(on_handle_apply_split_2_fp);
    mem_check_rx.recv_timeout(Duration::from_secs(3)).unwrap();

    // If value of `k22` is equal to `v22`, the previous split log must be applied.
    must_get_equal(&cluster.get_engine(2), b"k22", b"v22");

    // Once it's marked split in memcheck, destroy should not write tombstone otherwise it will
    // break the region states. Hence split should continue.
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.clear_send_filters();
    fail::remove(peer_check_stale_state_fp);

    must_get_none(&cluster.get_engine(2), b"k1");
}

// Test if a peer can be created from splitting when another uninitialied peer with the same
// peer id has been created on this store.
#[test]
fn test_split_should_split_existing_same_uninitialied_peer() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    fail::cfg("on_raft_gc_log_tick", "return()").unwrap();
    fail::cfg("peer_check_stale_state", "return()").unwrap();

    let r1 = cluster.run_conf_change();

    pd_client.must_add_peer(r1, new_peer(3, 3));

    assert_eq!(r1, 1);

    let before_check_snapshot_1_2_fp = "before_check_snapshot_1_2";
    fail::cfg(before_check_snapshot_1_2_fp, "pause").unwrap();

    pd_client.must_add_peer(r1, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");

    let before_check_snapshot_1000_2_fp = "before_check_snapshot_1000_2";
    fail::cfg(before_check_snapshot_1000_2_fp, "pause").unwrap();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    assert_eq!(left.get_id(), 1000);

    cluster.must_put(b"k11", b"v11");

    // Wait for region 1000 sending heartbeat and snapshot to store 2
    sleep_ms(200);

    fail::remove(before_check_snapshot_1_2_fp);
    // peer 2 applied snapshot
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    fail::remove(before_check_snapshot_1000_2_fp);

    must_get_equal(&cluster.get_engine(2), b"k11", b"v11");
}

// Test if a peer can be created from splitting when another uninitialied peer with different
// peer id has been created on this store.
#[test]
fn test_split_not_to_split_existing_different_uninitialied_peer() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    fail::cfg("on_raft_gc_log_tick", "return()").unwrap();
    let r1 = cluster.run_conf_change();
    assert_eq!(r1, 1);

    cluster.must_put(b"k0", b"v0");
    pd_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    let before_check_snapshot_1_2_fp = "before_check_snapshot_1_2";
    fail::cfg(before_check_snapshot_1_2_fp, "pause").unwrap();

    pd_client.must_add_peer(r1, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");

    // Wait for region 1 sending heartbeat and snapshot to store 2
    sleep_ms(200);

    cluster.add_send_filter(IsolationFilterFactory::new(2));

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    assert_eq!(left.get_id(), 1000);
    let left_peer_2 = find_peer(&left, 2).cloned().unwrap();

    pd_client.must_remove_peer(left.get_id(), left_peer_2);
    pd_client.must_add_peer(left.get_id(), new_peer(2, 4));

    let before_check_snapshot_1000_2_fp = "before_check_snapshot_1000_2";
    fail::cfg(before_check_snapshot_1000_2_fp, "pause").unwrap();

    cluster.clear_send_filters();

    // Wait for region 1000 sending heartbeat and snapshot to store 2
    sleep_ms(200);

    fail::remove(before_check_snapshot_1_2_fp);

    // peer 2 applied snapshot
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    // But only the right part because there is a peer 4 of region 1000 on local store
    must_get_none(&cluster.get_engine(2), b"k1");

    fail::remove(before_check_snapshot_1000_2_fp);
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
}

/// A filter that collects all snapshots.
///
/// It's different from the one in simulate_transport in three aspects:
/// 1. It will not flush the collected snapshots.
/// 2. It will not report error when collecting snapshots.
/// 3. It callers can access the collected snapshots.
pub struct CollectSnapshotFilter {
    pending_msg: Arc<Mutex<HashMap<u64, RaftMessage>>>,
    pending_count_sender: Mutex<mpsc::Sender<usize>>,
}

impl CollectSnapshotFilter {
    pub fn new(sender: mpsc::Sender<usize>) -> CollectSnapshotFilter {
        CollectSnapshotFilter {
            pending_msg: Arc::default(),
            pending_count_sender: Mutex::new(sender),
        }
    }
}

impl Filter for CollectSnapshotFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut to_send = vec![];
        let mut pending_msg = self.pending_msg.lock().unwrap();
        for msg in msgs.drain(..) {
            let (is_pending, from_peer_id) = {
                if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
                    let from_peer_id = msg.get_from_peer().get_id();
                    if pending_msg.contains_key(&from_peer_id) {
                        // Drop this snapshot message directly since it's from a seen peer
                        continue;
                    } else {
                        // Pile the snapshot from unseen peer
                        (true, from_peer_id)
                    }
                } else {
                    (false, 0)
                }
            };
            if is_pending {
                pending_msg.insert(from_peer_id, msg);
                let sender = self.pending_count_sender.lock().unwrap();
                sender.send(pending_msg.len()).unwrap();
            } else {
                to_send.push(msg);
            }
        }
        msgs.extend(to_send);
        check_messages(msgs)?;
        Ok(())
    }
}

/// If the uninitialized peer and split peer are fetched into one batch, and the first
/// one doesn't generate ready, the second one does, ready should not be mapped to the
/// first one.
#[test]
fn test_split_duplicated_batch() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_request_snapshot(&mut cluster);
    // Disable raft log gc in this test case.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);
    // Use one thread to make it more possible to be fetched into one batch.
    cluster.cfg.raft_store.store_batch_system.pool_size = 1;

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

    // Pile up snapshots of overlapped region ranges
    let (tx, rx) = mpsc::channel();
    let filter = CollectSnapshotFilter::new(tx);
    let pending_msgs = filter.pending_msg.clone();
    cluster.sim.wl().add_recv_filter(3, Box::new(filter));
    pd_client.must_add_peer(r1, new_peer(3, 3));
    let region = cluster.get_region(b"k1");
    // Ensure the snapshot of range ("", "") is sent and piled in filter.
    if let Err(e) = rx.recv_timeout(Duration::from_secs(1)) {
        panic!("the snapshot is not sent before split, e: {:?}", e);
    }
    // Split the region range and then there should be another snapshot for the split ranges.
    cluster.must_split(&region, b"k2");
    // Ensure second is also sent and piled in filter.
    if let Err(e) = rx.recv_timeout(Duration::from_secs(1)) {
        panic!("the snapshot is not sent before split, e: {:?}", e);
    }

    let (tx1, rx1) = mpsc::sync_channel(0);
    let tx1 = Mutex::new(tx1);
    fail::cfg_callback("on_split", move || {
        // First is for notification, second is waiting for configuration.
        let _ = tx1.lock().unwrap().send(());
        let _ = tx1.lock().unwrap().send(());
    })
    .unwrap();

    let r2 = cluster.get_region(b"k0");
    let filter_r2 = Arc::new(AtomicBool::new(true));
    // So uninitialized peer will not generate ready for response.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r2.get_id(), 3)
            .when(filter_r2.clone())
            .direction(Direction::Recv),
    ));
    // So peer can catch up logs and execute split
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1, 3)
            .msg_type(MessageType::MsgSnapshot)
            .direction(Direction::Recv),
    ));
    cluster.sim.wl().clear_recv_filters(3);
    // Start applying snapshot in source peer.
    for (peer_id, msg) in pending_msgs.lock().unwrap().iter() {
        if *peer_id < 1000 {
            cluster.sim.wl().send_raft_msg(msg.clone()).unwrap();
        }
    }

    let (tx2, rx2) = mpsc::sync_channel(0);
    // r1 has split.
    rx1.recv_timeout(Duration::from_secs(3)).unwrap();
    // Notify uninitialized peer to be ready be fetched at next try.
    for (peer_id, msg) in pending_msgs.lock().unwrap().iter() {
        if *peer_id >= 1000 {
            cluster.sim.wl().send_raft_msg(msg.clone()).unwrap();
        }
    }
    let tx2 = Mutex::new(tx2);
    fail::cfg_callback("after_split", move || {
        // First is for notification, second is waiting for configuration.
        let _ = tx2.lock().unwrap().send(());
        let _ = tx2.lock().unwrap().send(());
    })
    .unwrap();
    // Resume on_split hook.
    rx1.recv_timeout(Duration::from_secs(3)).unwrap();
    // Pause at the end of on_split.
    rx2.recv_timeout(Duration::from_secs(3)).unwrap();
    // New peer is generated, no need to filter any more.
    filter_r2.store(false, Ordering::SeqCst);
    // Force generating new messages so split peer will be notified and ready to
    // be fetched at next try.
    cluster.must_put(b"k11", b"v11");
    // Exit on_split hook.
    rx2.recv_timeout(Duration::from_secs(3)).unwrap();
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
}

/// We depend on split-check task to update approximate size of region even if this region does not
/// need to split.
#[test]
fn test_report_approximate_size_after_split_check() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store = RaftstoreConfig::default();
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.region_split_check_diff = ReadableSize::kb(64);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(300);
    cluster.run();
    cluster.must_put_cf("write", b"k0", b"k1");
    let region_id = cluster.get_region_id(b"k0");
    let approximate_size = cluster
        .pd_client
        .get_region_approximate_size(region_id)
        .unwrap_or_default();
    let approximate_keys = cluster
        .pd_client
        .get_region_approximate_keys(region_id)
        .unwrap_or_default();
    // It's either 0 for uninialized or 1 for 0.
    assert!(
        approximate_size <= 1 && approximate_keys <= 1,
        "{} {}",
        approximate_size,
        approximate_keys,
    );
    let (tx, rx) = mpsc::channel();
    let tx = Arc::new(Mutex::new(tx));

    fail::cfg_callback("on_split_region_check_tick", move || {
        // notify split region tick
        let _ = tx.lock().unwrap().send(());
        let tx1 = tx.clone();
        fail::cfg_callback("on_approximate_region_size", move || {
            // notify split check finished
            let _ = tx1.lock().unwrap().send(());
            let tx2 = tx1.clone();
            fail::cfg_callback("test_raftstore::pd::region_heartbeat", move || {
                // notify heartbeat region
                let _ = tx2.lock().unwrap().send(());
            })
            .unwrap();
        })
        .unwrap();
    })
    .unwrap();
    let value = vec![1_u8; 8096];
    for i in 0..10 {
        let mut reqs = vec![];
        for j in 0..10 {
            let k = format!("k{}", i * 10 + j);
            reqs.push(new_put_cf_cmd("write", k.as_bytes(), &value));
        }
        cluster.batch_put("k100".as_bytes(), reqs).unwrap();
    }
    rx.recv().unwrap();
    fail::remove("on_split_region_check_tick");
    rx.recv().unwrap();
    fail::remove("on_approximate_region_size");
    rx.recv().unwrap();
    fail::remove("test_raftstore::pd::region_heartbeat");
    let size = cluster
        .pd_client
        .get_region_approximate_size(region_id)
        .unwrap_or_default();
    // The region does not split, but it still refreshes the approximate_size.
    let region_number = cluster.pd_client.get_regions_number();
    assert_eq!(region_number, 1);
    assert!(size > approximate_size);
}

#[test]
fn test_split_with_concurrent_pessimistic_locking() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut mutation = Mutation::default();
    mutation.set_op(Op::PessimisticLock);
    mutation.key = b"key".to_vec();
    let mut req = PessimisticLockRequest::default();
    req.set_context(cluster.get_ctx(b"key"));
    req.set_mutations(vec![mutation].into());
    req.set_start_version(10);
    req.set_for_update_ts(10);
    req.set_primary_lock(b"key".to_vec());

    // 1. Locking happens when split invalidates in-memory pessimistic locks.
    // The pessimistic lock request has to fallback to propose locks. It should find
    // that the epoch has changed.
    fail::cfg("on_split_invalidate_locks", "pause").unwrap();
    cluster.split_region(&cluster.get_region(b"key"), b"a", Callback::None);
    thread::sleep(Duration::from_millis(300));

    let client2 = client.clone();
    let req2 = req.clone();
    let res = thread::spawn(move || client2.kv_pessimistic_lock(&req2).unwrap());
    thread::sleep(Duration::from_millis(200));
    fail::remove("on_split_invalidate_locks");
    let resp = res.join().unwrap();
    assert!(resp.get_region_error().has_epoch_not_match(), "{:?}", resp);

    // 2. Locking happens when split has finished
    // It needs to be rejected due to incorrect epoch, otherwise the lock may be written to the wrong region.
    fail::cfg("txn_before_process_write", "pause").unwrap();
    req.set_context(cluster.get_ctx(b"key"));
    let res = thread::spawn(move || client.kv_pessimistic_lock(&req).unwrap());
    thread::sleep(Duration::from_millis(200));

    cluster.split_region(&cluster.get_region(b"key"), b"b", Callback::None);
    thread::sleep(Duration::from_millis(300));

    fail::remove("txn_before_process_write");
    let resp = res.join().unwrap();
    assert!(resp.get_region_error().has_epoch_not_match(), "{:?}", resp);
}

#[test]
fn test_split_pessimistic_locks_with_concurrent_prewrite() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"a".to_vec());
    mutation.set_value(b"v".to_vec());
    let mut req = PrewriteRequest::default();
    req.set_context(cluster.get_ctx(b"a"));
    req.set_mutations(vec![mutation].into());
    req.set_try_one_pc(true);
    req.set_start_version(20);
    req.set_primary_lock(b"a".to_vec());
    let resp = client.kv_prewrite(&req).unwrap();
    let commit_ts = resp.one_pc_commit_ts;

    let txn_ext = cluster
        .must_get_snapshot_of_region(1)
        .ext()
        .get_txn_ext()
        .unwrap()
        .clone();
    let lock_a = PessimisticLock {
        primary: b"a".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: (commit_ts + 10).into(),
        min_commit_ts: (commit_ts + 10).into(),
    };
    let lock_c = PessimisticLock {
        primary: b"c".to_vec().into_boxed_slice(),
        start_ts: 15.into(),
        ttl: 3000,
        for_update_ts: (commit_ts + 10).into(),
        min_commit_ts: (commit_ts + 10).into(),
    };
    {
        let mut locks = txn_ext.pessimistic_locks.write();
        assert!(
            locks
                .insert(vec![
                    (Key::from_raw(b"a"), lock_a),
                    (Key::from_raw(b"c"), lock_c)
                ])
                .is_ok()
        );
    }

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"a".to_vec());
    mutation.set_value(b"v2".to_vec());
    let mut req = PrewriteRequest::default();
    req.set_context(cluster.get_ctx(b"a"));
    req.set_mutations(vec![mutation].into());
    req.set_is_pessimistic_lock(vec![true]);
    req.set_start_version(10);
    req.set_for_update_ts(commit_ts + 20);
    req.set_primary_lock(b"a".to_vec());

    // First let prewrite get snapshot
    fail::cfg("txn_before_process_write", "pause").unwrap();
    let resp = thread::spawn(move || client.kv_prewrite(&req).unwrap());
    thread::sleep(Duration::from_millis(150));

    // In the meantime, split region.
    fail::cfg("on_split_invalidate_locks", "pause").unwrap();
    cluster.split_region(&cluster.get_region(b"key"), b"a", Callback::None);
    thread::sleep(Duration::from_millis(300));

    // PrewriteResponse should contain an EpochNotMatch instead of PessimisticLockNotFound.
    fail::remove("txn_before_process_write");
    let resp = resp.join().unwrap();
    assert!(resp.get_region_error().has_epoch_not_match(), "{:?}", resp);
}

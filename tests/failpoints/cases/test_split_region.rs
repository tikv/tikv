// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, sync_channel},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use collections::HashMap;
use engine_traits::CF_WRITE;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{
        Mutation, Op, PessimisticLockRequest, PrewriteRequest, PrewriteRequestPessimisticAction::*,
    },
    metapb::Region,
    pdpb::CheckPolicy,
    raft_serverpb::RaftMessage,
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use raftstore::{
    store::{config::Config as RaftstoreConfig, util::is_vote_msg, Callback, PeerMsg},
    Result,
};
use test_raftstore::*;
use tikv::storage::{kv::SnapshotExt, Snapshot};
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    HandyRwLock,
};
use txn_types::{Key, PessimisticLock, TimeStamp};

#[test]
fn test_meta_inconsistency() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.hibernate_regions = false;
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    cluster.must_transfer_leader(region_id, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");

    // Add new peer on node 3, its snapshot apply is paused.
    fail::cfg("before_set_region_on_peer_3", "pause").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));

    // Let only heartbeat msg to pass so a replicate peer could be created on node 3
    // for peer 1003.
    let region_packet_filter_region_1000_peer_1003 =
        RegionPacketFilter::new(1000, 3).skip(MessageType::MsgHeartbeat);
    cluster
        .sim
        .wl()
        .add_recv_filter(3, Box::new(region_packet_filter_region_1000_peer_1003));

    // Trigger a region split to create region 1000 with peer 1001, 1002 and 1003.
    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k5");

    // Scheduler a larger peed id heartbeat msg to trigger peer destroy for peer
    // 1003, pause it before the meta.lock operation so new region insertions by
    // region split could go first.
    // Thus a inconsistency could happen because the destroy is handled
    // by a uninitialized peer but the new initialized region info is inserted into
    // the meta by region split.
    fail::cfg("before_destroy_peer_on_peer_1003", "pause").unwrap();
    let new_region = cluster.get_region(b"k4");
    let mut larger_id_msg = Box::<RaftMessage>::default();
    larger_id_msg.set_region_id(1000);
    larger_id_msg.set_to_peer(new_peer(3, 1113));
    larger_id_msg.set_region_epoch(new_region.get_region_epoch().clone());
    larger_id_msg
        .mut_region_epoch()
        .set_conf_ver(new_region.get_region_epoch().get_conf_ver() + 1);
    larger_id_msg.set_from_peer(new_peer(1, 1001));
    let raft_message = larger_id_msg.mut_message();
    raft_message.set_msg_type(MessageType::MsgHeartbeat);
    raft_message.set_from(1001);
    raft_message.set_to(1113);
    raft_message.set_term(6);
    cluster.sim.wl().send_raft_msg(*larger_id_msg).unwrap();
    thread::sleep(Duration::from_millis(500));

    // Let snapshot apply continue on peer 3 from region 0, then region split would
    // be applied too.
    fail::remove("before_set_region_on_peer_3");
    thread::sleep(Duration::from_millis(2000));

    // Let self destroy continue after the region split is finished.
    fail::remove("before_destroy_peer_on_peer_1003");
    sleep_ms(1000);

    // Clear the network partition nemesis, trigger a new region split, panic would
    // be encountered The thread 'raftstore-3-1::test_message_order_3' panicked
    // at 'meta corrupted: no region for 1000 7A6B35 when creating 1004
    // region_id: 1004 from_peer { id: 1005 store_id: 1 } to_peer { id: 1007
    // store_id: 3 } message { msg_type: MsgRequestPreVote to: 1007 from: 1005
    // term: 6 log_term: 5 index: 5 commit: 5 commit_term: 5 } region_epoch {
    // conf_ver: 3 version: 3 } end_key: 6B32'.
    cluster.sim.wl().clear_recv_filters(3);
    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    cluster.must_put(b"k1", b"v1");
}

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
    rx.recv_timeout(Duration::from_millis(100)).unwrap();
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
    after_sent_rx
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
    rx.recv_timeout(Duration::from_millis(100)).unwrap();
}

fn gen_split_region() -> (Region, Region, Region) {
    let mut cluster = new_server_cluster(0, 2);
    let region_max_size = 50000;
    let region_split_size = 30000;
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.coprocessor.region_max_size = Some(ReadableSize(region_max_size));
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

#[test]
fn test_region_size_after_split() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.region_split_check_diff = Some(ReadableSize(10));
    let region_max_size = 1440;
    let region_split_size = 960;
    cluster.cfg.coprocessor.region_max_size = Some(ReadableSize(region_max_size));
    cluster.cfg.coprocessor.region_split_size = ReadableSize(region_split_size);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let _r = cluster.run_conf_change();

    // insert 20 key value pairs into the cluster.
    // from 000000001 to 000000020
    let mut range = 1..;
    put_till_size(&mut cluster, region_max_size - 100, &mut range);
    sleep_ms(100);
    // disable check split.
    fail::cfg("on_split_region_check_tick", "return").unwrap();
    let max_key = put_till_size(&mut cluster, region_max_size, &mut range);
    // split by use key, split region 1 to region 1 and region 2.
    // region 1: ["000000010",""]
    // region 2: ["","000000010")
    let region = pd_client.get_region(&max_key).unwrap();
    cluster.must_split(&region, b"000000010");
    let size = cluster
        .pd_client
        .get_region_approximate_size(region.get_id())
        .unwrap_or_default();
    assert!(size >= region_max_size - 100, "{}", size);

    let region = pd_client.get_region(b"000000009").unwrap();
    let size1 = cluster
        .pd_client
        .get_region_approximate_size(region.get_id())
        .unwrap_or_default();
    assert_eq!(0, size1, "{}", size1);

    // split region by size check, the region 1 will be split to region 1 and region
    // 3. and the region3 will contains one half region size data.
    let region = pd_client.get_region(&max_key).unwrap();
    pd_client.split_region(region.clone(), CheckPolicy::Scan, vec![]);
    sleep_ms(200);
    let size2 = cluster
        .pd_client
        .get_region_approximate_size(region.get_id())
        .unwrap_or_default();
    assert!(size > size2, "{}:{}", size, size2);
    fail::remove("on_split_region_check_tick");

    let region = pd_client.get_region(b"000000010").unwrap();
    let size3 = cluster
        .pd_client
        .get_region_approximate_size(region.get_id())
        .unwrap_or_default();
    assert!(size3 > 0, "{}", size3);
}

// Test if a peer is created from splitting when another initialized peer with
// the same region id has already existed. In previous implementation, it can be
// created and panic will happen because there are two initialized peer with the
// same region id.
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

// Test if a peer is created from splitting when another initialized peer with
// the same region id existed before and has been destroyed now.
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

#[test]
fn test_stale_peer_handle_snap() {
    test_stale_peer_handle_raft_msg("on_snap_msg_1000_2");
}

#[test]
fn test_stale_peer_handle_vote() {
    test_stale_peer_handle_raft_msg("on_vote_msg_1000_2");
}

#[test]
fn test_stale_peer_handle_append() {
    test_stale_peer_handle_raft_msg("on_append_msg_1000_2");
}

#[test]
fn test_stale_peer_handle_heartbeat() {
    test_stale_peer_handle_raft_msg("on_heartbeat_msg_1000_2");
}

fn test_stale_peer_handle_raft_msg(on_handle_raft_msg_1000_2_fp: &str) {
    // The following diagram represents the final state of the test:
    //
    //                    ┌───────────┐  ┌───────────┐  ┌───────────┐
    //                    │           │  │           │  │           │
    //      Region 1      │ Peer 1    │  │ Peer 2    │  │ Peer 3    │
    //      [k2, +∞)      │           │  │           │  │           │
    // ───────────────────┼───────────┼──┼───────────┼──┼───────────┼──
    //                    │           │  │           │  │           │
    //      Region 1000   │ Peer 1001 │  │ Peer 1003 │  │ Peer 1002 │
    //      (-∞, k2)      │           │  │           │  │           │
    //                    └───────────┘  └───────────┘  └───────────┘
    //                       Store 1        Store 2        Store 3
    //
    // In this test, there is a split operation and Peer 1003 will be created
    // twice (by raft message and by split). The new Peer 1003 will replace the
    // old Peer 1003 and but it will be immediately removed. This test verifies
    // that TiKV would not panic if the old Peer 1003 continues to process a
    // remaining raft message (which may be a snapshot/vote/heartbeat/append
    // message).

    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    fail::cfg("on_raft_gc_log_tick", "return()").unwrap();
    let r1 = cluster.run_conf_change();
    // Add Peer 3
    pd_client.must_add_peer(r1, new_peer(3, 3));
    assert_eq!(r1, 1);

    // Pause the snapshot apply of Peer 2.
    let before_check_snapshot_1_2_fp = "before_check_snapshot_1_2";
    fail::cfg(before_check_snapshot_1_2_fp, "pause").unwrap();

    // Add Peer 2. The peer will be created but stuck at applying snapshot due
    // to the failpoint above.
    pd_client.must_add_peer(r1, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");

    // Before the split, pause Peer 1003 when processing a certain raft message.
    // The message type depends on the failpoint name input.
    fail::cfg(on_handle_raft_msg_1000_2_fp, "pause").unwrap();

    // Split the region into Region 1 and Region 1000. Peer 1003 will be created
    // for the first time when it receives a raft message from Peer 1001, but it
    // will remain uninitialized because it's paused due to the failpoint above.
    let region = pd_client.get_region(b"k1").unwrap();

    cluster.must_split(&region, b"k2");
    cluster.must_put(b"k22", b"v22");

    // Check that Store 2 doesn't have any data yet.
    must_get_none(&cluster.get_engine(2), b"k1");
    must_get_none(&cluster.get_engine(2), b"k22");

    // Unblock Peer 2. It will proceed to apply the split operation, which
    // creates Peer 1003 for the second time and replaces the old Peer 1003.
    fail::remove(before_check_snapshot_1_2_fp);

    // Verify that data can be accessed from Peer 2 and the new Peer 1003.
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k22", b"v22");

    // Immediately remove the new Peer 1003. This removes the region metadata.
    let left = pd_client.get_region(b"k1").unwrap();
    let left_peer_2 = find_peer(&left, 2).cloned().unwrap();
    pd_client.must_remove_peer(left.get_id(), left_peer_2);
    must_get_none(&cluster.get_engine(2), b"k1");
    must_get_equal(&cluster.get_engine(2), b"k22", b"v22");

    // Unblock the old Peer 1003 so that it can continue to process its raft
    // message. It would lead to a panic when it processes a snapshot message if
    // #17469 is not fixed.
    fail::remove(on_handle_raft_msg_1000_2_fp);

    // Waiting for the stale peer to handle its raft message.
    sleep_ms(300);

    must_get_none(&cluster.get_engine(2), b"k1");
    must_get_equal(&cluster.get_engine(2), b"k22", b"v22");
}

// TiKV uses memory lock to control the order between spliting and creating
// new peer. This case test if tikv continues split if the peer is destroyed
// after memory lock check.
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

    // Once it's marked split in memcheck, destroy should not write tombstone
    // otherwise it will break the region states. Hence split should continue.
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.clear_send_filters();
    fail::remove(peer_check_stale_state_fp);

    must_get_none(&cluster.get_engine(2), b"k1");
}

// Test if a peer can be created from splitting when another uninitialied peer
// with the same peer id has been created on this store.
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

// Test if a peer can be created from splitting when another uninitialied peer
// with different peer id has been created on this store.
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
    // But only the right part because there is a peer 4 of region 1000 on local
    // store
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

/// If the uninitialized peer and split peer are fetched into one batch, and the
/// first one doesn't generate ready, the second one does, ready should not be
/// mapped to the first one.
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
    // Split the region range and then there should be another snapshot for the
    // split ranges.
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

/// We depend on split-check task to update approximate size of region even if
/// this region does not need to split.
#[test]
fn test_report_approximate_size_after_split_check() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store = RaftstoreConfig::default();
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.region_split_check_diff = Some(ReadableSize::kb(64));
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
    // It needs to be rejected due to incorrect epoch, otherwise the lock may be
    // written to the wrong region.
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
        last_change_ts: 5.into(),
        versions_to_last_change: 3,
    };
    let lock_c = PessimisticLock {
        primary: b"c".to_vec().into_boxed_slice(),
        start_ts: 15.into(),
        ttl: 3000,
        for_update_ts: (commit_ts + 10).into(),
        min_commit_ts: (commit_ts + 10).into(),
        last_change_ts: 5.into(),
        versions_to_last_change: 3,
    };
    {
        let mut locks = txn_ext.pessimistic_locks.write();
        locks
            .insert(vec![
                (Key::from_raw(b"a"), lock_a),
                (Key::from_raw(b"c"), lock_c),
            ])
            .unwrap();
    }

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"a".to_vec());
    mutation.set_value(b"v2".to_vec());
    let mut req = PrewriteRequest::default();
    req.set_context(cluster.get_ctx(b"a"));
    req.set_mutations(vec![mutation].into());
    req.set_pessimistic_actions(vec![DoPessimisticCheck]);
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

    // PrewriteResponse should contain an EpochNotMatch instead of
    // PessimisticLockNotFound.
    fail::remove("txn_before_process_write");
    let resp = resp.join().unwrap();
    assert!(resp.get_region_error().has_epoch_not_match(), "{:?}", resp);

    fail::remove("on_split_invalidate_locks");
}

/// Logs are gced asynchronously. If an uninitialized peer is destroyed before
/// being replaced by split, then the asynchronous log gc response may arrive
/// after the peer is replaced, hence it will lead to incorrect memory state.
/// Actually, there is nothing to be gc for uninitialized peer. The case is to
/// guarantee such incorrect state will not happen.
#[test]
fn test_split_replace_skip_log_gc() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(15);
    cluster.cfg.raft_store.raft_log_gc_threshold = 15;
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    let pd_client = cluster.pd_client.clone();

    // Disable default max peer number check.
    pd_client.disable_default_operator();
    let r = cluster.run_conf_change();
    pd_client.must_add_peer(r, new_peer(3, 3));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let before_check_snapshot_1_2_fp = "before_check_snapshot_1_2";
    fail::cfg(before_check_snapshot_1_2_fp, "pause").unwrap();

    // So the split peer on store 2 always uninitialized.
    let filter = RegionPacketFilter::new(1000, 2).msg_type(MessageType::MsgSnapshot);
    cluster.add_send_filter(CloneFilterFactory(filter));

    pd_client.must_add_peer(r, new_peer(2, 2));
    let region = pd_client.get_region(b"k1").unwrap();
    // [-∞, k2), [k2, +∞)
    //    b         a
    cluster.must_split(&region, b"k2");

    cluster.must_put(b"k3", b"v3");

    // Because a is not initialized, so b must be created using heartbeat on store
    // 3.

    // Simulate raft log gc stall.
    let gc_fp = "worker_gc_raft_log_flush";
    let destroy_fp = "destroy_peer_after_pending_move";

    fail::cfg(gc_fp, "pause").unwrap();
    let (tx, rx) = crossbeam::channel::bounded(0);
    fail::cfg_callback(destroy_fp, move || {
        let _ = tx.send(());
        let _ = tx.send(());
    })
    .unwrap();

    let left = pd_client.get_region(b"k1").unwrap();
    let left_peer_on_store_2 = find_peer(&left, 2).unwrap();
    pd_client.must_remove_peer(left.get_id(), left_peer_on_store_2.clone());
    // Wait till destroy is triggered.
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    // Make it split.
    fail::remove(before_check_snapshot_1_2_fp);
    // Wait till split is finished.
    must_get_equal(&cluster.get_engine(2), b"k3", b"v3");
    // Wait a little bit so the uninitialized peer is replaced.
    thread::sleep(Duration::from_millis(10));
    // Resume destroy.
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    // Resume gc.
    fail::remove(gc_fp);
    // Check store 3 is still working correctly.
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(2), b"k4", b"v4");
}

#[test]
fn test_split_store_channel_full() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.notify_capacity = 10;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.messages_per_tick = 1;
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    cluster.run();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    let region = pd_client.get_region(b"k2").unwrap();
    let apply_fp = "before_nofity_apply_res";
    fail::cfg(apply_fp, "pause").unwrap();
    let (tx, rx) = mpsc::channel();
    cluster.split_region(
        &region,
        b"k2",
        Callback::write(Box::new(move |_| tx.send(()).unwrap())),
    );
    rx.recv().unwrap();
    let sender_fp = "loose_bounded_sender_check_interval";
    fail::cfg(sender_fp, "return").unwrap();
    let store_fp = "begin_raft_poller";
    fail::cfg(store_fp, "pause").unwrap();
    let raft_router = cluster.sim.read().unwrap().get_router(1).unwrap();
    for _ in 0..50 {
        raft_router.force_send(1, PeerMsg::Noop).unwrap();
    }
    fail::remove(apply_fp);
    fail::remove(store_fp);
    sleep_ms(300);
    let region = pd_client.get_region(b"k1").unwrap();
    assert_ne!(region.id, 1);
    fail::remove(sender_fp);
}

#[test]
fn test_split_region_with_no_valid_split_keys() {
    let mut cluster = test_raftstore::new_node_cluster(0, 3);
    cluster.cfg.coprocessor.region_split_size = ReadableSize::kb(1);
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(500);
    cluster.run();

    let (tx, rx) = sync_channel(5);
    fail::cfg_callback("on_compact_range_cf", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    let safe_point_inject = "safe_point_inject";
    fail::cfg(safe_point_inject, "return(100)").unwrap();

    let mut raw_key = String::new();
    let _ = (0..250)
        .map(|i: u8| {
            raw_key.push(i as char);
        })
        .collect::<Vec<_>>();
    for i in 0..20 {
        let key = Key::from_raw(raw_key.as_bytes());
        let key = key.append_ts(TimeStamp::new(i));
        cluster.must_put_cf(CF_WRITE, key.as_encoded(), b"val");
    }

    // one for default cf, one for write cf
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    for i in 0..20 {
        let key = Key::from_raw(raw_key.as_bytes());
        let key = key.append_ts(TimeStamp::new(i));
        cluster.must_put_cf(CF_WRITE, key.as_encoded(), b"val");
    }
    // at most one compaction will be triggered for each safe_point
    rx.try_recv().unwrap_err();

    fail::cfg(safe_point_inject, "return(200)").unwrap();
    for i in 0..20 {
        let key = Key::from_raw(raw_key.as_bytes());
        let key = key.append_ts(TimeStamp::new(i));
        cluster.must_put_cf(CF_WRITE, key.as_encoded(), b"val");
    }
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.try_recv().unwrap_err();
}

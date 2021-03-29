// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::*;
use std::thread;
use std::time::*;

use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::*;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::{PeerState, RaftMessage, RegionLocalState};
use kvproto::tikvpb::TikvClient;
use raft::eraftpb::MessageType;

use engine_rocks::Compat;
use engine_traits::{Peekable, CF_RAFT};
use pd_client::PdClient;
use raftstore::store::*;
use test_raftstore::*;
use tikv_util::config::*;
use tikv_util::HandyRwLock;

/// Test if merge is rollback as expected.
#[test]
fn test_node_merge_rollback() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    pd_client.must_add_peer(left.get_id(), new_peer(2, 2));
    pd_client.must_add_peer(right.get_id(), new_peer(2, 4));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    let target_region = pd_client.get_region(b"k3").unwrap();

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    // The call is finished when prepare_merge is applied.
    cluster.must_try_merge(region.get_id(), target_region.get_id());

    // Add a peer to trigger rollback.
    pd_client.must_add_peer(right.get_id(), new_peer(3, 5));
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");

    let mut region = pd_client.get_region(b"k1").unwrap();
    // After split and prepare_merge, version becomes 1 + 2 = 3;
    assert_eq!(region.get_region_epoch().get_version(), 3);
    // After ConfChange and prepare_merge, conf version becomes 1 + 2 = 3;
    assert_eq!(region.get_region_epoch().get_conf_ver(), 3);
    fail::remove(schedule_merge_fp);
    // Wait till rollback.
    cluster.must_put(b"k11", b"v11");

    // After rollback, version becomes 3 + 1 = 4;
    region.mut_region_epoch().set_version(4);
    for i in 1..3 {
        must_get_equal(&cluster.get_engine(i), b"k11", b"v11");
        let state_key = keys::region_state_key(region.get_id());
        let state: RegionLocalState = cluster
            .get_engine(i)
            .c()
            .get_msg_cf(CF_RAFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Normal);
        assert_eq!(*state.get_region(), region);
    }

    pd_client.must_remove_peer(right.get_id(), new_peer(3, 5));
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    let target_region = pd_client.get_region(b"k3").unwrap();
    cluster.must_try_merge(region.get_id(), target_region.get_id());
    let mut region = pd_client.get_region(b"k1").unwrap();

    // Split to trigger rollback.
    cluster.must_split(&right, b"k3");
    fail::remove(schedule_merge_fp);
    // Wait till rollback.
    cluster.must_put(b"k12", b"v12");

    // After premerge and rollback, conf_ver becomes 3 + 1 = 4, version becomes 4 + 2 = 6;
    region.mut_region_epoch().set_conf_ver(4);
    region.mut_region_epoch().set_version(6);
    for i in 1..3 {
        must_get_equal(&cluster.get_engine(i), b"k12", b"v12");
        let state_key = keys::region_state_key(region.get_id());
        let state: RegionLocalState = cluster
            .get_engine(i)
            .c()
            .get_msg_cf(CF_RAFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Normal);
        assert_eq!(*state.get_region(), region);
    }
}

/// Test if merge is still working when restart a cluster during merge.
#[test]
fn test_node_merge_restart() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.run();

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    cluster.must_try_merge(left.get_id(), right.get_id());
    let leader = cluster.leader_of_region(left.get_id()).unwrap();

    cluster.shutdown();
    let engine = cluster.get_engine(leader.get_store_id());
    let state_key = keys::region_state_key(left.get_id());
    let state: RegionLocalState = engine.c().get_msg_cf(CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Merging, "{:?}", state);
    let state_key = keys::region_state_key(right.get_id());
    let state: RegionLocalState = engine.c().get_msg_cf(CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Normal, "{:?}", state);
    fail::remove(schedule_merge_fp);
    cluster.start().unwrap();

    // Wait till merge is finished.
    pd_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    cluster.must_put(b"k4", b"v4");

    for i in 1..4 {
        must_get_equal(&cluster.get_engine(i), b"k4", b"v4");
        let state_key = keys::region_state_key(left.get_id());
        let state: RegionLocalState = cluster
            .get_engine(i)
            .c()
            .get_msg_cf(CF_RAFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Tombstone, "{:?}", state);
        let state_key = keys::region_state_key(right.get_id());
        let state: RegionLocalState = cluster
            .get_engine(i)
            .c()
            .get_msg_cf(CF_RAFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Normal, "{:?}", state);
        assert!(state.get_region().get_start_key().is_empty());
        assert!(state.get_region().get_end_key().is_empty());
    }

    // Now test if cluster works fine when it crash after merge is applied
    // but before notifying raftstore thread.
    let region = pd_client.get_region(b"k1").unwrap();
    let peer_on_store1 = find_peer(&region, 1).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    let peer_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), peer_on_store1);
    cluster.must_put(b"k11", b"v11");
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    let skip_destroy_fp = "raft_store_skip_destroy_peer";
    fail::cfg(skip_destroy_fp, "return()").unwrap();
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    pd_client.must_merge(left.get_id(), right.get_id());
    let peer = find_peer(&right, 3).unwrap().to_owned();
    pd_client.must_remove_peer(right.get_id(), peer);
    cluster.shutdown();
    fail::remove(skip_destroy_fp);
    cluster.clear_send_filters();
    cluster.start().unwrap();
    must_get_none(&cluster.get_engine(3), b"k1");
    must_get_none(&cluster.get_engine(3), b"k3");
}

/// Test if merge is still working when restart a cluster during catching up logs for merge.
#[test]
fn test_node_merge_catch_up_logs_restart() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    let peer_on_store1 = find_peer(&region, 1).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    // make sure the peer of left region on engine 3 has caught up logs.
    cluster.must_put(b"k0", b"v0");
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));
    cluster.must_put(b"k11", b"v11");
    must_get_none(&cluster.get_engine(3), b"k11");

    // after source peer is applied but before set it to tombstone
    fail::cfg("after_handle_catch_up_logs_for_merge_1003", "return()").unwrap();
    pd_client.must_merge(left.get_id(), right.get_id());
    thread::sleep(Duration::from_millis(100));
    cluster.shutdown();

    fail::remove("after_handle_catch_up_logs_for_merge_1003");
    cluster.start().unwrap();
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
}

/// Test if leader election is working properly when catching up logs for merge.
#[test]
fn test_node_merge_catch_up_logs_leader_election() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 25;
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 12;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    let peer_on_store1 = find_peer(&region, 1).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    let state1 = cluster.truncated_state(1000, 1);
    // let the entries committed but not applied
    fail::cfg("on_handle_apply_1003", "pause").unwrap();
    for i in 2..20 {
        cluster.must_put(format!("k1{}", i).as_bytes(), b"v");
    }

    // wait to trigger compact raft log
    cluster.wait_log_truncated(1000, 1, state1.get_index() + 1);

    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));
    cluster.must_put(b"k11", b"v11");
    must_get_none(&cluster.get_engine(3), b"k11");

    // let peer not destroyed before election timeout
    fail::cfg("before_peer_destroy_1003", "pause").unwrap();
    fail::remove("on_handle_apply_1003");
    pd_client.must_merge(left.get_id(), right.get_id());

    // wait election timeout
    thread::sleep(Duration::from_millis(500));
    fail::remove("before_peer_destroy_1003");

    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
}

// Test if merge is working properly if no need to catch up logs,
// also there may be a propose of compact log after prepare merge is proposed.
#[test]
fn test_node_merge_catch_up_logs_no_need() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 25;
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 12;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    let peer_on_store1 = find_peer(&region, 1).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    // put some keys to trigger compact raft log
    for i in 2..20 {
        cluster.must_put(format!("k1{}", i).as_bytes(), b"v");
    }

    // let the peer of left region on store 3 falls behind.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // make sure the peer is isolated.
    cluster.must_put(b"k11", b"v11");
    must_get_none(&cluster.get_engine(3), b"k11");

    // propose merge but not let apply index make progress.
    fail::cfg("apply_after_prepare_merge", "pause").unwrap();
    pd_client.merge_region(left.get_id(), right.get_id());
    must_get_none(&cluster.get_engine(3), b"k11");

    // wait to trigger compact raft log
    thread::sleep(Duration::from_millis(100));

    // let source region not merged
    fail::cfg("before_handle_catch_up_logs_for_merge", "pause").unwrap();
    fail::cfg("after_handle_catch_up_logs_for_merge", "pause").unwrap();
    // due to `before_handle_catch_up_logs_for_merge` failpoint, we already pass `apply_index < catch_up_logs.merge.get_commit()`
    // so now can let apply index make progress.
    fail::remove("apply_after_prepare_merge");

    // make sure all the logs are committed, including the compact command
    cluster.clear_send_filters();
    thread::sleep(Duration::from_millis(50));

    // let merge process continue
    fail::remove("before_handle_catch_up_logs_for_merge");
    fail::remove("after_handle_catch_up_logs_for_merge");
    thread::sleep(Duration::from_millis(50));

    // the source region should be merged and the peer should be destroyed.
    assert!(pd_client.check_merged(left.get_id()));
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    cluster.must_region_not_exist(left.get_id(), 3);
}

/// Test if merging state will be removed after accepting a snapshot.
#[test]
fn test_node_merge_recover_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 12;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    // Start the cluster and evict the region leader from peer 3.
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k3").unwrap();
    let target_region = pd_client.get_region(b"k1").unwrap();

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    cluster.must_try_merge(region.get_id(), target_region.get_id());

    // Remove a peer to trigger rollback.
    pd_client.must_remove_peer(left.get_id(), left.get_peers()[0].to_owned());
    must_get_none(&cluster.get_engine(3), b"k4");

    let step_store_3_region_1 = "step_message_3_1";
    fail::cfg(step_store_3_region_1, "return()").unwrap();
    fail::remove(schedule_merge_fp);

    for i in 0..100 {
        cluster.must_put(format!("k4{}", i).as_bytes(), b"v4");
    }
    fail::remove(step_store_3_region_1);
    must_get_equal(&cluster.get_engine(3), b"k40", b"v4");
    cluster.must_transfer_leader(1, new_peer(3, 3));
    cluster.must_put(b"k40", b"v5");
}

// Test if a merge handled properly when there are two different snapshots of one region arrive
// in one raftstore tick.
#[test]
fn test_node_merge_multiple_snapshots_together() {
    test_node_merge_multiple_snapshots(true)
}

// Test if a merge handled properly when there are two different snapshots of one region arrive
// in different raftstore tick.
#[test]
fn test_node_merge_multiple_snapshots_not_together() {
    test_node_merge_multiple_snapshots(false)
}

fn test_node_merge_multiple_snapshots(together: bool) {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    // make it gc quickly to trigger snapshot easily
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 10;
    cluster.cfg.raft_store.merge_max_log_gap = 9;
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    let target_leader = right
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 1)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(right.get_id(), target_leader);
    let target_leader = left
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 2)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(left.get_id(), target_leader);
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");

    // So cluster becomes:
    //  left region: 1         2(leader) I 3
    // right region: 1(leader) 2         I 3
    // I means isolation.(here just means 3 can not receive append log)
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(right.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Add a collect snapshot filter, it will delay snapshots until have collected multiple snapshots from different peers
    cluster.sim.wl().add_recv_filter(
        3,
        Box::new(LeadingDuplicatedSnapshotFilter::new(
            Arc::new(AtomicBool::new(false)),
            together,
        )),
    );
    // Write some data to trigger a snapshot of right region.
    for i in 200..210 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }
    // Wait for snapshot to generate and send
    thread::sleep(Duration::from_millis(100));

    // Merge left and right region, due to isolation, the regions on store 3 are not merged yet.
    pd_client.must_merge(left.get_id(), right.get_id());
    thread::sleep(Duration::from_millis(200));

    // Let peer of right region on store 3 to make append response to trigger a new snapshot
    // one is snapshot before merge, the other is snapshot after merge.
    // Here blocks raftstore for a while to make it not to apply snapshot and receive new log now.
    fail::cfg("on_raft_ready", "sleep(100)").unwrap();
    cluster.clear_send_filters();
    thread::sleep(Duration::from_millis(200));
    // Filter message again to make sure peer on store 3 can not catch up CommitMerge log
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(right.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));
    // Cause filter is added again, no need to block raftstore anymore
    fail::cfg("on_raft_ready", "off").unwrap();

    // Wait some time to let already merged peer on store 1 or store 2 to notify
    // the peer of left region on store 3 is stale.
    thread::sleep(Duration::from_millis(300));

    cluster.must_put(b"k9", b"v9");
    // let follower can reach the new log, then commit merge
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(3), b"k9", b"v9");
}

fn prepare_request_snapshot_cluster() -> (Cluster<NodeCluster>, Region, Region) {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k3").unwrap();
    let target_region = pd_client.get_region(b"k1").unwrap();

    // Make sure peer 1 is the leader.
    cluster.must_transfer_leader(region.get_id(), new_peer(1, 1));

    (cluster, region, target_region)
}

// Test if request snapshot is rejected during merging.
#[test]
fn test_node_merge_reject_request_snapshot() {
    let (mut cluster, region, target_region) = prepare_request_snapshot_cluster();

    let k = b"k3_for_apply_to_current_term";
    cluster.must_put(k, b"value");
    for i in 1..=3 {
        must_get_equal(&cluster.get_engine(i), k, b"value");
    }

    let apply_prepare_merge_fp = "apply_before_prepare_merge";
    fail::cfg(apply_prepare_merge_fp, "pause").unwrap();
    let prepare_merge = new_prepare_merge(target_region);
    let mut req = new_admin_request(region.get_id(), region.get_region_epoch(), prepare_merge);
    req.mut_header().set_peer(new_peer(1, 1));
    let (tx, rx) = mpsc::sync_channel(1);
    cluster
        .sim
        .rl()
        .async_command_on_node(
            1,
            req,
            Callback::Write {
                cb: Box::new(|_: WriteResponse| {}),
                proposed_cb: Some(Box::new(move || tx.send(()).unwrap())),
                committed_cb: None,
            },
        )
        .unwrap();
    // Proposing merge shouldn't fail.
    assert!(rx.recv_timeout(Duration::from_millis(200)).is_ok());

    // Install snapshot filter before requesting snapshot.
    let (tx, rx) = mpsc::channel();
    let notifier = Mutex::new(Some(tx));
    cluster.sim.wl().add_recv_filter(
        2,
        Box::new(RecvSnapshotFilter {
            notifier,
            region_id: region.get_id(),
        }),
    );
    cluster.must_request_snapshot(2, region.get_id());
    // Leader should reject request snapshot while merging.
    rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

    // Transfer leader to peer 3, new leader should reject request snapshot too.
    cluster.must_transfer_leader(region.get_id(), new_peer(3, 3));
    rx.recv_timeout(Duration::from_millis(500)).unwrap_err();
    fail::remove(apply_prepare_merge_fp);
}

// Test if merge is rejected during requesting snapshot.
#[test]
fn test_node_request_snapshot_reject_merge() {
    let (cluster, region, target_region) = prepare_request_snapshot_cluster();

    // Pause generating snapshot.
    let region_gen_snap_fp = "region_gen_snap";
    fail::cfg(region_gen_snap_fp, "pause").unwrap();

    // Install snapshot filter before requesting snapshot.
    let (tx, rx) = mpsc::channel();
    let notifier = Mutex::new(Some(tx));
    cluster.sim.wl().add_recv_filter(
        2,
        Box::new(RecvSnapshotFilter {
            notifier,
            region_id: region.get_id(),
        }),
    );
    cluster.must_request_snapshot(2, region.get_id());
    // Leader can not generate a snapshot.
    rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

    let prepare_merge = new_prepare_merge(target_region.clone());
    let mut req = new_admin_request(region.get_id(), region.get_region_epoch(), prepare_merge);
    req.mut_header().set_peer(new_peer(1, 1));
    cluster
        .sim
        .rl()
        .async_command_on_node(1, req, Callback::None)
        .unwrap();
    sleep_ms(200);

    // Merge will never happen.
    let target = cluster.get_region(target_region.get_start_key());
    assert_eq!(target, target_region);
    fail::remove(region_gen_snap_fp);
    // Drop cluster to ensure notifier will not send any message after rx is dropped.
    drop(cluster);
}

// Test if compact log is ignored after premerge was applied and restart
// I.e. is_merging flag should be set after restart
#[test]
fn test_node_merge_restart_after_apply_premerge_before_apply_compact_log() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.merge_max_log_gap = 10;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 11;
    // Rely on this config to trigger a compact log
    cluster.cfg.raft_store.raft_log_gc_size_limit = ReadableSize(1);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();
    // Prevent gc_log_tick to propose a compact log
    let raft_gc_log_tick_fp = "on_raft_gc_log_tick";
    fail::cfg(raft_gc_log_tick_fp, "return()").unwrap();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    let left_peer_1 = find_peer(&left, 1).cloned().unwrap();
    cluster.must_transfer_leader(left.get_id(), left_peer_1);

    // Make log gap between store 1 and store 3, for min_index in preMerge
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    for i in 0..6 {
        cluster.must_put(format!("k1{}", i).as_bytes(), b"v1");
    }
    // Prevent on_apply_res to update merge_state in Peer
    // If not, almost everything cannot propose including compact log
    let on_apply_res_fp = "on_apply_res";
    fail::cfg(on_apply_res_fp, "return()").unwrap();

    cluster.must_try_merge(left.get_id(), right.get_id());

    cluster.clear_send_filters();
    // Prevent apply fsm to apply compact log
    let handle_apply_fp = "on_handle_apply";
    fail::cfg(handle_apply_fp, "return()").unwrap();
    fail::remove(raft_gc_log_tick_fp);

    // Wait for compact log to be proposed and committed maybe
    sleep_ms(30);

    cluster.shutdown();

    fail::remove(handle_apply_fp);
    fail::remove(on_apply_res_fp);
    // Prevent sched_merge_tick to propose CommitMerge
    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    cluster.start().unwrap();

    let last_index = cluster.raft_local_state(left.get_id(), 1).get_last_index();
    // Wait for compact log to apply
    let timer = Instant::now();
    loop {
        let apply_index = cluster.apply_state(left.get_id(), 1).get_applied_index();
        if apply_index >= last_index {
            break;
        }
        if timer.elapsed() > Duration::from_secs(3) {
            panic!("logs are not applied after 3 seconds");
        }
        thread::sleep(Duration::from_millis(20));
    }
    // Now schedule merge
    fail::remove(schedule_merge_fp);

    pd_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    cluster.must_put(b"k123", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k123", b"v2");
}

/// Tests whether stale merge is rollback properly if it merges to the same target region again later.
#[test]
fn test_node_failed_merge_before_succeed_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.merge_max_log_gap = 30;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = 1;
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k5");

    let left = pd_client.get_region(b"k1").unwrap();
    let mut right = pd_client.get_region(b"k5").unwrap();
    let left_peer_1 = find_peer(&left, 1).cloned().unwrap();
    cluster.must_transfer_leader(left.get_id(), left_peer_1);

    let left_peer_3 = find_peer(&left, 3).cloned().unwrap();
    assert_eq!(left_peer_3.get_id(), 1003);

    // Prevent sched_merge_tick to propose CommitMerge
    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return").unwrap();

    // To minimize peers log gap for merging
    cluster.must_put(b"k11", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k11", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k11", b"v2");
    // Make peer 1003 can't receive PrepareMerge and RollbackMerge log
    cluster.add_send_filter(IsolationFilterFactory::new(3));

    cluster.must_try_merge(left.get_id(), right.get_id());

    // Change right region's epoch to make this merge failed
    cluster.must_split(&right, b"k8");
    fail::remove(schedule_merge_fp);
    // Wait for left region to rollback merge
    cluster.must_put(b"k12", b"v2");
    // Prevent apply fsm applying the `PrepareMerge` and `RollbackMerge` log after
    // cleaning send filter.
    let before_handle_normal_1003_fp = "before_handle_normal_1003";
    fail::cfg(before_handle_normal_1003_fp, "return").unwrap();
    cluster.clear_send_filters();

    right = pd_client.get_region(b"k5").unwrap();
    let right_peer_1 = find_peer(&right, 1).cloned().unwrap();
    cluster.must_transfer_leader(right.get_id(), right_peer_1);
    // Add some data for checking data integrity check at a later time
    for i in 0..5 {
        cluster.must_put(format!("k2{}", i).as_bytes(), b"v3");
    }
    // Do a really succeed merge
    pd_client.must_merge(left.get_id(), right.get_id());
    // Wait right region to send CatchUpLogs to left region.
    sleep_ms(100);
    // After executing CatchUpLogs in source peer fsm, the committed log will send
    // to apply fsm in the end of this batch. So even the first `on_ready_prepare_merge`
    // is executed after CatchUplogs, the latter committed logs is still sent to apply fsm
    // if CatchUpLogs and `on_ready_prepare_merge` is in different batch.
    //
    // In this case, the data is complete because the wrong up-to-date msg from the
    // first `on_ready_prepare_merge` is sent after all committed log.
    // Sleep a while to wait apply fsm to send `on_ready_prepare_merge` to peer fsm.
    let after_send_to_apply_1003_fp = "after_send_to_apply_1003";
    fail::cfg(after_send_to_apply_1003_fp, "sleep(300)").unwrap();

    fail::remove(before_handle_normal_1003_fp);
    // Wait `after_send_to_apply_1003` timeout
    sleep_ms(300);
    fail::remove(after_send_to_apply_1003_fp);
    // Check the data integrity
    for i in 0..5 {
        must_get_equal(&cluster.get_engine(3), format!("k2{}", i).as_bytes(), b"v3");
    }
}

/// Tests whether the source peer is destroyed correctly when transferring leader during committing merge.
///
/// In the previous merge flow, target peer deletes meta of source peer without marking it as pending remove.
/// If source peer becomes leader at the same time, it will panic due to corrupted meta.
#[test]
fn test_node_merge_transfer_leader() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.store_batch_system.max_batch_size = 1;
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    // To ensure the region has applied to its current term so that later `split` can success
    // without any retries. Then, `left_peer_3` will must be `1003`.
    let region = pd_client.get_region(b"k1").unwrap();
    let peer_1 = find_peer(&region, 1).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), peer_1);
    let k = b"k1_for_apply_to_current_term";
    cluster.must_put(k, b"value");
    must_get_equal(&cluster.get_engine(1), k, b"value");

    cluster.must_split(&region, b"k2");

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    let left_peer_1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_peer_1.clone());

    let left_peer_3 = find_peer(&left, 3).unwrap().to_owned();
    assert_eq!(left_peer_3.get_id(), 1003);

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    cluster.must_try_merge(left.get_id(), right.get_id());

    // Prevent peer 1003 to handle ready when it's leader
    let before_handle_raft_ready_1003 = "before_handle_raft_ready_1003";
    fail::cfg(before_handle_raft_ready_1003, "pause").unwrap();

    let epoch = cluster.get_region_epoch(left.get_id());
    let mut transfer_leader_req =
        new_admin_request(left.get_id(), &epoch, new_transfer_leader_cmd(left_peer_3));
    transfer_leader_req.mut_header().set_peer(left_peer_1);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, transfer_leader_req, Callback::None)
        .unwrap();
    fail::remove(schedule_merge_fp);

    pd_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    fail::remove(before_handle_raft_ready_1003);
    sleep_ms(100);
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

#[test]
fn test_node_merge_cascade_merge_with_apply_yield() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k5");
    let region = pd_client.get_region(b"k5").unwrap();
    cluster.must_split(&region, b"k9");

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k5").unwrap();
    let r3 = pd_client.get_region(b"k9").unwrap();

    pd_client.must_merge(r2.get_id(), r1.get_id());
    assert_eq!(r1.get_id(), 1000);
    let yield_apply_1000_fp = "yield_apply_1000";
    fail::cfg(yield_apply_1000_fp, "80%3*return()").unwrap();

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v2");
    }

    pd_client.must_merge(r3.get_id(), r1.get_id());

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v3");
    }
}

// Test if the rollback merge proposal is proposed before the majority of peers want to rollback
#[test]
fn test_node_multiple_rollback_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(20);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    let left_peer_1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_peer_1.clone());
    assert_eq!(left_peer_1.get_id(), 1001);

    let on_schedule_merge_fp = "on_schedule_merge";
    let on_check_merge_not_1001_fp = "on_check_merge_not_1001";

    let mut right_peer_1_id = find_peer(&right, 1).unwrap().get_id();

    for i in 0..3 {
        fail::cfg(on_schedule_merge_fp, "return()").unwrap();
        cluster.must_try_merge(left.get_id(), right.get_id());
        // Change the epoch of target region and the merge will fail
        pd_client.must_remove_peer(right.get_id(), new_peer(1, right_peer_1_id));
        right_peer_1_id += 100;
        pd_client.must_add_peer(right.get_id(), new_peer(1, right_peer_1_id));
        // Only the source leader is running `on_check_merge`
        fail::cfg(on_check_merge_not_1001_fp, "return()").unwrap();
        fail::remove(on_schedule_merge_fp);
        // In previous implementation, rollback merge proposal can be proposed by leader itself
        // So wait for the leader propose rollback merge if possible
        sleep_ms(100);
        // Check if the source region is still in merging mode.
        let mut l_r = pd_client.get_region(b"k1").unwrap();
        let req = new_request(
            l_r.get_id(),
            l_r.take_region_epoch(),
            vec![new_put_cf_cmd(
                "default",
                format!("k1{}", i).as_bytes(),
                b"vv",
            )],
            false,
        );
        let resp = cluster
            .call_command_on_leader(req, Duration::from_millis(100))
            .unwrap();
        if !resp
            .get_header()
            .get_error()
            .get_message()
            .contains("merging mode")
        {
            panic!("resp {:?} does not contain merging mode error", resp);
        }

        fail::remove(on_check_merge_not_1001_fp);
        // Write data for waiting the merge to rollback easily
        cluster.must_put(format!("k1{}", i).as_bytes(), b"vv");
        // Make sure source region is not merged to target region
        assert_eq!(pd_client.get_region(b"k1").unwrap().get_id(), left.get_id());
    }
}

// In the previous implementation, the source peer will propose rollback merge
// after the local target peer's epoch is larger than recorded previously.
// But it's wrong. This test constructs a case that writing data to the source region
// after merging. This operation can succeed in the previous implementation which
// causes data loss.
// In the current implementation, the rollback merge proposal can be proposed only when
// the number of peers who want to rollback merge is greater than the majority of all
// peers. If so, this merge is impossible to succeed.
// PS: A peer who wants to rollback merge means its local target peer's epoch is larger
// than recorded.
#[test]
fn test_node_merge_write_data_to_source_region_after_merging() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    // For snapshot after merging
    cluster.cfg.raft_store.merge_max_log_gap = 10;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 12;
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = 1;
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");

    let mut region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    let right_peer_2 = find_peer(&right, 2).cloned().unwrap();
    assert_eq!(right_peer_2.get_id(), 2);

    // Make sure peer 2 finish split before pause
    cluster.must_put(b"k2pause", b"vpause");
    must_get_equal(&cluster.get_engine(2), b"k2pause", b"vpause");

    let on_handle_apply_2_fp = "on_handle_apply_2";
    fail::cfg(on_handle_apply_2_fp, "pause").unwrap();

    let right_peer_1 = find_peer(&right, 1).cloned().unwrap();
    cluster.must_transfer_leader(right.get_id(), right_peer_1);

    let left_peer_3 = find_peer(&left, 3).cloned().unwrap();
    cluster.must_transfer_leader(left.get_id(), left_peer_3.clone());

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    cluster.must_try_merge(left.get_id(), right.get_id());

    cluster.add_send_filter(IsolationFilterFactory::new(3));

    fail::remove(schedule_merge_fp);

    pd_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let state1 = cluster.apply_state(region.get_id(), 1);
    for i in 0..15 {
        cluster.must_put(format!("k2{}", i).as_bytes(), b"v2");
    }
    cluster.wait_log_truncated(region.get_id(), 1, state1.get_applied_index());
    // Ignore this msg to make left region exist.
    let on_has_merge_target_fp = "on_has_merge_target";
    fail::cfg(on_has_merge_target_fp, "return").unwrap();

    cluster.clear_send_filters();
    // On store 3, now the right region is updated by snapshot not applying logs
    // so the left region still exist.
    // Wait for left region to rollback merge (in previous wrong implementation)
    sleep_ms(200);
    // Write data to left region
    let mut new_left = left;
    let mut epoch = new_left.take_region_epoch();
    // prepareMerge => conf_ver + 1, version + 1
    // rollbackMerge => version + 1
    epoch.set_conf_ver(epoch.get_conf_ver() + 1);
    epoch.set_version(epoch.get_version() + 2);
    let mut req = new_request(
        new_left.get_id(),
        epoch,
        vec![new_put_cf_cmd("default", b"k11", b"v11")],
        false,
    );
    req.mut_header().set_peer(left_peer_3);
    if let Ok(()) = cluster
        .sim
        .rl()
        .async_command_on_node(3, req, Callback::None)
    {
        sleep_ms(200);
        // The write must not succeed
        must_get_none(&cluster.get_engine(2), b"k11");
        must_get_none(&cluster.get_engine(3), b"k11");
    }

    fail::remove(on_handle_apply_2_fp);
}

/// In previous implementation, destroying its source peer(s) and applying snapshot is not **atomic**.
/// It may break the rule of our merging process.
///
/// A tikv crash after its source peers have destroyed but this target peer does not become to
/// `Applying` state which means it will not apply snapshot after this tikv restarts.
/// After this tikv restarts, a new leader may send logs to this target peer, then the panic may happen
/// because it can not find its source peers when applying `CommitMerge` log.
///
/// This test is to reproduce above situation.
#[test]
fn test_node_merge_crash_before_snapshot_then_catch_up_logs() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.merge_max_log_gap = 10;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 11;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    // Make merge check resume quickly.
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 10;
    // election timeout must be greater than lease
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(90);
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration::millis(500);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let on_raft_gc_log_tick_fp = "on_raft_gc_log_tick";
    fail::cfg(on_raft_gc_log_tick_fp, "return()").unwrap();

    cluster.run();

    let mut region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store1);
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();
    cluster.must_transfer_leader(right.get_id(), right_on_store1);

    cluster.must_put(b"k1", b"v1");

    cluster.add_send_filter(IsolationFilterFactory::new(3));

    pd_client.must_merge(left.get_id(), right.get_id());

    region = pd_client.get_region(b"k1").unwrap();
    // Write some logs and the logs' number is greater than `raft_log_gc_count_limit`
    // for latter log compaction
    for i in 2..15 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }

    // Aim at making peer 2 only know the compact log but do not know it is committed
    let condition = Arc::new(AtomicBool::new(false));
    let recv_filter = Box::new(
        RegionPacketFilter::new(region.get_id(), 2)
            .direction(Direction::Recv)
            .when(condition.clone())
            .set_msg_callback(Arc::new(move |msg: &RaftMessage| {
                if !condition.load(Ordering::Acquire)
                    && msg.get_message().get_msg_type() == MessageType::MsgAppend
                    && !msg.get_message().get_entries().is_empty()
                {
                    condition.store(true, Ordering::Release);
                }
            })),
    );
    cluster.sim.wl().add_recv_filter(2, recv_filter);

    let state1 = cluster.truncated_state(region.get_id(), 1);
    // Remove log compaction failpoint
    fail::remove(on_raft_gc_log_tick_fp);
    // Wait to trigger compact raft log
    cluster.wait_log_truncated(region.get_id(), 1, state1.get_index() + 1);

    let peer_on_store3 = find_peer(&region, 3).unwrap().to_owned();
    assert_eq!(peer_on_store3.get_id(), 3);
    // Make peer 3 do not handle snapshot ready
    // In previous implementation, destroying its source peer and applying snapshot is not atomic.
    // So making its source peer be destroyed and do not apply snapshot to reproduce the problem
    let before_handle_snapshot_ready_3_fp = "before_handle_snapshot_ready_3";
    fail::cfg(before_handle_snapshot_ready_3_fp, "return()").unwrap();

    cluster.clear_send_filters();
    // Peer 1 will send snapshot to peer 3
    // Source peer sends msg to others to get target region info until the election timeout.
    // The max election timeout is 2 * 10 * 10 = 200ms
    let election_timeout = 2
        * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
        * cluster.cfg.raft_store.raft_election_timeout_ticks as u64;
    sleep_ms(election_timeout + 100);

    cluster.stop_node(1);
    cluster.stop_node(3);

    cluster.sim.wl().clear_recv_filters(2);
    fail::remove(before_handle_snapshot_ready_3_fp);
    cluster.run_node(3).unwrap();
    // Peer 2 will become leader and it don't know the compact log is committed.
    // So it will send logs not snapshot to peer 3
    for i in 20..30 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }
    must_get_equal(&cluster.get_engine(3), b"k29", b"v");
}

/// Test if snapshot is applying correctly when crash happens.
#[test]
fn test_node_merge_crash_when_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.merge_max_log_gap = 10;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 11;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    // Make merge check resume quickly.
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 10;
    // election timeout must be greater than lease
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(90);
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration::millis(500);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let on_raft_gc_log_tick_fp = "on_raft_gc_log_tick";
    fail::cfg(on_raft_gc_log_tick_fp, "return()").unwrap();

    cluster.run();

    let mut region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    region = pd_client.get_region(b"k2").unwrap();
    cluster.must_split(&region, b"k3");

    region = pd_client.get_region(b"k3").unwrap();
    cluster.must_split(&region, b"k4");

    region = pd_client.get_region(b"k4").unwrap();
    cluster.must_split(&region, b"k5");

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r1_on_store1 = find_peer(&r1, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r1.get_id(), r1_on_store1);
    let r2 = pd_client.get_region(b"k2").unwrap();
    let r2_on_store1 = find_peer(&r2, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r2.get_id(), r2_on_store1);
    let r3 = pd_client.get_region(b"k3").unwrap();
    let r3_on_store1 = find_peer(&r3, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r3.get_id(), r3_on_store1);
    let r4 = pd_client.get_region(b"k4").unwrap();
    let r4_on_store1 = find_peer(&r4, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r4.get_id(), r4_on_store1);
    let r5 = pd_client.get_region(b"k5").unwrap();
    let r5_on_store1 = find_peer(&r5, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r5.get_id(), r5_on_store1);

    for i in 1..5 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
        must_get_equal(&cluster.get_engine(3), format!("k{}", i).as_bytes(), b"v");
    }

    cluster.add_send_filter(IsolationFilterFactory::new(3));

    pd_client.must_merge(r2.get_id(), r3.get_id());
    pd_client.must_merge(r4.get_id(), r3.get_id());
    pd_client.must_merge(r1.get_id(), r3.get_id());
    pd_client.must_merge(r5.get_id(), r3.get_id());

    for i in 1..5 {
        for j in 1..20 {
            cluster.must_put(format!("k{}{}", i, j).as_bytes(), b"vvv");
        }
    }

    region = pd_client.get_region(b"k1").unwrap();

    let state1 = cluster.truncated_state(region.get_id(), 1);
    // Remove log compaction failpoint
    fail::remove(on_raft_gc_log_tick_fp);
    // Wait to trigger compact raft log
    cluster.wait_log_truncated(region.get_id(), 1, state1.get_index() + 1);

    let on_region_worker_apply_fp = "on_region_worker_apply";
    fail::cfg(on_region_worker_apply_fp, "return()").unwrap();
    let on_region_worker_destroy_fp = "on_region_worker_destroy";
    fail::cfg(on_region_worker_destroy_fp, "return()").unwrap();

    cluster.clear_send_filters();
    let timer = Instant::now();
    loop {
        let local_state = cluster.region_local_state(region.get_id(), 3);
        if local_state.get_state() == PeerState::Applying {
            break;
        }
        if timer.elapsed() > Duration::from_secs(1) {
            panic!("not become applying state after 1 seconds.");
        }
        sleep_ms(10);
    }
    cluster.stop_node(3);
    fail::remove(on_region_worker_apply_fp);
    fail::remove(on_region_worker_destroy_fp);
    cluster.run_node(3).unwrap();

    for i in 1..5 {
        for j in 1..20 {
            must_get_equal(
                &cluster.get_engine(3),
                format!("k{}{}", i, j).as_bytes(),
                b"vvv",
            );
        }
    }
}

#[test]
fn test_prewrite_before_max_ts_is_synced() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.run();

    // Transfer leader to node 1 first to ensure all operations happen on node 1
    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let do_prewrite = |cluster: &mut Cluster<ServerCluster>| {
        let region_id = right.get_id();
        let leader = cluster.leader_of_region(region_id).unwrap();
        let epoch = cluster.get_region_epoch(region_id);
        let mut ctx = Context::default();
        ctx.set_region_id(region_id);
        ctx.set_peer(leader);
        ctx.set_region_epoch(epoch);

        let mut req = PrewriteRequest::default();
        req.set_context(ctx);
        req.set_primary_lock(b"key".to_vec());
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.set_key(b"key".to_vec());
        mutation.set_value(b"value".to_vec());
        req.mut_mutations().push(mutation);
        req.set_start_version(100);
        req.set_lock_ttl(20000);
        req.set_use_async_commit(true);
        client.kv_prewrite(&req).unwrap()
    };

    fail::cfg("test_raftstore_get_tso", "return(50)").unwrap();
    cluster.pd_client.must_merge(left.get_id(), right.get_id());
    let resp = do_prewrite(&mut cluster);
    assert!(resp.get_region_error().has_max_timestamp_not_synced());
    fail::remove("test_raftstore_get_tso");
    thread::sleep(Duration::from_millis(200));
    let resp = do_prewrite(&mut cluster);
    assert!(!resp.get_region_error().has_max_timestamp_not_synced());
}

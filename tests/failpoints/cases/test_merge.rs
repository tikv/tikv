// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Sender, channel, sync_channel},
        *,
    },
    thread,
    time::Duration,
};

use engine_traits::{CF_DEFAULT, CF_RAFT, Peekable};
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{PrewriteRequestPessimisticAction::*, *},
    raft_cmdpb::{self, RaftCmdRequest},
    raft_serverpb::{PeerState, RaftMessage, RegionLocalState},
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use raftstore::{router::RaftStoreRouter, store::*};
use raftstore_v2::router::{PeerMsg, PeerTick};
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv::storage::{Snapshot, kv::SnapshotExt};
use tikv_util::{HandyRwLock, config::*, future::block_on_timeout, time::Instant};
use txn_types::{Key, LastChange, PessimisticLock};

/// Test if merge is rollback as expected.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_node_merge_rollback() {
    let mut cluster = new_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
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

    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    fail::cfg_callback("on_apply_res_prepare_merge", move || {
        tx.lock().unwrap().send(()).unwrap();
    })
    .unwrap();

    cluster.merge_region(region.get_id(), target_region.get_id(), Callback::None);
    // PrepareMerge is applied.
    rx.recv().unwrap();

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
        let state = cluster.region_local_state(region.get_id(), i);
        assert_eq!(state.get_state(), PeerState::Normal);
        assert_eq!(*state.get_region(), region);
    }

    pd_client.must_remove_peer(right.get_id(), new_peer(3, 5));
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    let target_region = pd_client.get_region(b"k3").unwrap();
    cluster.merge_region(region.get_id(), target_region.get_id(), Callback::None);
    // PrepareMerge is applied.
    rx.recv().unwrap();

    let mut region = pd_client.get_region(b"k1").unwrap();

    // Split to trigger rollback.
    cluster.must_split(&right, b"k3");
    fail::remove(schedule_merge_fp);
    // Wait till rollback.
    cluster.must_put(b"k12", b"v12");

    // After premerge and rollback, conf_ver becomes 3 + 1 = 4, version becomes 4 +
    // 2 = 6;
    region.mut_region_epoch().set_conf_ver(4);
    region.mut_region_epoch().set_version(6);
    for i in 1..3 {
        must_get_equal(&cluster.get_engine(i), b"k12", b"v12");
        let state = cluster.region_local_state(region.get_id(), i);
        assert_eq!(state.get_state(), PeerState::Normal);
        assert_eq!(*state.get_region(), region);
    }
}

/// Test if merge is still working when restart a cluster during merge.
#[test]
fn test_node_merge_restart() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
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
    let state: RegionLocalState = engine.get_msg_cf(CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Merging, "{:?}", state);
    let state_key = keys::region_state_key(right.get_id());
    let state: RegionLocalState = engine.get_msg_cf(CF_RAFT, &state_key).unwrap().unwrap();
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
            .get_msg_cf(CF_RAFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Tombstone, "{:?}", state);
        let state_key = keys::region_state_key(right.get_id());
        let state: RegionLocalState = cluster
            .get_engine(i)
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

#[test]
fn test_async_io_apply_before_leader_persist_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.cmd_batch_concurrent_ready_max_count = 0;
    cluster.cfg.raft_store.store_io_pool_size = 1;
    cluster.cfg.raft_store.max_apply_unpersisted_log_limit = 10000;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    let peer_1 = find_peer(&left, 1).cloned().unwrap();
    cluster.must_transfer_leader(left.get_id(), peer_1.clone());

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let raft_before_save_on_store_1_fp = "raft_before_persist_on_store_1";
    // Skip persisting to simulate raft log persist lag but not block node restart.
    fail::cfg(raft_before_save_on_store_1_fp, "return").unwrap();

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    // Propose merge on leader will fail with timeout due to not persist.
    let req = cluster.new_prepare_merge(left.get_id(), right.get_id());
    cluster
        .call_command_on_leader(req, Duration::from_secs(1))
        .unwrap_err();

    cluster.shutdown();
    let engine = cluster.get_engine(peer_1.get_store_id());
    let state_key = keys::region_state_key(left.get_id());
    let state: RegionLocalState = engine.get_msg_cf(CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Normal, "{:?}", state);
    let state_key = keys::region_state_key(right.get_id());
    let state: RegionLocalState = engine.get_msg_cf(CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Normal, "{:?}", state);
    fail::remove(schedule_merge_fp);
    fail::remove(raft_before_save_on_store_1_fp);
    cluster.start().unwrap();

    // Wait till merge is finished.
    pd_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    cluster.must_put(b"k4", b"v4");

    for i in 1..4 {
        must_get_equal(&cluster.get_engine(i), b"k4", b"v4");
        let state_key = keys::region_state_key(left.get_id());
        let state: RegionLocalState = cluster
            .get_engine(i)
            .get_msg_cf(CF_RAFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Tombstone, "{:?}", state);
        let state_key = keys::region_state_key(right.get_id());
        let state: RegionLocalState = cluster
            .get_engine(i)
            .get_msg_cf(CF_RAFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Normal, "{:?}", state);
        assert!(state.get_region().get_start_key().is_empty());
        assert!(state.get_region().get_end_key().is_empty());
    }
}

/// Test if merge is still working when restart a cluster during catching up
/// logs for merge.
#[test]
fn test_node_merge_catch_up_logs_restart() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
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
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 25;
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(12);
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
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 25;
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(12);
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
    // due to `before_handle_catch_up_logs_for_merge` failpoint, we already pass
    // `apply_index < catch_up_logs.merge.get_commit()` so now can let apply
    // index make progress.
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
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(12);
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

// Test if a merge handled properly when there are two different snapshots of
// one region arrive in one raftstore tick.
#[test]
fn test_node_merge_multiple_snapshots_together() {
    test_node_merge_multiple_snapshots(true)
}

// Test if a merge handled properly when there are two different snapshots of
// one region arrive in different raftstore tick.
#[test]
fn test_node_merge_multiple_snapshots_not_together() {
    test_node_merge_multiple_snapshots(false)
}

fn test_node_merge_multiple_snapshots(together: bool) {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    ignore_merge_target_integrity(&mut cluster.cfg, &cluster.pd_client);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    // make it gc quickly to trigger snapshot easily
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(10);
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

    // Add a collect snapshot filter, it will delay snapshots until have collected
    // multiple snapshots from different peers
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

    // Merge left and right region, due to isolation, the regions on store 3 are not
    // merged yet.
    pd_client.must_merge(left.get_id(), right.get_id());
    thread::sleep(Duration::from_millis(200));

    // Let peer of right region on store 3 to make append response to trigger a new
    // snapshot one is snapshot before merge, the other is snapshot after merge.
    // Here blocks raftstore for a while to make it not to apply snapshot and
    // receive new log now.
    fail::cfg("on_raft_ready", "sleep(100)").unwrap();
    cluster.clear_send_filters();
    thread::sleep(Duration::from_millis(200));
    // Filter message again to make sure peer on store 3 can not catch up
    // CommitMerge log
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

// Test if compact log is ignored after premerge was applied and restart
// I.e. is_merging flag should be set after restart
#[test]
fn test_node_merge_restart_after_apply_premerge_before_apply_compact_log() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.merge_max_log_gap = 10;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(11);
    // Rely on this config to trigger a compact log
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize(1));
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
        if timer.saturating_elapsed() > Duration::from_secs(3) {
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

/// Tests whether stale merge is rollback properly if it merges to the same
/// target region again later.
#[test]
fn test_node_failed_merge_before_succeed_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.merge_max_log_gap = 30;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
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
    // to apply fsm in the end of this batch. So even the first
    // `on_ready_prepare_merge` is executed after CatchUplogs, the latter
    // committed logs is still sent to apply fsm if CatchUpLogs and
    // `on_ready_prepare_merge` is in different batch.
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

/// Tests whether the source peer is destroyed correctly when transferring
/// leader during committing merge.
///
/// In the previous merge flow, target peer deletes meta of source peer without
/// marking it as pending remove. If source peer becomes leader at the same
/// time, it will panic due to corrupted meta.
#[test]
fn test_node_merge_transfer_leader() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    // To ensure the region has applied to its current term so that later `split`
    // can success without any retries. Then, `left_peer_3` will must be `1003`.
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
    configure_for_merge(&mut cluster.cfg);
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
    let yield_apply_first_region_fp = "yield_apply_first_region";
    fail::cfg(yield_apply_first_region_fp, "80%3*return()").unwrap();

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v2");
    }

    pd_client.must_merge(r3.get_id(), r1.get_id());

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v3");
    }
}

// Test if the rollback merge proposal is proposed before the majority of peers
// want to rollback
#[test]
fn test_node_multiple_rollback_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
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
        // In previous implementation, rollback merge proposal can be proposed by leader
        // itself So wait for the leader propose rollback merge if possible
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
// But it's wrong. This test constructs a case that writing data to the source
// region after merging. This operation can succeed in the previous
// implementation which causes data loss.
// In the current implementation, the rollback merge proposal can be proposed
// only when the number of peers who want to rollback merge is greater than the
// majority of all peers. If so, this merge is impossible to succeed.
// PS: A peer who wants to rollback merge means its local target peer's epoch is
// larger than recorded.
#[test]
fn test_node_merge_write_data_to_source_region_after_merging() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    // For snapshot after merging
    cluster.cfg.raft_store.merge_max_log_gap = 10;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(12);
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = Some(1);
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

/// In previous implementation, destroying its source peer(s) and applying
/// snapshot is not **atomic**. It may break the rule of our merging process.
///
/// A tikv crash after its source peers have destroyed but this target peer does
/// not become to `Applying` state which means it will not apply snapshot after
/// this tikv restarts. After this tikv restarts, a new leader may send logs to
/// this target peer, then the panic may happen because it can not find its
/// source peers when applying `CommitMerge` log.
///
/// This test is to reproduce above situation.
#[test]
fn test_node_merge_crash_before_snapshot_then_catch_up_logs() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.merge_max_log_gap = 10;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(11);
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
    // Write some logs and the logs' number is greater than
    // `raft_log_gc_count_limit` for latter log compaction
    for i in 2..15 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }

    // Aim at making peer 2 only know the compact log but do not know it is
    // committed
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
    // In previous implementation, destroying its source peer and applying snapshot
    // is not atomic. So making its source peer be destroyed and do not apply
    // snapshot to reproduce the problem
    let before_handle_snapshot_ready_3_fp = "before_handle_snapshot_ready_3";
    fail::cfg(before_handle_snapshot_ready_3_fp, "return()").unwrap();

    cluster.clear_send_filters();
    // Peer 1 will send snapshot to peer 3
    // Source peer sends msg to others to get target region info until the election
    // timeout. The max election timeout is 2 * 10 * 10 = 200ms
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
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(11);
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
        if timer.saturating_elapsed() > Duration::from_secs(1) {
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
    configure_for_merge(&mut cluster.cfg);
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

/// Testing that the source peer's read delegate should not be removed by the
/// target peer and only removed when the peer is destroyed
#[test]
fn test_source_peer_read_delegate_after_apply() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_split(&cluster.get_region(b""), b"k2");
    let target = cluster.get_region(b"k1");
    let source = cluster.get_region(b"k3");

    cluster.must_transfer_leader(target.get_id(), find_peer(&target, 1).unwrap().to_owned());

    let on_destroy_peer_fp = "destroy_peer";
    fail::cfg(on_destroy_peer_fp, "pause").unwrap();

    // Merge finish means the leader of the target region have call
    // `on_ready_commit_merge`
    pd_client.must_merge(source.get_id(), target.get_id());

    // The source peer's `ReadDelegate` should not be removed yet and mark as
    // `pending_remove`
    assert!(
        cluster.store_metas[&1]
            .lock()
            .unwrap()
            .readers
            .get(&source.get_id())
            .unwrap()
            .pending_remove
    );

    fail::remove(on_destroy_peer_fp);
    // Wait for source peer is destroyed
    sleep_ms(100);

    assert!(
        !cluster.store_metas[&1]
            .lock()
            .unwrap()
            .readers
            .contains_key(&source.get_id())
    );
}

#[test]
fn test_merge_with_concurrent_pessimistic_locking() {
    let peer_size_limit = 512 << 10;
    let instance_size_limit = 100 << 20;
    let mut cluster = new_server_cluster(0, 2);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");

    // Transfer the leader of the right region to store 2. The leaders of source and
    // target regions don't need to be on the same store.
    cluster.must_transfer_leader(right.id, new_peer(2, 2));

    let snapshot = cluster.must_get_snapshot_of_region(left.id);
    let txn_ext = snapshot.txn_ext.unwrap();
    txn_ext
        .pessimistic_locks
        .write()
        .insert(
            vec![(
                Key::from_raw(b"k0"),
                PessimisticLock {
                    primary: b"k0".to_vec().into_boxed_slice(),
                    start_ts: 10.into(),
                    ttl: 3000,
                    for_update_ts: 20.into(),
                    min_commit_ts: 30.into(),
                    last_change: LastChange::make_exist(15.into(), 3),
                    is_locked_with_conflict: false,
                },
            )],
            peer_size_limit,
            instance_size_limit,
        )
        .unwrap();

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    fail::cfg("before_propose_locks_on_region_merge", "pause").unwrap();

    // 1. Locking before proposing pessimistic locks in the source region can
    // succeed.
    let client2 = client.clone();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::PessimisticLock);
    mutation.key = b"k1".to_vec();
    let mut req = PessimisticLockRequest::default();
    req.set_context(cluster.get_ctx(b"k1"));
    req.set_mutations(vec![mutation].into());
    req.set_start_version(10);
    req.set_for_update_ts(10);
    req.set_primary_lock(b"k1".to_vec());
    fail::cfg("txn_before_process_write", "pause").unwrap();
    let res = thread::spawn(move || client2.kv_pessimistic_lock(&req).unwrap());
    thread::sleep(Duration::from_millis(150));
    cluster.merge_region(left.id, right.id, Callback::None);
    thread::sleep(Duration::from_millis(150));
    fail::remove("txn_before_process_write");
    let resp = res.join().unwrap();
    assert!(!resp.has_region_error());
    fail::remove("before_propose_locks_on_region_merge");

    // 2. After locks are proposed, later pessimistic lock request should fail.
    let mut mutation = Mutation::default();
    mutation.set_op(Op::PessimisticLock);
    mutation.key = b"k11".to_vec();
    let mut req = PessimisticLockRequest::default();
    req.set_context(cluster.get_ctx(b"k11"));
    req.set_mutations(vec![mutation].into());
    req.set_start_version(10);
    req.set_for_update_ts(10);
    req.set_primary_lock(b"k11".to_vec());
    fail::cfg("txn_before_process_write", "pause").unwrap();
    let res = thread::spawn(move || client.kv_pessimistic_lock(&req).unwrap());
    thread::sleep(Duration::from_millis(200));
    fail::remove("txn_before_process_write");
    let resp = res.join().unwrap();
    assert!(resp.has_region_error());
}

#[test]
fn test_merge_pessimistic_locks_with_concurrent_prewrite() {
    let peer_size_limit = 512 << 10;
    let instance_size_limit = 100 << 20;
    let mut cluster = new_server_cluster(0, 2);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");

    cluster.must_transfer_leader(right.id, new_peer(2, 2));

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let snapshot = cluster.must_get_snapshot_of_region(left.id);
    let txn_ext = snapshot.txn_ext.unwrap();
    let lock = PessimisticLock {
        primary: b"k0".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
        last_change: LastChange::make_exist(15.into(), 3),
        is_locked_with_conflict: false,
    };
    txn_ext
        .pessimistic_locks
        .write()
        .insert(
            vec![
                (Key::from_raw(b"k0"), lock.clone()),
                (Key::from_raw(b"k1"), lock),
            ],
            peer_size_limit,
            instance_size_limit,
        )
        .unwrap();

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k0".to_vec());
    mutation.set_value(b"v".to_vec());
    let mut req = PrewriteRequest::default();
    req.set_context(cluster.get_ctx(b"k0"));
    req.set_mutations(vec![mutation].into());
    req.set_pessimistic_actions(vec![DoPessimisticCheck]);
    req.set_start_version(10);
    req.set_for_update_ts(40);
    req.set_primary_lock(b"k0".to_vec());

    // First, pause apply and prewrite.
    fail::cfg("on_handle_apply", "pause").unwrap();
    let req2 = req.clone();
    let client2 = client.clone();
    let resp = thread::spawn(move || client2.kv_prewrite(&req2).unwrap());
    thread::sleep(Duration::from_millis(500));

    // Then, start merging. PrepareMerge should wait until prewrite is done.
    cluster.merge_region(left.id, right.id, Callback::None);
    thread::sleep(Duration::from_millis(500));
    assert!(txn_ext.pessimistic_locks.read().is_writable());

    // But a later prewrite request should fail because we have already banned all
    // later proposals.
    req.mut_mutations()[0].set_key(b"k1".to_vec());
    let resp2 = thread::spawn(move || client.kv_prewrite(&req).unwrap());

    fail::remove("on_handle_apply");
    let resp = resp.join().unwrap();
    assert!(!resp.has_region_error(), "{:?}", resp);

    let resp2 = resp2.join().unwrap();
    assert!(resp2.has_region_error());
}

#[test]
fn test_retry_pending_prepare_merge_fail() {
    let peer_size_limit = 512 << 10;
    let instance_size_limit = 100 << 20;
    let mut cluster = new_server_cluster(0, 2);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");

    cluster.must_transfer_leader(right.id, new_peer(2, 2));

    // Insert lock l1 into the left region
    let snapshot = cluster.must_get_snapshot_of_region(left.id);
    let txn_ext = snapshot.txn_ext.unwrap();
    let l1 = PessimisticLock {
        primary: b"k1".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
        last_change: LastChange::make_exist(15.into(), 3),
        is_locked_with_conflict: false,
    };
    txn_ext
        .pessimistic_locks
        .write()
        .insert(
            vec![(Key::from_raw(b"k1"), l1)],
            peer_size_limit,
            instance_size_limit,
        )
        .unwrap();

    // Pause apply and write some data to the left region
    fail::cfg("on_handle_apply", "pause").unwrap();
    let (propose_tx, propose_rx) = mpsc::sync_channel(10);
    fail::cfg_callback("after_propose", move || propose_tx.send(()).unwrap()).unwrap();

    let mut rx = cluster.async_put(b"k1", b"v11").unwrap();
    propose_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    block_on_timeout(rx.as_mut(), Duration::from_millis(200)).unwrap_err();

    // Then, start merging. PrepareMerge should become pending because applied_index
    // is smaller than proposed_index.
    cluster.merge_region(left.id, right.id, Callback::None);
    propose_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    thread::sleep(Duration::from_millis(200));
    assert!(txn_ext.pessimistic_locks.read().is_writable());

    // Set disk full error to let PrepareMerge fail. (Set both peer to full to avoid
    // transferring leader)
    fail::cfg("disk_already_full_peer_1", "return").unwrap();
    fail::cfg("disk_already_full_peer_2", "return").unwrap();
    fail::remove("on_handle_apply");
    let res = block_on_timeout(rx, Duration::from_secs(1)).unwrap();
    assert!(!res.get_header().has_error(), "{:?}", res);

    propose_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    fail::remove("disk_already_full_peer_1");
    fail::remove("disk_already_full_peer_2");

    // Merge should not succeed because the disk is full.
    thread::sleep(Duration::from_millis(300));
    cluster.reset_leader_of_region(left.id);
    assert_eq!(cluster.get_region(b"k1"), left);

    cluster.must_put(b"k1", b"v12");
}

#[test]
fn test_merge_pessimistic_locks_propose_fail() {
    let peer_size_limit = 512 << 10;
    let instance_size_limit = 100 << 20;
    let mut cluster = new_server_cluster(0, 2);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");

    // Sending a TransferLeaeder message to make left region fail to propose.

    let snapshot = cluster.must_get_snapshot_of_region(left.id);
    let txn_ext = snapshot.ext().get_txn_ext().unwrap().clone();
    let lock = PessimisticLock {
        primary: b"k1".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
        last_change: LastChange::make_exist(15.into(), 3),
        is_locked_with_conflict: false,
    };
    txn_ext
        .pessimistic_locks
        .write()
        .insert(
            vec![(Key::from_raw(b"k1"), lock)],
            peer_size_limit,
            instance_size_limit,
        )
        .unwrap();

    fail::cfg("raft_propose", "pause").unwrap();

    cluster.merge_region(left.id, right.id, Callback::None);
    thread::sleep(Duration::from_millis(500));
    assert_eq!(
        txn_ext.pessimistic_locks.read().status,
        LocksStatus::MergingRegion
    );

    // With the fail point set, we will fail to propose the locks or the
    // PrepareMerge request.
    fail::cfg("raft_propose", "return()").unwrap();

    // But after that, the pessimistic locks status should remain unchanged.
    for _ in 0..5 {
        thread::sleep(Duration::from_millis(500));
        if txn_ext.pessimistic_locks.read().status == LocksStatus::Normal {
            return;
        }
    }
    panic!(
        "pessimistic locks status should return to Normal, but got {:?}",
        txn_ext.pessimistic_locks.read().status
    );
}

// Testing that when the source peer is destroyed while merging, it should not
// persist the `merge_state` thus won't generate gc message to destroy other
// peers
#[test]
fn test_destroy_source_peer_while_merging() {
    let mut cluster = new_node_cluster(0, 5);
    configure_for_merge(&mut cluster.cfg);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    for i in 1..=5 {
        must_get_equal(&cluster.get_engine(i), b"k1", b"v1");
        must_get_equal(&cluster.get_engine(i), b"k3", b"v3");
    }

    cluster.must_split(&pd_client.get_region(b"k1").unwrap(), b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();
    cluster.must_transfer_leader(right.get_id(), new_peer(1, 1));

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    // Start merge and wait until peer 5 apply prepare merge
    cluster.must_try_merge(right.get_id(), left.get_id());
    cluster.must_peer_state(right.get_id(), 5, PeerState::Merging);

    // filter heartbeat and append message for peer 5
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(right.get_id(), 5)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgHeartbeat)
            .msg_type(MessageType::MsgAppend),
    ));

    // remove peer from target region to trigger merge rollback.
    pd_client.must_remove_peer(left.get_id(), find_peer(&left, 2).unwrap().clone());
    must_get_none(&cluster.get_engine(2), b"k1");

    // Merge must rollbacked if we can put more data to the source region
    fail::remove(schedule_merge_fp);
    cluster.must_put(b"k4", b"v4");
    for i in 1..=4 {
        must_get_equal(&cluster.get_engine(i), b"k4", b"v4");
    }

    // remove peer 5 from peer list so it will destroy itself by tombstone message
    // and should not persist the `merge_state`
    pd_client.must_remove_peer(right.get_id(), new_peer(5, 5));
    must_get_none(&cluster.get_engine(5), b"k3");

    // so that other peers will send message to store 5
    pd_client.must_add_peer(right.get_id(), new_peer(5, 6));
    // but it is still in tombstone state due to the message filter
    let state = cluster.region_local_state(right.get_id(), 5);
    assert_eq!(state.get_state(), PeerState::Tombstone);

    // let the peer on store 4 have a larger peer id
    pd_client.must_remove_peer(right.get_id(), new_peer(4, 4));
    pd_client.must_add_peer(right.get_id(), new_peer(4, 7));
    must_get_equal(&cluster.get_engine(4), b"k4", b"v4");

    // if store 5 have persist the merge state, peer 2 and peer 3 will be destroyed
    // because store 5 will response their request vote message with a gc
    // message, and peer 7 will cause store 5 panic because peer 7 have larger
    // peer id than the peer in the merge state
    cluster.clear_send_filters();
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    cluster.must_put(b"k5", b"v5");
    assert!(!state.has_merge_state(), "{:?}", state);
    for i in 2..=5 {
        must_get_equal(&cluster.get_engine(i), b"k5", b"v5");
    }
}

struct MsgTimeoutFilter {
    // wrap with mutex to make tx Sync.
    tx: Mutex<Sender<RaftMessage>>,
}

impl Filter for MsgTimeoutFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> raftstore::Result<()> {
        let mut res = Vec::with_capacity(msgs.len());
        for m in msgs.drain(..) {
            if m.get_message().msg_type == MessageType::MsgTimeoutNow {
                self.tx.lock().unwrap().send(m).unwrap();
            } else {
                res.push(m);
            }
        }

        *msgs = res;
        check_messages(msgs)
    }
}

// Concurrent execution between transfer leader and merge can cause rollback and
// commit merge at the same time before this fix which corrupt the region.
// It can happen as this:
// Assume at the begin, leader of source and target are both on node-1
// 1. node-1 transfer leader to node-2: execute up to sending MsgTimeoutNow
// (leader_transferre has been set), but before becoming follower.
// 2. node-1 source region propose, and apply PrepareMerge
// 3. node-1 target region propose CommitMerge but fail (due to
//    leader_transferre being set)
// 4. node-1 source region successfully proposed rollback merge
// 5. node-2 target region became leader and apply the first no-op entry
// 6. node-2 target region successfully proposed commit merge
// Now, rollback at source region and commit at target region are both proposed
// and will be executed which will cause region corrupt
#[test]
fn test_concurrent_between_transfer_leader_and_merge() {
    use test_raftstore_v2::*;
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    for i in 0..3 {
        must_get_equal(&cluster.get_engine(i + 1), b"k1", b"v1");
        must_get_equal(&cluster.get_engine(i + 1), b"k3", b"v3");
    }

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let right = pd_client.get_region(b"k1").unwrap();
    let left = pd_client.get_region(b"k3").unwrap();
    cluster.must_transfer_leader(
        left.get_id(),
        left.get_peers()
            .iter()
            .find(|p| p.store_id == 1)
            .cloned()
            .unwrap(),
    );

    cluster.must_transfer_leader(
        right.get_id(),
        right
            .get_peers()
            .iter()
            .find(|p| p.store_id == 1)
            .cloned()
            .unwrap(),
    );

    // Source region: 1, Target Region: 1000
    // Let target region in leader_transfering status by interceptting MsgTimeoutNow
    // msg by using Filter. So we make node-1-1000 be in leader_transferring status
    // for some time.
    let (tx, rx_msg) = channel();
    let filter = MsgTimeoutFilter { tx: Mutex::new(tx) };
    cluster.add_send_filter_on_node(1, Box::new(filter));

    pd_client.transfer_leader(
        right.get_id(),
        right
            .get_peers()
            .iter()
            .find(|p| p.store_id == 2)
            .cloned()
            .unwrap(),
        vec![],
    );

    let msg = rx_msg.recv().unwrap();

    // Now, node-1-1000 is in leader_transferring status. After it reject proposing
    // commit merge, make node-1-1 block before proposing rollback merge until
    // node-2-1000 propose commit merge.

    fail::cfg("on_reject_commit_merge_1", "pause").unwrap();

    let router = cluster.get_router(2).unwrap();
    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    let _ = fail::cfg_callback("propose_commit_merge_1", move || {
        tx.lock().unwrap().send(()).unwrap();
    });

    let (tx2, rx2) = channel();
    let tx2 = Mutex::new(tx2);
    let _ = fail::cfg_callback("on_propose_commit_merge_success", move || {
        tx2.lock().unwrap().send(()).unwrap();
    });

    cluster.merge_region(left.get_id(), right.get_id(), Callback::None);

    // Actually, store 1 should not reach the line of propose_commit_merge_1
    let _ = rx.recv_timeout(Duration::from_secs(2));
    router
        .force_send(
            msg.get_region_id(),
            PeerMsg::RaftMessage(Box::new(msg), None),
        )
        .unwrap();

    // Wait region 1 of node 2 to become leader
    rx2.recv().unwrap();
    fail::remove("on_reject_commit_merge_1");

    wait_region_epoch_change(&cluster, &right, Duration::from_secs(5));

    let region = pd_client.get_region(b"k1").unwrap();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(region.get_start_key(), right.get_start_key());
    assert_eq!(region.get_end_key(), left.get_end_key());

    cluster.must_put(b"k4", b"v4");
}

#[test]
fn test_deterministic_commit_rollback_merge() {
    use test_raftstore_v2::*;
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    // Use a large election tick to stable test.
    configure_for_lease_read(&mut cluster.cfg, None, Some(1000));
    // Use 2 threads for polling peers, so that they can run concurrently.
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.cfg.raft_store.store_batch_system.max_batch_size = Some(1);
    cluster.run();

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();
    let right_1 = find_peer(&right, 1).unwrap().clone();
    cluster.must_transfer_leader(right.get_id(), right_1);
    let left_2 = find_peer(&left, 2).unwrap().clone();
    cluster.must_transfer_leader(left.get_id(), left_2);

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    for i in 0..3 {
        must_get_equal(&cluster.get_engine(i + 1), b"k1", b"v1");
        must_get_equal(&cluster.get_engine(i + 1), b"k3", b"v3");
    }

    // Delay 1003 apply by dropping append response, so that proposal will fail
    // due to applied_term != current_term.
    let target_region_id = left.get_id();
    cluster.add_recv_filter_on_node(
        1,
        Box::new(DropMessageFilter::new(Arc::new(move |m| {
            if m.get_region_id() == target_region_id {
                return m.get_message().get_msg_type() != MessageType::MsgAppendResponse;
            }
            true
        }))),
    );

    let left_1 = find_peer(&left, 1).unwrap().clone();
    cluster.must_transfer_leader(left.get_id(), left_1);

    // left(1000) <- right(1).
    let (tx1, rx1) = channel();
    let (tx2, rx2) = channel();
    let tx1 = Mutex::new(tx1);
    let rx2 = Mutex::new(rx2);
    fail::cfg_callback("on_propose_commit_merge_fail_store_1", move || {
        tx1.lock().unwrap().send(()).unwrap();
        rx2.lock().unwrap().recv().unwrap();
    })
    .unwrap();
    cluster.merge_region(right.get_id(), left.get_id(), Callback::None);

    // Wait for target fails to propose commit merge.
    rx1.recv_timeout(Duration::from_secs(5)).unwrap();
    // Let target apply continue, and new AskCommitMerge messages will propose
    // commit merge successfully.
    cluster.clear_recv_filter_on_node(1);

    // Trigger a CheckMerge tick, so source will send a AskCommitMerge again.
    fail::cfg("ask_target_peer_to_commit_merge_store_1", "pause").unwrap();
    let router = cluster.get_router(1).unwrap();
    router
        .check_send(1, PeerMsg::Tick(PeerTick::CheckMerge))
        .unwrap();

    // Send RejectCommitMerge to source.
    tx2.send(()).unwrap();
    fail::remove("on_propose_commit_merge_fail_store_1");

    // Wait for target applies to current term.
    cluster.must_put(b"k1", b"v11");

    // By remove the failpoint, CheckMerge tick sends a AskCommitMerge again.
    fail::remove("ask_target_peer_to_commit_merge_store_1");
    // At this point, source region will propose rollback merge if commit merge
    // is not deterministic.

    // Wait for source handle commit or rollback merge.
    wait_region_epoch_change(&cluster, &left, Duration::from_secs(5));

    // No matter commit merge or rollback merge, cluster must be available to
    // process requests
    cluster.must_put(b"k0", b"v0");
    cluster.must_put(b"k4", b"v4");
}

struct MsgVoteFilter {}

impl Filter for MsgVoteFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> raftstore::Result<()> {
        msgs.retain(|m| {
            let msg_type = m.get_message().msg_type;
            msg_type != MessageType::MsgRequestPreVote && msg_type != MessageType::MsgRequestVote
        });
        check_messages(msgs)
    }
}

// Before the fix of this PR (#15649), after prepare merge, raft cmd can still
// be proposed if restart is involved. If the proposed raft cmd is CompactLog,
// panic can occur during fetch entries: see issue https://github.com/tikv/tikv/issues/15633.
// Consider the case:
// 1. node-1 apply PrepareMerge (assume log index 30), so it's in is_merging
//    status which reject all proposals except for Rollback Merge
// 2. node-1 advance persisted_apply to 30
// 3. node-1 restart and became leader. Now, it's not in is_merging status, so
//    proposals can be proposed
// 4. node-1 propose CompactLog, replicate it to other nodes, and commit
// 5. node-0 apply PrepareMerge
// 6. node-0 apply CompactLog
// 6. node-0 fetches raft log entries which is required by
//    AdminCmdType::CommitMerge and panic (due to compacted)
#[test]
fn test_restart_may_lose_merging_state() {
    use test_raftstore_v2::*;
    let mut cluster = new_node_cluster(0, 2);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(12);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(10);

    cluster.run();
    fail::cfg("maybe_propose_compact_log", "return").unwrap();
    fail::cfg("on_ask_commit_merge", "return").unwrap();
    fail::cfg("flush_before_close_threshold", "return(0)").unwrap();

    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    fail::cfg_callback("on_apply_res_prepare_merge", move || {
        tx.lock().unwrap().send(()).unwrap();
    })
    .unwrap();

    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k20");

    let source = cluster.get_region(b"k05");
    let target = cluster.get_region(b"k25");

    cluster.add_send_filter_on_node(2, Box::new(MsgVoteFilter {}));

    cluster.must_transfer_leader(
        source.id,
        source
            .get_peers()
            .iter()
            .find(|p| p.store_id == 1)
            .cloned()
            .unwrap(),
    );
    cluster.must_transfer_leader(
        target.id,
        target
            .get_peers()
            .iter()
            .find(|p| p.store_id == 1)
            .cloned()
            .unwrap(),
    );

    for i in 0..20 {
        let k = format!("k{:02}", i);
        cluster.must_put(k.as_bytes(), b"val");
    }

    cluster.merge_region(source.id, target.id, Callback::None);

    rx.recv().unwrap();
    let router = cluster.get_router(1).unwrap();
    let (tx, rx) = sync_channel(1);
    let msg = PeerMsg::FlushBeforeClose { tx };
    router.force_send(source.id, msg).unwrap();
    rx.recv().unwrap();

    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    fail::cfg_callback("on_apply_res_commit_merge_2", move || {
        tx.lock().unwrap().send(()).unwrap();
    })
    .unwrap();

    cluster.stop_node(1);
    // Need to avoid propose commit merge, before node 1 becomes leader. Otherwise,
    // the commit merge will be rejected.
    let (tx2, rx2) = channel();
    let tx2 = Mutex::new(tx2);
    fail::cfg_callback("on_applied_current_term", move || {
        tx2.lock().unwrap().send(()).unwrap();
    })
    .unwrap();

    fail::remove("maybe_propose_compact_log");
    cluster.run_node(1).unwrap();

    // we have two regions.
    rx2.recv().unwrap();
    rx2.recv().unwrap();
    fail::remove("on_ask_commit_merge");
    // wait node 2 to apply commit merge
    rx.recv_timeout(Duration::from_secs(10)).unwrap();

    wait_region_epoch_change(&cluster, &target, Duration::from_secs(5));

    let region = cluster.get_region(b"k1");
    assert_eq!(region.get_id(), target.get_id());
    assert_eq!(region.get_start_key(), source.get_start_key());
    assert_eq!(region.get_end_key(), target.get_end_key());

    cluster.must_put(b"k400", b"v400");
}

// If a node is isolated during merge, and the target peer is replaced by a peer
// with a larger ID, then the snapshot of the target peer covers the source
// regions as well.
// In such cases, the snapshot becomes an "atomic_snapshot" which needs to
// destroy the source peer too.
// This test case checks the race between destroying the source peer by atomic
// snapshot and the gc message. The source peer must be successfully destroyed
// in this case.
#[test_case(test_raftstore::new_node_cluster)]
fn test_destroy_race_during_atomic_snapshot_after_merge() {
    let mut cluster = new_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");

    // Allow raft messages to source peer on store 3 before PrepareMerge.
    let left_filter_block = Arc::new(atomic::AtomicBool::new(false));
    let left_filter_block_ = left_filter_block.clone();
    let left_blocked_messages = Arc::new(Mutex::new(vec![]));
    let left_filter = RegionPacketFilter::new(left.get_id(), 3)
        .direction(Direction::Recv)
        .when(left_filter_block.clone())
        .reserve_dropped(left_blocked_messages.clone())
        .set_msg_callback(Arc::new(move |msg: &RaftMessage| {
            if left_filter_block.load(atomic::Ordering::SeqCst) {
                return;
            }
            for e in msg.get_message().get_entries() {
                let ctx = raftstore::store::ProposalContext::from_bytes(&e.context);
                if ctx.contains(raftstore::store::ProposalContext::PREPARE_MERGE) {
                    // Block further messages.
                    left_filter_block.store(true, atomic::Ordering::SeqCst);
                }
            }
        }));
    cluster.sim.wl().add_recv_filter(3, Box::new(left_filter));
    // Block messages to target peer on store 3.
    let right_filter_block = Arc::new(atomic::AtomicBool::new(true));
    let new_peer_id = 1004;
    let (new_peer_id_tx, new_peer_id_rx) = std::sync::mpsc::channel();
    let new_peer_id_tx = Mutex::new(Some(new_peer_id_tx));
    let (new_peer_snap_tx, new_peer_snap_rx) = std::sync::mpsc::channel();
    let new_peer_snap_tx = Mutex::new(new_peer_snap_tx);
    let right_filter = RegionPacketFilter::new(right.get_id(), 3)
        .direction(Direction::Recv)
        .when(right_filter_block.clone())
        .set_msg_callback(Arc::new(move |msg: &RaftMessage| {
            if msg.get_to_peer().get_id() == new_peer_id {
                let _ = new_peer_id_tx.lock().unwrap().take().map(|tx| tx.send(()));
                if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
                    let _ = new_peer_snap_tx.lock().unwrap().send(());
                }
            }
        }));
    cluster.sim.wl().add_recv_filter(3, Box::new(right_filter));
    pd_client.must_merge(left.get_id(), right.get_id());

    // Make target peer on store 3 a stale peer.
    pd_client.must_remove_peer(right.get_id(), find_peer(&right, 3).unwrap().to_owned());
    pd_client.must_add_peer(right.get_id(), new_peer(3, new_peer_id));
    // Unblock messages to target peer on store 3.
    right_filter_block.store(false, atomic::Ordering::SeqCst);
    // Wait for receiving new peer id message to destroy stale target peer.
    new_peer_id_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    cluster.must_region_not_exist(right.get_id(), 3);
    // Let source peer continue prepare merge. It will fails to schedule merge,
    // because the target peer is destroyed.
    left_filter_block_.store(false, atomic::Ordering::SeqCst);
    // Before sending blocked messages, make sure source peer is paused at
    // destroy apply delegate, so that the new right peer snapshot can will
    // try to destroy source peer before applying snapshot.
    fail::cfg("on_apply_handle_destroy", "pause").unwrap();
    // Send blocked messages to source peer. Prepare merge must fail to schedule
    // CommitMerge because now target peer stale peer is destroyed.
    let router = cluster.sim.wl().get_router(3).unwrap();
    for raft_msg in std::mem::take(&mut *left_blocked_messages.lock().unwrap()) {
        router.send_raft_message(raft_msg).unwrap();
    }
    // Wait the new right peer snapshot.
    new_peer_snap_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    // Give it some time to step snapshot message.
    sleep_ms(500);
    // Let source peer destroy continue, so it races with atomic snapshot destroy.
    fail::remove("on_apply_handle_destroy");

    // New peer applies snapshot eventually.
    cluster.must_transfer_leader(right.get_id(), new_peer(3, new_peer_id));
    cluster.must_put(b"k4", b"v4");
}

// `test_raft_log_gc_after_merge` tests when a region is destoryed, e.g. due to
// region merge, PeerFsm can still handle pending raft messages correctly.
#[test]
fn test_raft_log_gc_after_merge() {
    let mut cluster = new_node_cluster(0, 1);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");

    fail::cfg_callback("destroy_region_before_gc_flush", move || {
        fail::cfg("pause_on_peer_collect_message", "pause").unwrap();
    })
    .unwrap();

    let (tx, rx) = channel();
    fail::cfg_callback("destroy_region_after_gc_flush", move || {
        tx.send(()).unwrap();
    })
    .unwrap();

    // the right peer's id is 1.
    pd_client.must_merge(right.get_id(), left.get_id());
    rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let raft_router = cluster.get_router(1).unwrap();

    // send a raft cmd to test when peer fsm is closed, this cmd will still be
    // handled.
    let cmd = {
        let mut cmd = RaftCmdRequest::default();
        let mut req = raft_cmdpb::Request::default();
        req.set_read_index(raft_cmdpb::ReadIndexRequest::default());
        cmd.mut_requests().push(req);
        cmd.mut_header().region_id = 1;
        cmd
    };
    let (tx, rx) = std::sync::mpsc::channel();
    let callback = Callback::read(Box::new(move |req| {
        tx.send(req).unwrap();
    }));
    let cmd_req = RaftCommand::new(cmd, callback);
    raft_router.send_raft_command(cmd_req).unwrap();

    // send a casual msg that can trigger panic after peer fms closed.
    raft_router
        .send_casual_msg(1, CasualMessage::ForceCompactRaftLogs)
        .unwrap();

    fail::remove("pause_on_peer_collect_message");

    // wait some time for merge finish.
    std::thread::sleep(Duration::from_secs(1));
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");

    let resp = rx.recv().unwrap();
    assert!(resp.response.get_header().has_error());
}

// This case tests following scenario:
// When the disk IO is slow or block and PrepareMerge is proposed, it is
// possible that the leader's committed index is higher than its persisted
// index. In this case, we should still ensure that all peers can recover
// necessary raft logs from the CatchUpLogs phase.
// For more information, see: https://github.com/tikv/tikv/issues/18129
#[test]
fn test_node_merge_with_apply_ahead_of_persist() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);
    cluster.cfg.raft_store.cmd_batch_concurrent_ready_max_count = 32;
    cluster.cfg.raft_store.store_io_pool_size = 1;
    // even if "early apply" is disabled, the raft committed index can still be
    // higher than persisted/matched index.
    cluster.cfg.raft_store.max_apply_unpersisted_log_limit = 0;

    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");

    cluster.must_put(b"k1", b"v2");
    cluster.must_put(b"k3", b"v2");

    let raft_before_save_on_store_1_fp = "raft_before_persist_on_store_1";
    // Skip persisting to simulate raft log persist lag but not block node restart.
    fail::cfg(raft_before_save_on_store_1_fp, "return").unwrap();

    let cmd = new_put_cf_cmd(CF_DEFAULT, b"k1", b"v3");
    let req = new_request(left.id, left.get_region_epoch().clone(), vec![cmd], false);
    cluster
        .call_command_on_leader(req, Duration::from_secs(1))
        .unwrap_err();
    let cmd = new_put_cf_cmd(CF_DEFAULT, b"k3", b"v3");
    let req = new_request(right.id, right.get_region_epoch().clone(), vec![cmd], false);
    cluster
        .call_command_on_leader(req, Duration::from_secs(1))
        .unwrap_err();

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    // Propose merge on leader will fail with timeout due to not persist.
    let req = cluster.new_prepare_merge(left.get_id(), right.get_id());
    cluster
        .call_command_on_leader(req, Duration::from_secs(1))
        .unwrap_err();

    // stop node 1, some unpersisted logs will be lost.
    cluster.stop_node(1);

    fail::remove(raft_before_save_on_store_1_fp);
    fail::remove(schedule_merge_fp);

    // Wait till merge is finished.
    pd_client.check_merged_timeout(left.get_id(), Duration::from_secs(5));

    // restart node 1 to let it apply the merge.
    cluster.run_node(1).unwrap();

    let region = cluster.get_region(b"k1");
    let peer = region
        .get_peers()
        .iter()
        .find(|p| p.store_id == 1)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(1, peer);

    cluster.must_put(b"k1", b"v3");
}

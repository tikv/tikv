// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::*, sync::*, thread, time::*};

use engine_rocks::Compat;
use engine_traits::{Peekable, CF_LOCK, CF_RAFT, CF_WRITE};
use kvproto::{
    kvrpcpb::Context,
    raft_cmdpb::CmdType,
    raft_serverpb::{PeerState, RaftMessage, RegionLocalState},
};
use pd_client::PdClient;
use raft::eraftpb::{ConfChangeType, MessageType};
use raftstore::store::{Callback, LocksStatus};
use test_raftstore::*;
use tikv::storage::{
    kv::{SnapContext, SnapshotExt},
    Engine, Snapshot,
};
use tikv_util::{config::*, HandyRwLock};
use txn_types::{Key, PessimisticLock};

/// Test if merge is working as expected in a general condition.
#[test]
fn test_node_base_merge() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);

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
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(left.get_end_key(), right.get_start_key());
    assert_eq!(right.get_start_key(), b"k2");
    let get = new_request(
        right.get_id(),
        right.get_region_epoch().clone(),
        vec![new_get_cmd(b"k1")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);
    assert!(
        resp.get_header().get_error().has_key_not_in_region(),
        "{:?}",
        resp
    );

    pd_client.must_merge(left.get_id(), right.get_id());

    let region = pd_client.get_region(b"k1").unwrap();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());
    let origin_epoch = left.get_region_epoch();
    let new_epoch = region.get_region_epoch();
    // PrepareMerge + CommitMerge, so it should be 2.
    assert_eq!(new_epoch.get_version(), origin_epoch.get_version() + 2);
    assert_eq!(new_epoch.get_conf_ver(), origin_epoch.get_conf_ver());
    let get = new_request(
        region.get_id(),
        new_epoch.to_owned(),
        vec![new_get_cmd(b"k1")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");

    let version = left.get_region_epoch().get_version();
    let conf_ver = left.get_region_epoch().get_conf_ver();
    'outer: for i in 1..4 {
        let state_key = keys::region_state_key(left.get_id());
        let mut state = RegionLocalState::default();
        for _ in 0..3 {
            state = cluster
                .get_engine(i)
                .c()
                .get_msg_cf(CF_RAFT, &state_key)
                .unwrap()
                .unwrap();
            if state.get_state() == PeerState::Tombstone {
                let epoch = state.get_region().get_region_epoch();
                assert_eq!(epoch.get_version(), version + 1);
                assert_eq!(epoch.get_conf_ver(), conf_ver + 1);
                continue 'outer;
            }
            thread::sleep(Duration::from_millis(500));
        }
        panic!("store {} is still not merged: {:?}", i, state);
    }

    cluster.must_put(b"k4", b"v4");
}

#[test]
fn test_node_merge_with_slow_learner() {
    let mut cluster = new_node_cluster(0, 2);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.raft_log_gc_threshold = 40;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(40);
    cluster.cfg.raft_store.merge_max_log_gap = 15;
    cluster.pd_client.disable_default_operator();

    // Create a cluster with peer 1 as leader and peer 2 as learner.
    let r1 = cluster.run_conf_change();
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.must_add_peer(r1, new_learner_peer(2, 2));

    // Split the region.
    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(left.get_end_key(), right.get_start_key());
    assert_eq!(right.get_start_key(), b"k2");

    // Make sure the leader has received the learner's last index.
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k3", b"v3");

    cluster.add_send_filter(IsolationFilterFactory::new(2));
    (0..20).for_each(|i| cluster.must_put(b"k1", format!("v{}", i).as_bytes()));

    // Merge 2 regions under isolation should fail.
    let merge = new_prepare_merge(right.clone());
    let req = new_admin_request(left.get_id(), left.get_region_epoch(), merge);
    let resp = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("log gap")
    );

    cluster.clear_send_filters();
    cluster.must_put(b"k11", b"v100");
    must_get_equal(&cluster.get_engine(1), b"k11", b"v100");
    must_get_equal(&cluster.get_engine(2), b"k11", b"v100");

    pd_client.must_merge(left.get_id(), right.get_id());

    // Test slow learner will be cleaned up when merge can't be continued.
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k5");
    cluster.must_put(b"k4", b"v4");
    cluster.must_put(b"k5", b"v5");
    must_get_equal(&cluster.get_engine(2), b"k4", b"v4");
    must_get_equal(&cluster.get_engine(2), b"k5", b"v5");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k5").unwrap();
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    pd_client.must_merge(left.get_id(), right.get_id());
    let state1 = cluster.truncated_state(right.get_id(), 1);
    (0..50).for_each(|i| cluster.must_put(b"k2", format!("v{}", i).as_bytes()));

    // Wait to trigger compact raft log
    cluster.wait_log_truncated(right.get_id(), 1, state1.get_index() + 1);
    cluster.clear_send_filters();
    cluster.must_put(b"k6", b"v6");
    must_get_equal(&cluster.get_engine(2), b"k6", b"v6");
}

/// Test whether merge will be aborted if prerequisites is not met.
#[test]
fn test_node_merge_prerequisites_check() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store1);
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();
    cluster.must_transfer_leader(right.get_id(), right_on_store1);

    // first MsgAppend will append log, second MsgAppend will set commit index,
    // So only allowing first MsgAppend to make source peer have uncommitted entries.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend)
            .allow(1),
    ));
    // make the source peer's commit index can't be updated by MsgHeartbeat.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 3)
            .msg_type(MessageType::MsgHeartbeat)
            .direction(Direction::Recv),
    ));
    cluster.must_split(&left, b"k11");
    let res = cluster.try_merge(left.get_id(), right.get_id());
    // log gap (min_committed, last_index] contains admin entries.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_send_filters();
    cluster.must_put(b"k22", b"v22");
    must_get_equal(&cluster.get_engine(3), b"k22", b"v22");

    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(
        right.get_id(),
        3,
    )));
    // It doesn't matter if the index and term is correct.
    let compact_log = new_compact_log_request(100, 10);
    let req = new_admin_request(right.get_id(), right.get_region_epoch(), compact_log);
    debug!("requesting {:?}", req);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res.get_header().has_error(), "{:?}", res);
    let res = cluster.try_merge(right.get_id(), left.get_id());
    // log gap (min_matched, last_index] contains admin entries.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_send_filters();
    cluster.must_put(b"k23", b"v23");
    must_get_equal(&cluster.get_engine(3), b"k23", b"v23");

    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(
        right.get_id(),
        3,
    )));
    let mut large_bytes = vec![b'k', b'3'];
    // 3M
    large_bytes.extend(repeat(b'0').take(1024 * 1024 * 3));
    cluster.must_put(&large_bytes, &large_bytes);
    cluster.must_put(&large_bytes, &large_bytes);
    // So log gap now contains 12M data, which exceeds the default max entry size.
    let res = cluster.try_merge(right.get_id(), left.get_id());
    // log gap contains admin entries.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_send_filters();
    cluster.must_put(b"k24", b"v24");
    must_get_equal(&cluster.get_engine(3), b"k24", b"v24");
}

/// Test if stale peer will be handled properly after merge.
#[test]
fn test_node_check_merged_message() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    // test if stale peer before conf removal is destroyed automatically
    let mut region = pd_client.get_region(b"k1").unwrap();
    pd_client.must_add_peer(region.get_id(), new_peer(2, 2));
    pd_client.must_add_peer(region.get_id(), new_peer(3, 3));

    cluster.must_split(&region, b"k2");
    let mut left = pd_client.get_region(b"k1").unwrap();
    let mut right = pd_client.get_region(b"k2").unwrap();
    pd_client.must_add_peer(left.get_id(), new_peer(4, 4));
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");
    cluster.add_send_filter(IsolationFilterFactory::new(4));
    pd_client.must_remove_peer(left.get_id(), new_peer(4, 4));
    pd_client.must_merge(left.get_id(), right.get_id());
    cluster.clear_send_filters();
    must_get_none(&cluster.get_engine(4), b"k1");

    // test gc work under complicated situation.
    cluster.must_put(b"k5", b"v5");
    region = pd_client.get_region(b"k2").unwrap();
    cluster.must_split(&region, b"k2");
    region = pd_client.get_region(b"k4").unwrap();
    cluster.must_split(&region, b"k4");
    left = pd_client.get_region(b"k1").unwrap();
    let middle = pd_client.get_region(b"k3").unwrap();
    let middle_on_store1 = find_peer(&middle, 1).unwrap().to_owned();
    cluster.must_transfer_leader(middle.get_id(), middle_on_store1);
    right = pd_client.get_region(b"k5").unwrap();
    let left_on_store3 = find_peer(&left, 3).unwrap().to_owned();
    pd_client.must_remove_peer(left.get_id(), left_on_store3);
    must_get_none(&cluster.get_engine(3), b"k1");
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    left = pd_client.get_region(b"k1").unwrap();
    pd_client.must_add_peer(left.get_id(), new_peer(3, 5));
    left = pd_client.get_region(b"k1").unwrap();
    pd_client.must_merge(middle.get_id(), left.get_id());
    pd_client.must_merge(right.get_id(), left.get_id());
    cluster.must_delete(b"k3");
    cluster.must_delete(b"k5");
    cluster.must_put(b"k4", b"v4");
    cluster.clear_send_filters();
    let engine3 = cluster.get_engine(3);
    must_get_equal(&engine3, b"k1", b"v1");
    must_get_equal(&engine3, b"k4", b"v4");
    must_get_none(&engine3, b"k3");
    must_get_none(&engine3, b"v5");
}

#[test]
fn test_node_merge_slow_split_right() {
    test_node_merge_slow_split(true);
}

#[test]
fn test_node_merge_slow_split_left() {
    test_node_merge_slow_split(false);
}

// Test if a merge handled properly when there is a unfinished slow split before merge.
fn test_node_merge_slow_split(is_right_derive: bool) {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.cfg.raft_store.right_derive_when_split = is_right_derive;

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
        RegionPacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(right.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));
    cluster.must_split(&right, b"k3");

    // left region and right region on store 3 fall behind
    // so after split, the new generated region is not on store 3 now
    let right1 = pd_client.get_region(b"k2").unwrap();
    let right2 = pd_client.get_region(b"k3").unwrap();
    assert_ne!(right1.get_id(), right2.get_id());
    pd_client.must_merge(left.get_id(), right1.get_id());
    // after merge, the left region still exists on store 3

    cluster.must_put(b"k0", b"v0");
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");
}

/// Test various cases that a store is isolated during merge.
#[test]
fn test_node_merge_dist_isolation() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    cluster.must_transfer_leader(right.get_id(), new_peer(1, 1));
    let target_leader = left
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 3)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(left.get_id(), target_leader);
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");

    // So cluster becomes:
    //  left region: 1         I 2 3(leader)
    // right region: 1(leader) I 2 3
    // I means isolation.
    cluster.add_send_filter(IsolationFilterFactory::new(1));
    pd_client.must_merge(left.get_id(), right.get_id());
    cluster.must_put(b"k4", b"v4");
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(1), b"k4", b"v4");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    cluster.must_put(b"k11", b"v11");
    pd_client.must_remove_peer(right.get_id(), new_peer(3, 3));
    cluster.must_put(b"k33", b"v33");

    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(right.get_id(), 3).direction(Direction::Recv),
    ));
    pd_client.must_add_peer(right.get_id(), new_peer(3, 4));
    let right = pd_client.get_region(b"k3").unwrap();
    // So cluster becomes:
    //  left region: 1         2   3(leader)
    // right region: 1(leader) 2  [3]
    // [x] means a replica exists logically but is not created on the store x yet.
    let res = cluster.try_merge(region.get_id(), right.get_id());
    // Leader can't find replica 3 of right region, so it fails.
    assert!(res.get_header().has_error(), "{:?}", res);

    let target_leader = left
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 2)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(left.get_id(), target_leader);
    pd_client.must_merge(left.get_id(), right.get_id());
    cluster.must_put(b"k4", b"v4");

    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

/// Similar to `test_node_merge_dist_isolation`, but make the isolated store
/// way behind others so others have to send it a snapshot.
#[test]
fn test_node_merge_brain_split() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(12);

    cluster.run();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();

    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    // The split regions' leaders could be at store 3, so transfer them to peer 1.
    let left_peer_1 = find_peer(&left, 1).cloned().unwrap();
    cluster.must_transfer_leader(left.get_id(), left_peer_1);
    let right_peer_1 = find_peer(&right, 1).cloned().unwrap();
    cluster.must_transfer_leader(right.get_id(), right_peer_1);

    cluster.must_put(b"k11", b"v11");
    cluster.must_put(b"k21", b"v21");
    // Make sure peers on store 3 have replicated latest update, which means
    // they have already reported their progresses to leader.
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    must_get_equal(&cluster.get_engine(3), b"k21", b"v21");

    cluster.add_send_filter(IsolationFilterFactory::new(3));
    // So cluster becomes:
    //  left region: 1(leader) 2 I 3
    // right region: 1(leader) 2 I 3
    // I means isolation.
    pd_client.must_merge(left.get_id(), right.get_id());

    for i in 0..100 {
        cluster.must_put(format!("k4{}", i).as_bytes(), b"v4");
    }
    must_get_equal(&cluster.get_engine(2), b"k40", b"v4");
    must_get_equal(&cluster.get_engine(1), b"k40", b"v4");

    cluster.clear_send_filters();

    // Wait until store 3 get data after merging
    must_get_equal(&cluster.get_engine(3), b"k40", b"v4");
    let right_peer_3 = find_peer(&right, 3).cloned().unwrap();
    cluster.must_transfer_leader(right.get_id(), right_peer_3);
    cluster.must_put(b"k40", b"v5");

    // Make sure the two regions are already merged on store 3.
    let state_key = keys::region_state_key(left.get_id());
    let state: RegionLocalState = cluster
        .get_engine(3)
        .c()
        .get_msg_cf(CF_RAFT, &state_key)
        .unwrap()
        .unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);
    must_get_equal(&cluster.get_engine(3), b"k40", b"v5");
    for i in 1..100 {
        must_get_equal(&cluster.get_engine(3), format!("k4{}", i).as_bytes(), b"v4");
    }

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let region = pd_client.get_region(b"k2").unwrap();
    cluster.must_split(&region, b"k3");
    let middle = pd_client.get_region(b"k2").unwrap();
    let peer_on_store1 = find_peer(&middle, 1).unwrap().to_owned();
    cluster.must_transfer_leader(middle.get_id(), peer_on_store1);
    cluster.must_put(b"k22", b"v22");
    cluster.must_put(b"k33", b"v33");
    must_get_equal(&cluster.get_engine(3), b"k33", b"v33");
    let left = pd_client.get_region(b"k1").unwrap();
    pd_client.disable_default_operator();
    let peer_on_left = find_peer(&left, 3).unwrap().to_owned();
    pd_client.must_remove_peer(left.get_id(), peer_on_left);
    let right = pd_client.get_region(b"k3").unwrap();
    let peer_on_right = find_peer(&right, 3).unwrap().to_owned();
    pd_client.must_remove_peer(right.get_id(), peer_on_right);
    must_get_none(&cluster.get_engine(3), b"k11");
    must_get_equal(&cluster.get_engine(3), b"k22", b"v22");
    must_get_none(&cluster.get_engine(3), b"k33");
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    pd_client.must_add_peer(left.get_id(), new_peer(3, 11));
    pd_client.must_merge(middle.get_id(), left.get_id());
    pd_client.must_remove_peer(left.get_id(), new_peer(3, 11));
    pd_client.must_merge(right.get_id(), left.get_id());
    pd_client.must_add_peer(left.get_id(), new_peer(3, 12));
    let region = pd_client.get_region(b"k1").unwrap();
    // So cluster becomes
    // store   3: k2 [middle] k3
    // store 1/2: [  new_left ] k4 [left]
    cluster.must_split(&region, b"k4");
    cluster.must_put(b"k12", b"v12");
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(3), b"k12", b"v12");
}

/// Test whether approximate size and keys are updated after merge
#[test]
fn test_merge_approximate_size_and_keys() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(20);
    cluster.run();

    let mut range = 1..;
    let middle_key = put_cf_till_size(&mut cluster, CF_WRITE, 100, &mut range);
    let max_key = put_cf_till_size(&mut cluster, CF_WRITE, 100, &mut range);

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"").unwrap();

    cluster.must_split(&region, &middle_key);
    // make sure split check is invoked so size and keys are updated.
    thread::sleep(Duration::from_millis(100));

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();
    assert_ne!(left, right);

    // make sure all peer's approximate size is not None.
    cluster.must_transfer_leader(right.get_id(), right.get_peers()[0].clone());
    thread::sleep(Duration::from_millis(100));
    cluster.must_transfer_leader(right.get_id(), right.get_peers()[1].clone());
    thread::sleep(Duration::from_millis(100));
    cluster.must_transfer_leader(right.get_id(), right.get_peers()[2].clone());
    thread::sleep(Duration::from_millis(100));

    let size = pd_client
        .get_region_approximate_size(right.get_id())
        .unwrap();
    assert_ne!(size, 0);
    let keys = pd_client
        .get_region_approximate_keys(right.get_id())
        .unwrap();
    assert_ne!(keys, 0);

    pd_client.must_merge(left.get_id(), right.get_id());
    // make sure split check is invoked so size and keys are updated.
    thread::sleep(Duration::from_millis(100));

    let region = pd_client.get_region(b"").unwrap();
    // size and keys should be updated.
    assert_ne!(
        pd_client
            .get_region_approximate_size(region.get_id())
            .unwrap(),
        size
    );
    assert_ne!(
        pd_client
            .get_region_approximate_keys(region.get_id())
            .unwrap(),
        keys
    );

    // after merge and then transfer leader, if not update new leader's approximate size, it maybe be stale.
    cluster.must_transfer_leader(region.get_id(), region.get_peers()[0].clone());
    // make sure split check is invoked
    thread::sleep(Duration::from_millis(100));
    assert_ne!(
        pd_client
            .get_region_approximate_size(region.get_id())
            .unwrap(),
        size
    );
    assert_ne!(
        pd_client
            .get_region_approximate_keys(region.get_id())
            .unwrap(),
        keys
    );
}

#[test]
fn test_node_merge_update_region() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    // Election timeout and max leader lease is 1s.
    configure_for_lease_read(&mut cluster, Some(100), Some(10));

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    // Make sure the leader is in lease.
    cluster.must_put(b"k1", b"v2");

    // "k3" is not in the range of left.
    let get = new_request(
        left.get_id(),
        left.get_region_epoch().clone(),
        vec![new_get_cmd(b"k3")],
        false,
    );
    debug!("requesting key not in range {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);
    assert!(
        resp.get_header().get_error().has_key_not_in_region(),
        "{:?}",
        resp
    );

    // Merge right to left.
    pd_client.must_merge(right.get_id(), left.get_id());

    let origin_leader = cluster.leader_of_region(left.get_id()).unwrap();
    let new_leader = left
        .get_peers()
        .iter()
        .cloned()
        .find(|p| p.get_id() != origin_leader.get_id())
        .unwrap();

    // Make sure merge is done in the new_leader.
    // There is only one region in the cluster, "k0" must belongs to it.
    cluster.must_put(b"k0", b"v0");
    must_get_equal(&cluster.get_engine(new_leader.get_store_id()), b"k0", b"v0");

    // Transfer leadership to the new_leader.
    cluster.must_transfer_leader(left.get_id(), new_leader);

    // Make sure the leader is in lease.
    cluster.must_put(b"k0", b"v1");

    let new_region = pd_client.get_region(b"k2").unwrap();
    let get = new_request(
        new_region.get_id(),
        new_region.get_region_epoch().clone(),
        vec![new_get_cmd(b"k3")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v3");
}

/// Test if merge is working properly when merge entries is empty but commit index is not updated.
#[test]
fn test_node_merge_catch_up_logs_empty_entries() {
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

    // first MsgAppend will append log, second MsgAppend will set commit index,
    // So only allowing first MsgAppend to make source peer have uncommitted entries.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend)
            .allow(1),
    ));
    // make the source peer have no way to know the uncommitted entries can be applied from heartbeat.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 3)
            .msg_type(MessageType::MsgHeartbeat)
            .direction(Direction::Recv),
    ));
    // make the source peer have no way to know the uncommitted entries can be applied from target region.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(right.get_id(), 3)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Recv),
    ));
    pd_client.must_merge(left.get_id(), right.get_id());
    cluster.must_region_not_exist(left.get_id(), 2);
    cluster.shutdown();
    cluster.clear_send_filters();

    // as expected, merge process will forward the commit index
    // and the source peer will be destroyed.
    cluster.start().unwrap();
    cluster.must_region_not_exist(left.get_id(), 3);
}

#[test]
fn test_merge_with_slow_promote() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    pd_client.must_add_peer(left.get_id(), new_peer(3, left.get_id() + 3));
    pd_client.must_add_peer(right.get_id(), new_learner_peer(3, right.get_id() + 3));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");

    let delay_filter =
        Box::new(RegionPacketFilter::new(right.get_id(), 3).direction(Direction::Recv));
    cluster.sim.wl().add_send_filter(3, delay_filter);

    pd_client.must_add_peer(right.get_id(), new_peer(3, right.get_id() + 3));
    pd_client.must_merge(right.get_id(), left.get_id());
    cluster.sim.wl().clear_send_filters(3);
    cluster.must_transfer_leader(left.get_id(), new_peer(3, left.get_id() + 3));
}

/// Test whether a isolated store recover properly if there is no target peer
/// on this store before isolated.
/// A (-∞, k2), B [k2, +∞) on store 1,2,4
/// store 4 is isolated
/// B merge to A (target peer A is not created on store 4. It‘s just exist logically)
/// A split => C (-∞, k3), A [k3, +∞)
/// Then network recovery
#[test]
fn test_merge_isolated_store_with_no_target_peer() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    let region = pd_client.get_region(b"k1").unwrap();
    // (-∞, k2), [k2, +∞)
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store1);
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();
    cluster.must_transfer_leader(right.get_id(), right_on_store1);

    pd_client.must_add_peer(right.get_id(), new_peer(4, 4));
    let right_on_store3 = find_peer(&right, 3).unwrap().to_owned();
    pd_client.must_remove_peer(right.get_id(), right_on_store3);

    // Ensure snapshot is sent and applied.
    must_get_equal(&cluster.get_engine(4), b"k4", b"v1");
    cluster.must_put(b"k22", b"v22");
    // Ensure leader has updated its progress.
    must_get_equal(&cluster.get_engine(4), b"k22", b"v22");

    cluster.add_send_filter(IsolationFilterFactory::new(4));

    pd_client.must_add_peer(left.get_id(), new_peer(4, 5));
    let left_on_store3 = find_peer(&left, 3).unwrap().to_owned();
    pd_client.must_remove_peer(left.get_id(), left_on_store3);

    pd_client.must_merge(right.get_id(), left.get_id());

    let new_left = pd_client.get_region(b"k1").unwrap();
    // (-∞, k3), [k3, +∞)
    cluster.must_split(&new_left, b"k3");
    // Now new_left region range is [k3, +∞)
    cluster.must_put(b"k345", b"v345");
    cluster.clear_send_filters();

    must_get_equal(&cluster.get_engine(4), b"k345", b"v345");
}

/// Test whether a isolated peer can recover when two other regions merge to its region
#[test]
fn test_merge_cascade_merge_isolated() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let mut region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    region = pd_client.get_region(b"k2").unwrap();
    cluster.must_split(&region, b"k3");

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_put(b"k3", b"v3");

    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k2").unwrap();
    let r3 = pd_client.get_region(b"k3").unwrap();

    let r1_on_store1 = find_peer(&r1, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r1.get_id(), r1_on_store1);
    let r2_on_store2 = find_peer(&r2, 2).unwrap().to_owned();
    cluster.must_transfer_leader(r2.get_id(), r2_on_store2);
    let r3_on_store1 = find_peer(&r3, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r3.get_id(), r3_on_store1);

    // Wait will all followers respond their progress.
    thread::sleep(Duration::from_millis(100));

    cluster.add_send_filter(IsolationFilterFactory::new(3));

    // r1, r3 both merge to r2
    pd_client.must_merge(r1.get_id(), r2.get_id());
    pd_client.must_merge(r3.get_id(), r2.get_id());

    cluster.must_put(b"k4", b"v4");

    cluster.clear_send_filters();

    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

// Test if a learner can be destroyed properly when it's isolated and removed by conf change
// before its region merge to another region
#[test]
fn test_merge_isolated_not_in_merge_learner() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();

    pd_client.must_add_peer(left.get_id(), new_learner_peer(2, 2));
    // Ensure this learner exists
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.stop_node(2);

    pd_client.must_remove_peer(left.get_id(), new_learner_peer(2, 2));

    pd_client.must_add_peer(left.get_id(), new_peer(3, 3));
    pd_client.must_remove_peer(left.get_id(), left_on_store1);

    pd_client.must_add_peer(right.get_id(), new_peer(3, 4));
    pd_client.must_remove_peer(right.get_id(), right_on_store1);

    pd_client.must_merge(left.get_id(), right.get_id());
    // Add a new learner on store 2 to trigger peer 2 send check-stale-peer msg to other peers
    pd_client.must_add_peer(right.get_id(), new_learner_peer(2, 5));

    cluster.must_put(b"k123", b"v123");

    cluster.run_node(2).unwrap();
    // We can see if the old peer 2 is destroyed
    must_get_equal(&cluster.get_engine(2), b"k123", b"v123");
}

// Test if a learner can be destroyed properly when it's isolated and removed by conf change
// before another region merge to its region
#[test]
fn test_merge_isolated_stale_learner() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;
    // Do not rely on pd to remove stale peer
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration::hours(2);
    cluster.cfg.raft_store.abnormal_leader_missing_duration = ReadableDuration::minutes(10);
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration::minutes(5);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    let mut region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    pd_client.must_add_peer(left.get_id(), new_learner_peer(2, 2));
    // Ensure this learner exists
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.stop_node(2);

    pd_client.must_remove_peer(left.get_id(), new_learner_peer(2, 2));

    pd_client.must_merge(right.get_id(), left.get_id());

    region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let new_left = pd_client.get_region(b"k1").unwrap();
    assert_ne!(left.get_id(), new_left.get_id());
    // Add a new learner on store 2 to trigger peer 2 send check-stale-peer msg to other peers
    pd_client.must_add_peer(new_left.get_id(), new_learner_peer(2, 5));
    cluster.must_put(b"k123", b"v123");

    cluster.run_node(2).unwrap();
    // We can see if the old peer 2 is destroyed
    must_get_equal(&cluster.get_engine(2), b"k123", b"v123");
}

/// Test if a learner can be destroyed properly in such conditions as follows
/// 1. A peer is isolated
/// 2. Be the last removed peer in its peer list
/// 3. Then its region merges to another region.
/// 4. Isolation disappears
#[test]
fn test_merge_isolated_not_in_merge_learner_2() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    let right_on_store1 = find_peer(&right, 1).unwrap().to_owned();

    pd_client.must_add_peer(left.get_id(), new_learner_peer(2, 2));
    // Ensure this learner exists
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.stop_node(2);

    pd_client.must_add_peer(left.get_id(), new_peer(3, 3));
    pd_client.must_remove_peer(left.get_id(), left_on_store1);

    pd_client.must_add_peer(right.get_id(), new_peer(3, 4));
    pd_client.must_remove_peer(right.get_id(), right_on_store1);
    // The peer list of peer 2 is (1001, 1), (2, 2)
    pd_client.must_remove_peer(left.get_id(), new_learner_peer(2, 2));

    pd_client.must_merge(left.get_id(), right.get_id());

    cluster.run_node(2).unwrap();
    // When the abnormal leader missing duration has passed, the check-stale-peer msg will be sent to peer 1001.
    // After that, a new peer list will be returned (2, 2) (3, 3).
    // Then peer 2 sends the check-stale-peer msg to peer 3 and it will get a tombstone response.
    // Finally peer 2 will be destroyed.
    must_get_none(&cluster.get_engine(2), b"k1");
}

/// Test if a peer can be removed if its target peer has been removed and doesn't apply the
/// CommitMerge log.
#[test]
fn test_merge_remove_target_peer_isolated() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    let mut region = pd_client.get_region(b"k1").unwrap();
    pd_client.must_add_peer(region.get_id(), new_peer(2, 2));
    pd_client.must_add_peer(region.get_id(), new_peer(3, 3));

    cluster.must_split(&region, b"k2");
    region = pd_client.get_region(b"k2").unwrap();
    cluster.must_split(&region, b"k3");

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k2").unwrap();
    let r3 = pd_client.get_region(b"k3").unwrap();

    let r1_on_store1 = find_peer(&r1, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r1.get_id(), r1_on_store1);
    let r2_on_store2 = find_peer(&r2, 2).unwrap().to_owned();
    cluster.must_transfer_leader(r2.get_id(), r2_on_store2);

    for i in 1..4 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    for i in 1..4 {
        must_get_equal(&cluster.get_engine(3), format!("k{}", i).as_bytes(), b"v1");
    }

    cluster.add_send_filter(IsolationFilterFactory::new(3));
    // Make region r2's epoch > r2 peer on store 3.
    // r2 peer on store 3 will be removed whose epoch is staler than the epoch when r1 merge to r2.
    pd_client.must_add_peer(r2.get_id(), new_peer(4, 4));
    pd_client.must_remove_peer(r2.get_id(), new_peer(4, 4));

    let r2_on_store3 = find_peer(&r2, 3).unwrap().to_owned();
    let r3_on_store3 = find_peer(&r3, 3).unwrap().to_owned();

    pd_client.must_merge(r1.get_id(), r2.get_id());

    pd_client.must_remove_peer(r2.get_id(), r2_on_store3);
    pd_client.must_remove_peer(r3.get_id(), r3_on_store3);

    pd_client.must_merge(r2.get_id(), r3.get_id());

    cluster.clear_send_filters();

    for i in 1..4 {
        must_get_none(&cluster.get_engine(3), format!("k{}", i).as_bytes());
    }
}

#[test]
fn test_sync_max_ts_after_region_merge() {
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

    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    let storage = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let wait_for_synced = |cluster: &mut Cluster<ServerCluster>| {
        let region_id = right.get_id();
        let leader = cluster.leader_of_region(region_id).unwrap();
        let epoch = cluster.get_region_epoch(region_id);
        let mut ctx = Context::default();
        ctx.set_region_id(region_id);
        ctx.set_peer(leader);
        ctx.set_region_epoch(epoch);
        let snap_ctx = SnapContext {
            pb_ctx: &ctx,
            ..Default::default()
        };
        let snapshot = storage.snapshot(snap_ctx).unwrap();
        let txn_ext = snapshot.txn_ext.clone().unwrap();
        for retry in 0..10 {
            if txn_ext.is_max_ts_synced() {
                break;
            }
            thread::sleep(Duration::from_millis(1 << retry));
        }
        assert!(snapshot.ext().is_max_ts_synced());
    };

    wait_for_synced(&mut cluster);
    let max_ts = cm.max_ts();

    cluster.pd_client.trigger_tso_failure();
    // Merge left to right
    cluster.pd_client.must_merge(left.get_id(), right.get_id());

    wait_for_synced(&mut cluster);
    let new_max_ts = cm.max_ts();
    assert!(new_max_ts > max_ts);
}

/// If a follower is demoted by a snapshot, its meta will be changed. The case is to ensure
/// asserts in code can tolerate the change.
#[test]
fn test_merge_snapshot_demote() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    configure_for_snapshot(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    let region = pd_client.get_region(b"k1").unwrap();
    pd_client.must_add_peer(region.get_id(), new_peer(2, 2));
    pd_client.must_add_peer(region.get_id(), new_peer(3, 3));

    cluster.must_split(&region, b"k2");

    let r1 = pd_client.get_region(b"k1").unwrap();
    let r2 = pd_client.get_region(b"k3").unwrap();

    let r2_on_store1 = find_peer(&r2, 1).unwrap().to_owned();
    cluster.must_transfer_leader(r2.get_id(), r2_on_store1);

    // So r2 on store 3 will lag behind.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r2.get_id(), 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    let last_index = cluster.raft_local_state(r2.get_id(), 1).get_last_index();
    for i in 1..4 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    pd_client.must_merge(r1.get_id(), r2.get_id());
    cluster.wait_log_truncated(r2.get_id(), 1, last_index + 1);

    // Now demote r2 on store 3 to learner, so its meta will be changed.
    let r2_on_store3 = find_peer(&r2, 3).unwrap().to_owned();
    pd_client.must_joint_confchange(
        r2.get_id(),
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(4, 4)),
            (
                ConfChangeType::AddLearnerNode,
                new_learner_peer(3, r2_on_store3.get_id()),
            ),
        ],
    );

    cluster.clear_send_filters();
    // Now snapshot should be generated and merge on store 3 should be aborted.
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

#[test]
fn test_propose_in_memory_pessimistic_locks() {
    let mut cluster = new_server_cluster(0, 2);
    configure_for_merge(&mut cluster);
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

    // Transfer the leader of the right region to store 2. The leaders of source and target
    // regions don't need to be on the same store.
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
    };
    assert!(
        txn_ext
            .pessimistic_locks
            .write()
            .insert(vec![(Key::from_raw(b"k1"), l1.clone())])
            .is_ok()
    );

    // Insert lock l2 into the right region
    let snapshot = cluster.must_get_snapshot_of_region(right.id);
    let txn_ext = snapshot.txn_ext.unwrap();
    let l2 = PessimisticLock {
        primary: b"k3".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
    };
    assert!(
        txn_ext
            .pessimistic_locks
            .write()
            .insert(vec![(Key::from_raw(b"k3"), l2.clone())])
            .is_ok()
    );

    // Merge left region into the right region
    pd_client.must_merge(left.id, right.id);

    // After the left region is merged into the right region, its pessimistic locks should be
    // proposed and applied to the storage.
    let snapshot = cluster.must_get_snapshot_of_region(right.id);
    let value = snapshot
        .get_cf(CF_LOCK, &Key::from_raw(b"k1"))
        .unwrap()
        .unwrap();
    assert_eq!(value, l1.into_lock().to_bytes());

    // The lock belonging to the target region should remain unchanged.
    let snapshot = cluster.must_get_snapshot_of_region(right.id);
    let txn_ext = snapshot.txn_ext.unwrap();
    assert_eq!(
        txn_ext.pessimistic_locks.read().get(&Key::from_raw(b"k3")),
        Some(&(l2, false))
    );
}

#[test]
fn test_merge_pessimistic_locks_when_gap_is_too_large() {
    let mut cluster = new_server_cluster(0, 2);
    configure_for_merge(&mut cluster);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    // Set raft_entry_max_size to 64 KiB. We will try to make the gap larger than the limit later.
    cluster.cfg.raft_store.raft_entry_max_size = ReadableSize::kb(64);
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

    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(
        left.get_id(),
        2,
    )));

    let large_bytes = vec![b'v'; 32 << 10]; // 32 KiB
    // 4 * 32 KiB = 128 KiB > raft_entry_max_size
    for _ in 0..4 {
        cluster.async_put(b"k1", &large_bytes).unwrap();
    }

    cluster.merge_region(left.id, right.id, Callback::None);
    thread::sleep(Duration::from_millis(150));

    // The gap is too large, so the previous merge should fail. And this new put request
    // should be allowed.
    let res = cluster.async_put(b"k1", b"new_val").unwrap();

    cluster.clear_send_filters();
    assert!(res.recv().is_ok());

    assert_eq!(cluster.must_get(b"k1").unwrap(), b"new_val");
}

#[test]
fn test_merge_pessimistic_locks_repeated_merge() {
    let mut cluster = new_server_cluster(0, 2);
    configure_for_merge(&mut cluster);
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

    let snapshot = cluster.must_get_snapshot_of_region(left.id);
    let txn_ext = snapshot.ext().get_txn_ext().unwrap().clone();
    let lock = PessimisticLock {
        primary: b"k1".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
    };
    assert!(
        txn_ext
            .pessimistic_locks
            .write()
            .insert(vec![(Key::from_raw(b"k1"), lock.clone())])
            .is_ok()
    );

    // Filter MsgAppend, so the proposed PrepareMerge will not succeed
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.id, 2)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Recv),
    ));
    cluster.merge_region(left.id, right.id, Callback::None);
    cluster.merge_region(left.id, right.id, Callback::None);
    thread::sleep(Duration::from_millis(150));

    // After that, the pessimistic locks status should remain in Merging state.
    // Failing to propose the second merge region will not revert the state
    assert_eq!(
        txn_ext.pessimistic_locks.read().status,
        LocksStatus::MergingRegion
    );

    cluster.clear_send_filters();
    pd_client.check_merged_timeout(left.id, Duration::from_secs(5));
    let snapshot = cluster.must_get_snapshot_of_region(right.id);
    let value = snapshot
        .get_cf(CF_LOCK, &Key::from_raw(b"k1"))
        .unwrap()
        .unwrap();
    assert_eq!(value, lock.into_lock().to_bytes());
}

/// Check if merge is cleaned up if the merge target is destroyed several times before it's ever
/// scheduled.
#[test]
fn test_node_merge_long_isolated() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    ignore_merge_target_integrity(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    cluster.must_transfer_leader(right.get_id(), new_peer(3, 3));
    let target_leader = peer_on_store(&left, 3);
    cluster.must_transfer_leader(left.get_id(), target_leader);
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");

    // So cluster becomes:
    //  left region: 1 I 2 3(leader)
    // right region: 1 I 2 3(leader)
    // I means isolation.
    cluster.add_send_filter(IsolationFilterFactory::new(1));
    pd_client.must_merge(left.get_id(), right.get_id());
    pd_client.must_remove_peer(right.get_id(), peer_on_store(&right, 1));
    // Split to make sure the range of new peer won't overlap with source.
    let right = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&right, b"k2");
    cluster.must_put(b"k4", b"v4");
    // Ensure the node is removed, so it will not catch up any logs but just destroy itself.
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
    must_get_equal(&cluster.get_engine(2), b"k4", b"v4");

    let filter = RegionPacketFilter::new(left.get_id(), 1);
    cluster.clear_send_filters();
    // Ensure source region will not take any actions.
    cluster.add_send_filter(CloneFilterFactory(filter));
    must_get_none(&cluster.get_engine(1), b"k3");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    // So new peer will not apply snapshot.
    let filter = RegionPacketFilter::new(right.get_id(), 1).msg_type(MessageType::MsgSnapshot);
    cluster.add_send_filter(CloneFilterFactory(filter));
    pd_client.must_add_peer(right.get_id(), new_peer(1, 1010));
    cluster.must_put(b"k5", b"v5");
    must_get_equal(&cluster.get_engine(2), b"k5", b"v5");
    must_get_none(&cluster.get_engine(1), b"k3");

    // Now peer(1, 1010) should probably created in memory but not persisted.
    pd_client.must_remove_peer(right.get_id(), new_peer(1, 1010));
    cluster.wait_tombstone(right.get_id(), new_peer(1, 1010), true);
    cluster.clear_send_filters();
    // Source peer should discover it's impossible to proceed and cleanup itself.
    must_get_none(&cluster.get_engine(1), b"k1");
}

#[test]
fn test_stale_message_after_merge() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_merge(&mut cluster);
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

    pd_client.must_remove_peer(left.get_id(), find_peer(&left, 3).unwrap().to_owned());
    pd_client.must_add_peer(left.get_id(), new_peer(3, 1004));
    pd_client.must_merge(left.get_id(), right.get_id());

    // Such stale message can be sent due to network error, consider the following example:
    // 1. Store 1 and Store 3 can't reach each other, so peer 1003 start election and send `RequestVote`
    //    message to peer 1001, and fail due to network error, but this message is keep backoff-retry to send out
    // 2. Peer 1002 become the new leader and remove peer 1003 and add peer 1004 on store 3, then the region is
    //    merged into other region, the merge can success because peer 1002 can reach both peer 1001 and peer 1004
    // 3. Network recover, so peer 1003's `RequestVote` message is sent to peer 1001 after it is merged
    //
    // the backoff-retry of a stale message is hard to simulated in test, so here just send this stale message directly
    let mut raft_msg = RaftMessage::default();
    raft_msg.set_region_id(left.get_id());
    raft_msg.set_from_peer(find_peer(&left, 3).unwrap().to_owned());
    raft_msg.set_to_peer(find_peer(&left, 1).unwrap().to_owned());
    raft_msg.set_region_epoch(left.get_region_epoch().to_owned());
    cluster.send_raft_msg(raft_msg).unwrap();

    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

/// Check whether merge should be prevented if follower may not have enough logs.
#[test]
fn test_prepare_merge_with_reset_matched() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let r = cluster.run_conf_change();
    pd_client.must_add_peer(r, new_peer(2, 2));
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    pd_client.add_peer(r, new_peer(3, 3));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");
    thread::sleep(Duration::from_millis(10));
    // So leader will replicate next command but can't know whether follower (2, 2)
    // also commits the command. Supposing the index is i0.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(left.get_id(), 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppendResponse)
            .allow(1),
    ));
    cluster.must_put(b"k11", b"v11");
    cluster.clear_send_filters();
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    // So peer (3, 3) only have logs after i0.
    must_get_equal(&cluster.get_engine(3), b"k11", b"v11");
    // Clear match information.
    let left_on_store3 = find_peer(&left, 3).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store3);
    let left_on_store1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_on_store1);
    let res = cluster.try_merge(left.get_id(), right.get_id());
    // Now leader still knows peer(2, 2) has committed i0 - 1, so the min_match will
    // become i0 - 1. But i0 - 1 is not a safe index as peer(3, 3) starts from i0 + 1.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_send_filters();
    // Now leader should replicate more logs and figure out a safe index.
    pd_client.must_merge(left.get_id(), right.get_id());
}

/// Check if prepare merge min index is chosen correctly even if all match indexes are
/// correct.
#[test]
fn test_prepare_merge_with_5_nodes_snapshot() {
    let mut cluster = new_server_cluster(0, 5);
    configure_for_merge(&mut cluster);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.run();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k3");

    let peer_on_store1 = find_peer(&left, 1).unwrap().clone();
    cluster.must_transfer_leader(left.get_id(), peer_on_store1);
    must_get_equal(&cluster.get_engine(5), b"k1", b"v1");
    let peer_on_store5 = find_peer(&left, 5).unwrap().clone();
    pd_client.must_remove_peer(left.get_id(), peer_on_store5);
    must_get_none(&cluster.get_engine(5), b"k1");
    cluster.add_send_filter(IsolationFilterFactory::new(5));
    pd_client.add_peer(left.get_id(), new_peer(5, 16));

    // Make sure there will be no admin entries after min_matched.
    for (k, v) in &[(b"k11", b"v11"), (b"k12", b"v12")] {
        cluster.must_put(*k, *v);
        must_get_equal(&cluster.get_engine(4), *k, *v);
    }
    cluster.add_send_filter(IsolationFilterFactory::new(4));
    // So index of peer 4 becomes min_matched.
    cluster.must_put(b"k13", b"v13");
    must_get_equal(&cluster.get_engine(1), b"k13", b"v13");

    // Only remove send filter on store 5.
    cluster.clear_send_filters();
    cluster.add_send_filter(IsolationFilterFactory::new(4));
    must_get_equal(&cluster.get_engine(5), b"k13", b"v13");
    let res = cluster.try_merge(left.get_id(), right.get_id());
    // min_matched from peer 4 is beyond the first index of peer 5, it should not be chosen
    // for prepare merge.
    assert!(res.get_header().has_error(), "{:?}", res);
    cluster.clear_send_filters();
    // Now leader should replicate more logs and figure out a safe index.
    pd_client.must_merge(left.get_id(), right.get_id());
}

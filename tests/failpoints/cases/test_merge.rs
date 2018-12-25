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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::*;

use fail;
use futures::Future;

use kvproto::raft_serverpb::{PeerState, RegionLocalState};
use raft::eraftpb::MessageType;

use test_raftstore::*;
use tikv::pd::PdClient;
use tikv::raftstore::store::keys;
use tikv::raftstore::store::Peekable;
use tikv::storage::CF_RAFT;
use tikv::util::config::*;
use tikv::util::HandyRwLock;

/// Test if merge is rollback as expected.
#[test]
fn test_node_merge_rollback() {
    let _guard = ::setup();
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
    cluster.try_merge(region.get_id(), target_region.get_id());

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
            .get_msg_cf(CF_RAFT, &state_key)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_state(), PeerState::Normal);
        assert_eq!(*state.get_region(), region);
    }

    pd_client.must_remove_peer(right.get_id(), new_peer(3, 5));
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    let target_region = pd_client.get_region(b"k1").unwrap();
    cluster.try_merge(region.get_id(), target_region.get_id());
    let mut region = pd_client.get_region(b"k1").unwrap();

    // Split to trigger rollback.
    cluster.must_split(&right, b"k3");
    fail::remove(schedule_merge_fp);
    // Wait till rollback.
    cluster.must_put(b"k12", b"v12");

    // After premerge and rollback, version becomes 4 + 2 = 6;
    region.mut_region_epoch().set_version(4);
    for i in 1..3 {
        must_get_equal(&cluster.get_engine(i), b"k12", b"v12");
        let state_key = keys::region_state_key(region.get_id());
        let state: RegionLocalState = cluster
            .get_engine(i)
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
    let _guard = ::setup();
    // ::util::init_log();
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

    cluster.try_merge(left.get_id(), right.get_id());
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
    cluster.start();

    // Wait till merge is finished.
    let timer = Instant::now();
    loop {
        if pd_client
            .get_region_by_id(left.get_id())
            .wait()
            .unwrap()
            .is_none()
        {
            break;
        }

        if timer.elapsed() > Duration::from_secs(5) {
            panic!("region still not merged after 5 secs");
        }
        sleep_ms(10);
    }

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
    cluster.start();
    must_get_none(&cluster.get_engine(3), b"k1");
    must_get_none(&cluster.get_engine(3), b"k3");
}

/// Test if merging state will be removed after accepting a snapshot.
#[test]
fn test_node_merge_recover_snapshot() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.raft_log_gc_threshold = 12;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 12;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k3").unwrap();
    let target_region = pd_client.get_region(b"k1").unwrap();

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    cluster.try_merge(region.get_id(), target_region.get_id());

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

// To be fixed
//
// Test if a merge handled properly when there are two different snapshots of one region arrive
// in different raftstore tick.
// #[test]
// fn test_node_merge_multiple_snapshots_not_together() {
//     test_node_merge_multiple_snapshots(false)
// }

fn test_node_merge_multiple_snapshots(together: bool) {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_merge(&mut cluster);
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
        box LeadingDuplicatedSnapshotFilter::new(Arc::new(AtomicBool::new(false)), together),
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
    // Then the old and new snapshot messages are received and handled in one tick,
    // so `pending_cross_snap` may updated improperly and make merge source peer destory itself.
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
    // the peer of left region on store 3 is stale, and then the peer will check
    // `pending_cross_snap`
    thread::sleep(Duration::from_millis(300));

    cluster.must_put(b"k9", b"v9");
    // let follower can reach the new log, then commit merge
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(3), b"k9", b"v9");
}

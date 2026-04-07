// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{CF_RAFT, Peekable, RaftEngineReadOnly};
use futures::executor::block_on;
use pd_client::PdClient;
use raftstore::store::{INIT_EPOCH_VER, RAFT_INIT_LOG_INDEX};
use tikv_util::store::new_peer;
use txn_types::{Key, TimeStamp};

use crate::cluster::{Cluster, split_helper::split_region};

#[test]
fn test_split() {
    let mut cluster = Cluster::default();
    let store_id = cluster.node(0).id();
    let raft_engine = cluster.node(0).running_state().unwrap().raft_engine.clone();
    let router = &mut cluster.routers[0];

    let region_2 = 2;
    let region = router.region_detail(region_2);
    let peer = region.get_peers()[0].clone();
    router.wait_applied_to_current_term(region_2, Duration::from_secs(3));

    // Region 2 ["", ""]
    //   -> Region 2    ["", "k22"]
    //      Region 1000 ["k22", ""] peer(1, 10)
    let region_state = raft_engine
        .get_region_state(region_2, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    let (left, mut right) = split_region(
        router,
        region,
        peer.clone(),
        1000,
        new_peer(store_id, 10),
        Some(b"k11"),
        Some(b"k33"),
        b"k22",
        b"k22",
        false,
    );
    let region_state = raft_engine
        .get_region_state(region_2, u64::MAX)
        .unwrap()
        .unwrap();
    assert_ne!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    assert_eq!(
        region_state.get_region().get_region_epoch().get_version(),
        INIT_EPOCH_VER + 1
    );
    let region_state0 = raft_engine
        .get_region_state(region_2, region_state.get_tablet_index())
        .unwrap()
        .unwrap();
    assert_eq!(region_state, region_state0);
    let flushed_index = raft_engine
        .get_flushed_index(region_2, CF_RAFT)
        .unwrap()
        .unwrap();
    assert!(
        flushed_index >= region_state.get_tablet_index(),
        "{flushed_index} >= {}",
        region_state.get_tablet_index()
    );

    // Region 2 ["", "k22"]
    //   -> Region 2    ["", "k11"]
    //      Region 1001 ["k11", "k22"] peer(1, 11)
    let _ = split_region(
        router,
        left,
        peer,
        1001,
        new_peer(store_id, 11),
        Some(b"k00"),
        Some(b"k11"),
        b"k11",
        b"k11",
        false,
    );
    let region_state = raft_engine
        .get_region_state(region_2, u64::MAX)
        .unwrap()
        .unwrap();
    assert_ne!(
        region_state.get_tablet_index(),
        region_state0.get_tablet_index()
    );
    assert_eq!(
        region_state.get_region().get_region_epoch().get_version(),
        INIT_EPOCH_VER + 2
    );
    let region_state1 = raft_engine
        .get_region_state(region_2, region_state.get_tablet_index())
        .unwrap()
        .unwrap();
    assert_eq!(region_state, region_state1);
    let flushed_index = raft_engine
        .get_flushed_index(region_2, CF_RAFT)
        .unwrap()
        .unwrap();
    assert!(
        flushed_index >= region_state.get_tablet_index(),
        "{flushed_index} >= {}",
        region_state.get_tablet_index()
    );

    // Region 1000 ["k22", ""] peer(1, 10)
    //   -> Region 1000 ["k22", "k33"] peer(1, 10)
    //      Region 1002 ["k33", ""]    peer(1, 12)
    let region_1000 = 1000;
    let region_state = raft_engine
        .get_region_state(region_1000, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    right = split_region(
        router,
        right,
        new_peer(store_id, 10),
        1002,
        new_peer(store_id, 12),
        Some(b"k22"),
        Some(b"k33"),
        b"k33",
        b"k33",
        false,
    )
    .1;
    let region_state = raft_engine
        .get_region_state(region_1000, u64::MAX)
        .unwrap()
        .unwrap();
    assert_ne!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    assert_eq!(
        region_state.get_region().get_region_epoch().get_version(),
        INIT_EPOCH_VER + 2
    );
    let region_state2 = raft_engine
        .get_region_state(region_1000, region_state.get_tablet_index())
        .unwrap()
        .unwrap();
    assert_eq!(region_state, region_state2);
    let flushed_index = raft_engine
        .get_flushed_index(region_1000, CF_RAFT)
        .unwrap()
        .unwrap();
    assert!(
        flushed_index >= region_state.get_tablet_index(),
        "{flushed_index} >= {}",
        region_state.get_tablet_index()
    );

    // 1002 -> 1002, 1003
    let split_key = Key::from_raw(b"k44").append_ts(TimeStamp::zero());
    let actual_split_key = split_key.clone().truncate_ts().unwrap();
    split_region(
        router,
        right,
        new_peer(store_id, 12),
        1003,
        new_peer(store_id, 13),
        Some(b"k33"),
        Some(b"k55"),
        split_key.as_encoded(),
        actual_split_key.as_encoded(),
        false,
    );

    // Split should survive restart.
    drop(raft_engine);
    cluster.restart(0);
    let region_and_key = vec![
        (2, b"k00"),
        (1000, b"k22"),
        (1001, b"k11"),
        (1002, b"k33"),
        (1003, b"k55"),
    ];
    for (region_id, key) in region_and_key {
        let snapshot = cluster.routers[0].stale_snapshot(region_id);
        assert!(
            snapshot.get_value(key).unwrap().is_some(),
            "{} {:?}",
            region_id,
            key
        );
    }
}

// TODO: test split race with
// - created peer
// - created peer with pending snapshot
// - created peer with persisting snapshot
// - created peer with persisted snapshot

/// Test that after split, the new region correctly reports its leader to PD.
/// This verifies that the heartbeat is sent after the leader election completes
/// (from on_role_changed), not prematurely during post_split_init when the peer
/// may still be a Candidate and pending_peers would be incorrectly reported.
#[test]
fn test_split_region_heartbeat_to_pd() {
    let cluster = Cluster::with_node_count(1, None);
    let store_id = cluster.node(0).id();
    let router = &cluster.routers[0];

    let region_id = 2;
    let region = router.region_detail(region_id);
    let peer = region.get_peers()[0].clone();
    router.wait_applied_to_current_term(region_id, Duration::from_secs(3));

    // Split region 2 into region 2 ["", "k22"] and region 1000 ["k22", ""]
    let split_region_id = 1000;
    let (left, right) = split_region(
        router,
        region,
        peer,
        split_region_id,
        new_peer(store_id, 10),
        Some(b"k11"),
        Some(b"k33"),
        b"k22",
        b"k22",
        false,
    );

    // Verify both regions have leaders reported to PD.
    // The split_region helper already waits 1 second for split to complete,
    // which is enough time for leader election and heartbeat.
    for (rid, expected_peer_id) in [(region_id, peer.get_id()), (split_region_id, 10)] {
        let mut found = false;
        for _ in 0..10 {
            let resp = block_on(cluster.node(0).pd_client().get_region_leader_by_id(rid)).unwrap();
            if let Some((r, p)) = resp {
                assert_eq!(r.get_id(), rid);
                assert_eq!(p.get_id(), expected_peer_id);
                assert_eq!(p.get_store_id(), store_id);
                found = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        assert!(found, "region {} should have leader reported to PD", rid);
    }

    // Verify region boundaries are correct
    assert_eq!(left.get_end_key(), b"k22");
    assert_eq!(right.get_start_key(), b"k22");
}

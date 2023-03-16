// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{Peekable, RaftEngineReadOnly, CF_RAFT};
use raftstore::store::{INIT_EPOCH_VER, RAFT_INIT_LOG_INDEX};
use tikv_util::store::new_peer;
use txn_types::{Key, TimeStamp};

use crate::cluster::{split_helper::split_region, Cluster};

#[test]
fn test_split() {
    let mut cluster = Cluster::default();
    let store_id = cluster.node(0).id();
    let raft_engine = cluster.node(0).running_state().unwrap().raft_engine.clone();
    let router = &mut cluster.routers[0];
    // let factory = cluster.node(0).tablet_factory();

    let region_id = 2;
    let peer = new_peer(store_id, 3);
    let region = router.region_detail(region_id);
    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    // Region 2 ["", ""] peer(1, 3)
    //   -> Region 2    ["", "k22"] peer(1, 3)
    //      Region 1000 ["k22", ""] peer(1, 10)
    let region_state = raft_engine.get_region_state(2, u64::MAX).unwrap().unwrap();
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
    let region_state = raft_engine.get_region_state(2, u64::MAX).unwrap().unwrap();
    assert_ne!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    assert_eq!(
        region_state.get_region().get_region_epoch().get_version(),
        INIT_EPOCH_VER + 1
    );
    let region_state0 = raft_engine
        .get_region_state(2, region_state.get_tablet_index())
        .unwrap()
        .unwrap();
    assert_eq!(region_state, region_state0);
    let flushed_index = raft_engine.get_flushed_index(2, CF_RAFT).unwrap().unwrap();
    assert!(
        flushed_index >= region_state.get_tablet_index(),
        "{flushed_index} >= {}",
        region_state.get_tablet_index()
    );

    // Region 2 ["", "k22"] peer(1, 3)
    //   -> Region 2    ["", "k11"]    peer(1, 3)
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
    let region_state = raft_engine.get_region_state(2, u64::MAX).unwrap().unwrap();
    assert_ne!(
        region_state.get_tablet_index(),
        region_state0.get_tablet_index()
    );
    assert_eq!(
        region_state.get_region().get_region_epoch().get_version(),
        INIT_EPOCH_VER + 2
    );
    let region_state1 = raft_engine
        .get_region_state(2, region_state.get_tablet_index())
        .unwrap()
        .unwrap();
    assert_eq!(region_state, region_state1);
    let flushed_index = raft_engine.get_flushed_index(2, CF_RAFT).unwrap().unwrap();
    assert!(
        flushed_index >= region_state.get_tablet_index(),
        "{flushed_index} >= {}",
        region_state.get_tablet_index()
    );

    // Region 1000 ["k22", ""] peer(1, 10)
    //   -> Region 1000 ["k22", "k33"] peer(1, 10)
    //      Region 1002 ["k33", ""]    peer(1, 12)
    let region_state = raft_engine
        .get_region_state(1000, u64::MAX)
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
        .get_region_state(1000, u64::MAX)
        .unwrap()
        .unwrap();
    assert_ne!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    assert_eq!(
        region_state.get_region().get_region_epoch().get_version(),
        INIT_EPOCH_VER + 2
    );
    let region_state2 = raft_engine
        .get_region_state(1000, region_state.get_tablet_index())
        .unwrap()
        .unwrap();
    assert_eq!(region_state, region_state2);
    let flushed_index = raft_engine.get_flushed_index(2, CF_RAFT).unwrap().unwrap();
    assert!(
        flushed_index >= region_state.get_tablet_index(),
        "{flushed_index} >= {}",
        region_state.get_tablet_index()
    );

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

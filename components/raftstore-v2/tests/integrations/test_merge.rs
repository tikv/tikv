// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{Peekable, RaftEngineReadOnly};
use kvproto::metapb::{Peer, Region};
use raftstore::store::RAFT_INIT_LOG_INDEX;
use tikv_util::store::new_peer;

use crate::cluster::{merge_helper::merge_region, split_helper::split_region, Cluster, TestRouter};

#[test]
fn test_merge() {
    let mut cluster = Cluster::default();
    let store_id = cluster.node(0).id();
    let raft_engine = cluster.node(0).running_state().unwrap().raft_engine.clone();
    let router = &mut cluster.routers[0];

    let do_split =
        |r: &mut TestRouter, region: Region, peer: &Peer, v: u64| -> (Region, Region, Peer) {
            let rid = region.get_id();
            let old_region_state = raft_engine
                .get_region_state(rid, u64::MAX)
                .unwrap()
                .unwrap();
            let new_peer = new_peer(store_id, peer.get_id() + 1);
            let (lhs, rhs) = split_region(
                r,
                region,
                peer.clone(),
                rid + 1,
                new_peer.clone(),
                Some(format!("k{}{}", rid, v).as_bytes()),
                Some(format!("k{}{}", rid + 1, v).as_bytes()),
                format!("k{}", rid + 1).as_bytes(),
                format!("k{}", rid + 1).as_bytes(),
                false,
            );
            let region_state = raft_engine
                .get_region_state(rid, u64::MAX)
                .unwrap()
                .unwrap();
            assert!(region_state.get_tablet_index() > old_region_state.get_tablet_index());
            assert_eq!(
                region_state.get_region().get_region_epoch().get_version(),
                old_region_state
                    .get_region()
                    .get_region_epoch()
                    .get_version()
                    + 1,
            );
            let region_state = raft_engine
                .get_region_state(rid + 1, u64::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
            (lhs, rhs, new_peer)
        };

    let region_1 = router.region_detail(2);
    let peer_1 = region_1.get_peers()[0].clone();
    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    // Split into 6.
    let (region_1, region_2, peer_2) = do_split(router, region_1, &peer_1, 1);
    let (region_2, region_3, peer_3) = do_split(router, region_2, &peer_2, 2);
    let (region_3, region_4, peer_4) = do_split(router, region_3, &peer_3, 3);
    let (region_4, region_5, peer_5) = do_split(router, region_4, &peer_4, 4);
    let (region_5, region_6, peer_6) = do_split(router, region_5, &peer_5, 5);
    drop(raft_engine);
    // The last region version is smaller.
    for (i, v) in [1, 2, 3, 4, 5, 5].iter().enumerate() {
        let rid = region_1.get_id() + i as u64;
        let snapshot = router.stale_snapshot(rid);
        let key = format!("k{rid}{v}");
        assert!(
            snapshot.get_value(key.as_bytes()).unwrap().is_some(),
            "{} {:?}",
            rid,
            key
        );
    }

    let region_2 = merge_region(&cluster, 0, region_1.clone(), peer_1, region_2, true);
    {
        let snapshot = cluster.routers[0].stale_snapshot(region_2.get_id());
        let key = format!("k{}1", region_1.get_id());
        assert!(snapshot.get_value(key.as_bytes()).unwrap().is_some());
    }
    let region_5 = merge_region(&cluster, 0, region_6.clone(), peer_6, region_5, true);
    {
        let snapshot = cluster.routers[0].stale_snapshot(region_5.get_id());
        let key = format!("k{}5", region_6.get_id());
        assert!(snapshot.get_value(key.as_bytes()).unwrap().is_some());
    }
    let region_3 = merge_region(&cluster, 0, region_2, peer_2, region_3, true);
    let region_4 = merge_region(&cluster, 0, region_3, peer_3, region_4, true);
    let region_5 = merge_region(&cluster, 0, region_4, peer_4, region_5, true);

    cluster.restart(0);
    let snapshot = cluster.routers[0].stale_snapshot(region_5.get_id());
    for (i, v) in [1, 2, 3, 4, 5, 5].iter().enumerate() {
        let rid = region_1.get_id() + i as u64;
        let key = format!("k{rid}{v}");
        assert!(
            snapshot.get_value(key.as_bytes()).unwrap().is_some(),
            "{} {:?}",
            rid,
            key
        );
    }
}

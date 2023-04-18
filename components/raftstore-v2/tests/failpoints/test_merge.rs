// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::Peekable;
use tikv_util::store::new_peer;

use crate::cluster::{
    life_helper::assert_peer_not_exist, merge_helper::merge_region, split_helper::split_region,
    Cluster,
};

#[test]
fn test_source_and_target_both_replay() {
    let mut cluster = Cluster::default();
    let store_id = cluster.node(0).id();
    let router = &mut cluster.routers[0];

    let region_1 = router.region_detail(2);
    let peer_1 = region_1.get_peers()[0].clone();
    router.wait_applied_to_current_term(2, Duration::from_secs(3));
    let peer_2 = new_peer(store_id, peer_1.get_id() + 1);
    let region_1_id = region_1.get_id();
    let region_2_id = region_1_id + 1;
    let (region_1, region_2) = split_region(
        router,
        region_1,
        peer_1.clone(),
        region_2_id,
        peer_2,
        Some(format!("k{}k", region_1_id).as_bytes()),
        Some(format!("k{}k", region_2_id).as_bytes()),
        format!("k{}", region_2_id).as_bytes(),
        format!("k{}", region_2_id).as_bytes(),
        false,
    );

    {
        let _fp = fail::FailGuard::new("after_acquire_source_checkpoint", "1*return->off");
        merge_region(&cluster, 0, region_1, peer_1, region_2, false);
    }

    cluster.restart(0);
    let router = &mut cluster.routers[0];
    // Wait for replay.
    let mut retry = 0;
    while retry < 50 {
        // Read region 1 data from region 2.
        let snapshot = router.stale_snapshot(region_2_id);
        let key = format!("k{region_1_id}k");
        if let Ok(Some(_)) = snapshot.get_value(key.as_bytes()) {
            return;
        }
        retry += 1;
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("merge not replayed after 5s");
}

#[test]
fn test_source_destroy_before_target_apply() {
    let mut cluster = Cluster::default();
    let store_id = cluster.node(0).id();
    let router = &mut cluster.routers[0];

    let region_1 = router.region_detail(2);
    let peer_1 = region_1.get_peers()[0].clone();
    router.wait_applied_to_current_term(2, Duration::from_secs(3));
    let peer_2 = new_peer(store_id, peer_1.get_id() + 1);
    let region_1_id = region_1.get_id();
    let region_2_id = region_1_id + 1;
    let (region_1, region_2) = split_region(
        router,
        region_1,
        peer_1.clone(),
        region_2_id,
        peer_2,
        Some(format!("k{}k", region_1_id).as_bytes()),
        Some(format!("k{}k", region_2_id).as_bytes()),
        format!("k{}", region_2_id).as_bytes(),
        format!("k{}", region_2_id).as_bytes(),
        false,
    );

    {
        // Sending CatchUpLogs will make source destroy early (without waiting for
        // AckCommitMerge).
        let _fp1 = fail::FailGuard::new("force_send_catch_up_logs", "1*return->off");
        let _fp2 = fail::FailGuard::new("after_acquire_source_checkpoint", "1*return->off");
        merge_region(&cluster, 0, region_1, peer_1.clone(), region_2, false);
    }
    assert_peer_not_exist(region_1_id, peer_1.get_id(), &cluster.routers[0]);

    cluster.restart(0);
    let router = &mut cluster.routers[0];
    // Wait for replay.
    let mut retry = 0;
    while retry < 50 {
        // Read region 1 data from region 2.
        let snapshot = router.stale_snapshot(region_2_id);
        let key = format!("k{region_1_id}k");
        if let Ok(Some(_)) = snapshot.get_value(key.as_bytes()) {
            return;
        }
        retry += 1;
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("merge not replayed after 5s");
}

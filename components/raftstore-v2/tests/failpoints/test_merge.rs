// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc, Mutex},
    time::Duration,
};

use engine_traits::Peekable;
use raftstore_v2::router::{PeerMsg, PeerTick};
use tikv_util::{config::ReadableDuration, info, store::new_peer};

use crate::cluster::{
    life_helper::assert_peer_not_exist,
    merge_helper::merge_region,
    split_helper::{put, split_region},
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

#[test]
fn test_rollback() {
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
        peer_2.clone(),
        Some(format!("k{}k", region_1_id).as_bytes()),
        Some(format!("k{}k", region_2_id).as_bytes()),
        format!("k{}", region_2_id).as_bytes(),
        format!("k{}", region_2_id).as_bytes(),
        false,
    );

    let region_3_id = region_2_id + 1;
    let peer_3 = new_peer(store_id, peer_2.get_id() + 1);
    let router_clone = Mutex::new(cluster.routers[0].clone());
    let region_2_clone = region_2.clone();
    fail::cfg_callback("start_commit_merge", move || {
        split_region(
            &router_clone.lock().unwrap(),
            region_2_clone.clone(),
            peer_2.clone(),
            region_3_id,
            peer_3.clone(),
            Some(format!("k{}k", region_2_id).as_bytes()),
            Some(format!("k{}k", region_3_id).as_bytes()),
            format!("k{}", region_3_id).as_bytes(),
            format!("k{}", region_3_id).as_bytes(),
            false,
        );
        fail::remove("start_commit_merge");
    })
    .unwrap();
    merge_region(&cluster, 0, region_1, peer_1, region_2, false);

    let mut resp = Default::default();
    for _ in 0..10 {
        resp = put(
            &cluster.routers[0],
            region_1_id,
            format!("k{}k2", region_1_id).as_bytes(),
        );
        if !resp.get_header().has_error() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(!resp.get_header().has_error(), "{:?}", resp);
}

// Target is merging.
#[test]
fn test_merge_conflict_0() {
    let mut cluster = Cluster::with_configs(1, None, None, |cfg| {
        cfg.merge_check_tick_interval = ReadableDuration::millis(100);
    });
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
        peer_2.clone(),
        Some(format!("k{}k", region_1_id).as_bytes()),
        Some(format!("k{}k", region_2_id).as_bytes()),
        format!("k{}", region_2_id).as_bytes(),
        format!("k{}", region_2_id).as_bytes(),
        false,
    );

    let peer_3 = new_peer(store_id, peer_1.get_id() + 2);
    let region_3_id = region_2_id + 1;
    let (region_2, region_3) = split_region(
        router,
        region_2,
        peer_2.clone(),
        region_3_id,
        peer_3,
        Some(format!("k{}k", region_2_id).as_bytes()),
        Some(format!("k{}k", region_3_id).as_bytes()),
        format!("k{}", region_3_id).as_bytes(),
        format!("k{}", region_3_id).as_bytes(),
        false,
    );
    info!("regions: {:?}, {:?}, {:?}", region_1, region_2, region_3);

    // pause merge progress of 2+3.
    let fp = fail::FailGuard::new("apply_commit_merge", "pause");
    merge_region(
        &cluster,
        0,
        region_2.clone(),
        peer_2,
        region_3.clone(),
        false,
    );
    // start merging 1+2. it should be aborted.
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    fail::cfg_callback("apply_rollback_merge", move || {
        tx.lock().unwrap().send(()).unwrap();
    })
    .unwrap();
    let region_2 = cluster.routers[0].region_detail(region_2.get_id());
    merge_region(&cluster, 0, region_1, peer_1, region_2, false);
    drop(fp);
    // wait for rollback.
    rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
    fail::remove("apply_rollback_merge");

    // Check region 1 is not merged and can serve writes.
    let mut resp = Default::default();
    for _ in 0..10 {
        resp = put(
            &cluster.routers[0],
            region_1_id,
            format!("k{}k2", region_1_id).as_bytes(),
        );
        if !resp.get_header().has_error() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    // Ref https://github.com/tikv/yatp/issues/82, the nested future pool high_priority_pool can be
    // leaked. We must wait for apply to finish.
    cluster.routers[0].wait_applied_to_current_term(region_3.get_id(), Duration::from_secs(3));
}

// Target has been merged and destroyed.
#[test]
fn test_merge_conflict_1() {
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
        peer_2.clone(),
        Some(format!("k{}k", region_1_id).as_bytes()),
        Some(format!("k{}k", region_2_id).as_bytes()),
        format!("k{}", region_2_id).as_bytes(),
        format!("k{}", region_2_id).as_bytes(),
        false,
    );

    let peer_3 = new_peer(store_id, peer_1.get_id() + 2);
    let region_3_id = region_2_id + 1;
    let (region_2, region_3) = split_region(
        router,
        region_2,
        peer_2.clone(),
        region_3_id,
        peer_3,
        Some(format!("k{}k", region_2_id).as_bytes()),
        Some(format!("k{}k", region_3_id).as_bytes()),
        format!("k{}", region_3_id).as_bytes(),
        format!("k{}", region_3_id).as_bytes(),
        false,
    );

    // pause merge progress of 1+2.
    assert_eq!(region_1.get_id(), 2);
    let fp = fail::FailGuard::new("ask_target_peer_to_commit_merge_2", "return");
    merge_region(
        &cluster,
        0,
        region_1.clone(),
        peer_1,
        region_2.clone(),
        false,
    );
    // merge 2+3.
    merge_region(
        &cluster,
        0,
        region_2.clone(),
        peer_2.clone(),
        region_3,
        true,
    );
    assert_peer_not_exist(region_2.get_id(), peer_2.get_id(), &cluster.routers[0]);
    // resume merging 1+2. it should be aborted.
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    fail::cfg_callback("apply_rollback_merge", move || {
        tx.lock().unwrap().send(()).unwrap();
        fail::remove("apply_rollback_merge");
    })
    .unwrap();
    drop(fp);
    cluster.routers[0]
        .send(region_1.get_id(), PeerMsg::Tick(PeerTick::CheckMerge))
        .unwrap();
    // wait for rollback.
    rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();

    // Check region 1 is not merged and can serve writes.
    let mut resp = Default::default();
    for _ in 0..10 {
        resp = put(
            &cluster.routers[0],
            region_1_id,
            format!("k{}k2", region_1_id).as_bytes(),
        );
        if !resp.get_header().has_error() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(!resp.get_header().has_error(), "{:?}", resp);
}

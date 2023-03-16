// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{Peekable, CF_DEFAULT};
use futures::executor::block_on;
use raftstore_v2::{router::PeerMsg, SimpleWriteEncoder};
use tikv_util::store::new_peer;

use crate::cluster::{merge_helper::merge_region, split_helper::split_region, Cluster};

#[test]
fn test_restart_resume() {
    let mut cluster = Cluster::default();
    let store_id = cluster.node(0).id();
    let router = &mut cluster.routers[0];

    let region_1 = router.region_detail(2);
    let peer_1 = region_1.get_peers()[0].clone();
    router.wait_applied_to_current_term(2, Duration::from_secs(3));
    let peer_2 = new_peer(store_id, peer_1.get_id() + 1);
    let region_1_id = region_1.get_id();
    let (region_1, region_2) = split_region(
        router,
        region_1,
        peer_1.clone(),
        region_1_id + 1,
        peer_2,
        Some(format!("k{}k", region_1_id).as_bytes()),
        Some(format!("k{}k", region_1_id + 1).as_bytes()),
        format!("k{}", region_1_id + 1).as_bytes(),
        format!("k{}", region_1_id + 1).as_bytes(),
        false,
    );
    let region_2_id = region_2.get_id();

    let router2 = std::sync::Mutex::new(router.clone());
    fail::cfg_callback("apply_before_commit_merge", move || {
        // We must commit another entry to persist committed index.
        let router2 = router2.lock().unwrap();
        let header = Box::new(router2.new_request_for(region_2_id).take_header());
        let mut put = SimpleWriteEncoder::with_capacity(64);
        put.put(
            CF_DEFAULT,
            format!("k{}k", region_1_id + 1).as_bytes(),
            b"v1",
        );
        let (msg, mut sub) = PeerMsg::simple_write(header, put.encode());
        router2.send(region_1_id, msg).unwrap();
        block_on(sub.wait_committed());

        // Drop further raft writes, especially the persist of admin flushed.
        fail::cfg("raft_before_save_on_store_1", "return").unwrap()
    })
    .unwrap();

    let region_2 = merge_region(router, region_1, peer_1, region_2);
    let new_epoch = region_2.get_region_epoch();
    {
        // Read region 1 data from region 2.
        let snapshot = router.stale_snapshot(region_2_id);
        let key = format!("k{region_1_id}k");
        assert!(
            snapshot.get_value(key.as_bytes()).unwrap().is_some(),
            "{} {:?}",
            region_2_id,
            key
        );
    }

    // Restore raft writes during replay.
    fail::cfg_callback("apply_before_commit_merge", || {
        fail::remove("raft_before_save_on_store_1");
        fail::remove("apply_before_commit_merge");
    })
    .unwrap();
    cluster.restart(0);
    let router = &mut cluster.routers[0];
    // Wait for replay.
    let mut retry = 0;
    while router
        .new_request_for(region_2_id)
        .take_header()
        .get_region_epoch()
        != new_epoch
    {
        std::thread::sleep(Duration::from_millis(100));
        retry += 1;
        if retry > 50 {
            panic!("merge not replayed after 5s");
        }
    }
    {
        // Read region 1 data from region 2.
        let snapshot = router.stale_snapshot(region_2_id);
        let key = format!("k{region_1_id}k");
        assert!(
            snapshot.get_value(key.as_bytes()).unwrap().is_some(),
            "{} {:?}",
            region_2_id,
            key
        );
    }

    cluster.restart(0);
    let router = &mut cluster.routers[0];
    {
        // Read region 1 data from region 2.
        let snapshot = router.stale_snapshot(region_2_id);
        let key = format!("k{region_1_id}k");
        assert!(
            snapshot.get_value(key.as_bytes()).unwrap().is_some(),
            "{} {:?}",
            region_2_id,
            key
        );
    }
}

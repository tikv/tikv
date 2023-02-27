// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    thread,
    time::{Duration, Instant},
};

use engine_traits::{RaftEngineReadOnly, CF_DEFAULT};
use futures::executor::block_on;
use raftstore::store::RAFT_INIT_LOG_INDEX;
use raftstore_v2::{router::PeerMsg, SimpleWriteEncoder};

use crate::cluster::{split_helper::split_region, Cluster};

/// If a node is restarted after metadata is persisted before tablet is not
/// installed, it should resume install the tablet.
#[test]
fn test_restart_resume() {
    let mut cluster = Cluster::default();
    let raft_engine = cluster.node(0).running_state().unwrap().raft_engine.clone();
    let router = &mut cluster.routers[0];

    let region_id = 2;
    let region = router.region_detail(region_id);
    let peer = region.get_peers()[0].clone();
    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    let fp = "async_write_before_cb";
    fail::cfg(fp, "return").unwrap();

    let split_region_id = 1000;
    let mut new_peer = peer.clone();
    new_peer.set_id(1001);
    split_region(
        router,
        region,
        peer,
        split_region_id,
        new_peer,
        None,
        None,
        b"k11",
        b"k11",
        true,
    );

    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"k22", b"value");
    let header = Box::new(router.new_request_for(region_id).take_header());
    let (msg, mut sub) = PeerMsg::simple_write(header, put.encode());
    router.send(region_id, msg).unwrap();
    // Send a command to ensure split init is triggered.
    block_on(sub.wait_proposed());

    let region_state = raft_engine
        .get_region_state(split_region_id, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    let path = cluster
        .node(0)
        .tablet_registry()
        .tablet_path(split_region_id, RAFT_INIT_LOG_INDEX);
    assert!(!path.exists(), "{} should not exist", path.display());
    drop(raft_engine);

    cluster.restart(0);
    // If split is resumed, the tablet should be installed.
    assert!(
        path.exists(),
        "{} should exist after restart",
        path.display()
    );

    // Both region should be recovered correctly.
    let cases = vec![
        (split_region_id, b"k01", b"v01"),
        (region_id, b"k21", b"v21"),
    ];
    let router = &mut cluster.routers[0];
    let new_epoch = router
        .new_request_for(split_region_id)
        .take_header()
        .take_region_epoch();
    // Split will be resumed for region 2, not removing the fp will make write block
    // forever.
    fail::remove(fp);
    let timer = Instant::now();
    for (region_id, key, val) in cases {
        let mut put = SimpleWriteEncoder::with_capacity(64);
        put.put(CF_DEFAULT, key, val);
        let mut header = Box::new(router.new_request_for(region_id).take_header());
        while timer.elapsed() < Duration::from_secs(3) {
            // We need to wait till source peer replay split.
            if *header.get_region_epoch() != new_epoch {
                thread::sleep(Duration::from_millis(100));
                header = Box::new(router.new_request_for(region_id).take_header());
                continue;
            }
            break;
        }
        assert_eq!(*header.get_region_epoch(), new_epoch, "{:?}", header);
        let (msg, sub) = PeerMsg::simple_write(header, put.encode());
        router.send(region_id, msg).unwrap();
        // Send a command to ensure split init is triggered.
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
    }
}

// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use health_controller::types::LatencyInspector;
use raftstore::store::msg::StoreMsg as StoreMsgV1;
use raftstore_v2::router::StoreMsg as StoreMsgV2;
use test_raftstore::Simulator as S1;
use test_raftstore_v2::Simulator as S2;
use tikv_util::{time::Instant, HandyRwLock};

#[test]
fn test_region_detail() {
    let count = 5;
    let mut cluster = test_raftstore::new_server_cluster(0, count);
    cluster.run();

    let leader = cluster.leader_of_region(1).unwrap();
    let region_detail = cluster.region_detail(1, 1);
    assert!(region_detail.has_region());
    let region = region_detail.get_region();
    assert_eq!(region.get_id(), 1);
    assert!(region.get_start_key().is_empty());
    assert!(region.get_end_key().is_empty());
    assert_eq!(region.get_peers().len(), 5);
    let epoch = region.get_region_epoch();
    assert_eq!(epoch.get_conf_ver(), 1);
    assert_eq!(epoch.get_version(), 1);

    assert!(region_detail.has_leader());
    assert_eq!(region_detail.get_leader(), &leader);
}

#[test]
fn test_latency_inspect() {
    let mut cluster_v1 = test_raftstore::new_node_cluster(0, 1);
    cluster_v1.cfg.raft_store.store_io_pool_size = 2;
    cluster_v1.run();
    let mut cluster_v2 = test_raftstore_v2::new_node_cluster(0, 1);
    cluster_v2.run();
    let (router_v1, router_v2) = (
        cluster_v1.sim.wl().get_router(1).unwrap(),
        cluster_v2.sim.wl().get_router(1).unwrap(),
    );
    {
        // Test send LatencyInspect to V1.
        let (tx, rx) = std::sync::mpsc::sync_channel(10);
        let inspector = LatencyInspector::new(
            1,
            Box::new(move |_, duration| {
                let dur = duration.sum();
                tx.send(dur).unwrap();
            }),
        );
        let msg = StoreMsgV1::LatencyInspect {
            send_time: Instant::now(),
            inspector,
        };
        router_v1.send_control(msg).unwrap();
        rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
    }
    {
        // Test send LatencyInspect to V2.
        let (tx, rx) = std::sync::mpsc::sync_channel(10);
        let inspector = LatencyInspector::new(
            1,
            Box::new(move |_, duration| {
                let dur = duration.sum();
                tx.send(dur).unwrap();
            }),
        );
        let msg = StoreMsgV2::LatencyInspect {
            send_time: Instant::now(),
            inspector,
        };
        router_v2.send_control(msg).unwrap();
        rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
    }
}

#[test]
fn test_sync_latency_inspect() {
    let mut cluster = test_raftstore::new_node_cluster(0, 1);
    cluster.cfg.raft_store.store_io_pool_size = 0;
    cluster.run();
    let router = cluster.sim.wl().get_router(1).unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(10);
    let inspector = LatencyInspector::new(
        1,
        Box::new(move |_, duration| {
            let dur = duration.sum();
            tx.send(dur).unwrap();
        }),
    );
    let msg = StoreMsgV1::LatencyInspect {
        send_time: Instant::now(),
        inspector,
    };
    router.send_control(msg).unwrap();
    rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
}

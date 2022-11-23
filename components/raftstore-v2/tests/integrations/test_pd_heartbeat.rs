// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use kvproto::raft_cmdpb::{RaftCmdRequest, StatusCmdType};
use pd_client::PdClient;
use tikv_util::store::new_peer;

use crate::cluster::Cluster;

#[test]
fn test_region_heartbeat() {
    let region_id = 2;
    let cluster = Cluster::with_node_count(1, None);
    let router = cluster.router(0);

    // When there is only one peer, it should campaign immediately.
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionLeader);
    let res = router.query(region_id, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    assert_eq!(
        *status_resp.get_region_leader().get_leader(),
        new_peer(1, 3)
    );

    for _ in 0..5 {
        let resp = block_on(
            cluster
                .node(0)
                .pd_client()
                .get_region_leader_by_id(region_id),
        )
        .unwrap();
        if let Some((region, peer)) = resp {
            assert_eq!(region.get_id(), region_id);
            assert_eq!(peer.get_id(), 3);
            assert_eq!(peer.get_store_id(), 1);
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("failed to get region leader");
}

#[test]
fn test_store_heartbeat() {
    let cluster = Cluster::with_node_count(1, None);
    let store_id = cluster.node(0).id();
    for _ in 0..5 {
        let stats = block_on(cluster.node(0).pd_client().get_store_stats_async(store_id)).unwrap();
        if stats.get_start_time() > 0 {
            assert_ne!(stats.get_capacity(), 0);
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("failed to get store stats");
}

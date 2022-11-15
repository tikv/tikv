// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use kvproto::raft_cmdpb::{RaftCmdRequest, StatusCmdType};
use pd_client::PdClient;
use tikv_util::store::new_peer;

use crate::cluster::Cluster;

#[test]
fn test_region_heartbeat() {
    let region_id = 2;
    let cluster = Cluster::with_node_count(2, None);
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

    let (region, peer) = block_on(
        cluster
            .node(0)
            .pd_client()
            .get_region_leader_by_id(region_id),
    )
    .unwrap()
    .unwrap();
    assert_eq!(region.get_id(), region_id);
    assert_eq!(peer.get_id(), 3);
    assert_eq!(peer.get_store_id(), 1);
}

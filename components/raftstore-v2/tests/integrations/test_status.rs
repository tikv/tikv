// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_cmdpb::{RaftCmdRequest, StatusCmdType};
use tikv_util::store::new_peer;

use crate::cluster::Cluster;

#[test]
fn test_status() {
    let cluster = Cluster::default();
    let router = &cluster.routers[0];
    // When there is only one peer, it should campaign immediately.
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionLeader);
    let res = router.query(2, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    assert_eq!(
        *status_resp.get_region_leader().get_leader(),
        new_peer(1, 3)
    );

    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionDetail);
    let res = router.query(2, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    let detail = status_resp.get_region_detail();
    assert_eq!(*detail.get_leader(), new_peer(1, 3));
    let region = detail.get_region();
    assert_eq!(region.get_id(), 2);
    assert!(region.get_start_key().is_empty());
    assert!(region.get_end_key().is_empty());
    assert_eq!(*region.get_peers(), vec![new_peer(1, 3)]);
    assert_eq!(region.get_region_epoch().get_version(), 1);
    assert_eq!(region.get_region_epoch().get_conf_ver(), 1);

    // Invalid store id should return error.
    req.mut_header().mut_peer().set_store_id(4);
    let res = router.query(2, req).unwrap();
    let resp = res.response().unwrap();
    assert!(
        resp.get_header().get_error().has_store_not_match(),
        "{:?}",
        resp
    );

    // TODO: add a peer then check for region change and leadership change.
}

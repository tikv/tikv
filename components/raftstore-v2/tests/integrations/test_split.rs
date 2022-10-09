// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use kvproto::raft_cmdpb::{RaftCmdRequest, StatusCmdType};
use tikv_util::store::new_peer;

use crate::cluster::Cluster;

#[test]
fn test_split() {
    let cluster = Cluster::default();
    let router = cluster.router(0);
    std::thread::sleep(std::time::Duration::from_millis(200));
    let region_id = 2;
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionDetail);
    let res = router.query(region_id, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    let detail = status_resp.get_region_detail();
    let mut region = detail.get_region().clone();
}

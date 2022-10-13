use std::time::Duration;

use futures::executor::block_on;
// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use kvproto::{
    pdpb,
    raft_cmdpb::{
        AdminCmdType, AdminRequest, CmdType, RaftCmdRequest, Request, SplitRequest, StatusCmdType,
    },
};
use raftstore::store::{INIT_EPOCH_CONF_VER, INIT_EPOCH_VER};
use raftstore_v2::router::PeerMsg;
use tikv_util::store::new_peer;

use crate::cluster::Cluster;

fn new_batch_split_region_request(
    split_keys: Vec<Vec<u8>>,
    ids: Vec<pdpb::SplitId>,
    right_derive: bool,
) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::BatchSplit);
    req.mut_splits().set_right_derive(right_derive);
    let mut requests = Vec::with_capacity(ids.len());
    for (mut id, key) in ids.into_iter().zip(split_keys) {
        let mut split = SplitRequest::default();
        split.set_split_key(key);
        split.set_new_region_id(id.get_new_region_id());
        split.set_new_peer_ids(id.take_new_peer_ids());
        requests.push(split);
    }
    req.mut_splits().set_requests(requests.into());
    req
}

#[test]
fn test_split() {
    let cluster = Cluster::default();
    let router = cluster.router(0);

    let region_id2 = 2;
    let peer3 = new_peer(1, 3);
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_region_id(region_id2);
    let epoch = req.mut_header().mut_region_epoch();
    epoch.set_version(INIT_EPOCH_VER);
    epoch.set_conf_ver(INIT_EPOCH_CONF_VER);
    req.mut_header().set_peer(peer3.clone());

    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(b"k11".to_vec());
    put_req.mut_put().set_value(b"v1".to_vec());
    req.mut_requests().push(put_req);

    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(b"k33".to_vec());
    put_req.mut_put().set_value(b"v3".to_vec());
    req.mut_requests().push(put_req);

    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    let (msg, mut sub) = PeerMsg::raft_command(req.clone());
    router.send(2, msg).unwrap();
    assert!(block_on(sub.wait_proposed()));
    assert!(block_on(sub.wait_committed()));
    let resp = block_on(sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    let mut split_id = pdpb::SplitId::new();
    let region_id1000 = 1000;
    let peer10 = new_peer(1, 10);
    split_id.new_region_id = region_id1000;
    split_id.new_peer_ids = vec![peer10.id];
    let admin_req = new_batch_split_region_request(vec![b"k22".to_vec()], vec![split_id], false);
    req.mut_requests().clear();
    req.set_admin_request(admin_req);
    let (msg, sub) = PeerMsg::raft_command(req.clone());
    router.send(2, msg).unwrap();
    block_on(sub.result()).unwrap();

    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(peer3);
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionDetail);
    let res = router.query(region_id2, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    let detail = status_resp.get_region_detail();
    let region = detail.get_region().clone();
    println!("{:?}", region);

    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(peer10);
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionDetail);
    let res = router.query(region_id1000, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    let detail = status_resp.get_region_detail();
    let region = detail.get_region().clone();
    println!("{:?}", region);
}

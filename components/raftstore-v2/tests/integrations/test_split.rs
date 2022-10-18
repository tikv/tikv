use std::time::Duration;

use engine_traits::{KvEngine, MiscExt, OpenOptions, Peekable, SyncMutable, TabletFactory};
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

use crate::{cluster::Cluster, util::new_snap_request};

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
    let store_id = 1;
    let cluster = Cluster::default();
    let mut router = cluster.router(0);
    let factory = cluster.node(0).tablet_factory();

    let region_id2 = 2;
    let peer3 = new_peer(store_id, 3);
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_region_id(region_id2);
    let epoch = req.mut_header().mut_region_epoch();
    epoch.set_version(INIT_EPOCH_VER);
    epoch.set_conf_ver(INIT_EPOCH_CONF_VER);
    req.mut_header().set_peer(peer3.clone());

    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(b"zk11".to_vec());
    put_req.mut_put().set_value(b"v1".to_vec());
    req.mut_requests().push(put_req);

    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(b"zk33".to_vec());
    put_req.mut_put().set_value(b"v3".to_vec());
    req.mut_requests().push(put_req);

    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    let tablet = factory
        .open_tablet(
            region_id2,
            None,
            OpenOptions::default().set_cache_only(true),
        )
        .unwrap();
    tablet.put(b"zk11", b"v1").unwrap();
    tablet.flush_cfs(true).unwrap();
    tablet.put(b"zk33", b"v3").unwrap();
    tablet.flush_cfs(true).unwrap();

    // let (msg, mut sub) = PeerMsg::raft_command(req.clone());
    // router.send(2, msg).unwrap();
    // assert!(block_on(sub.wait_proposed()));
    // assert!(block_on(sub.wait_committed()));
    // let resp = block_on(sub.result()).unwrap();
    // assert!(!resp.get_header().has_error(), "{:?}", resp);

    let mut split_id = pdpb::SplitId::new();
    let region_id1000 = 1000;
    let peer10 = new_peer(store_id, 10);
    let split_key = b"k22".to_vec();
    split_id.new_region_id = region_id1000;
    split_id.new_peer_ids = vec![peer10.id];
    let admin_req = new_batch_split_region_request(vec![split_key.clone()], vec![split_id], false);
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
    assert_eq!(region.get_end_key(), &split_key);
    let snap_req = new_snap_request(store_id, 3, 6, region);
    let snap = block_on(async { router.get_snapshot(snap_req).await.unwrap() });
    assert_eq!(snap.get_value(b"k11").unwrap().unwrap(), b"v1");
    snap.get_value(b"k33").unwrap_err();

    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(peer10);
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionDetail);
    let res = router.query(region_id1000, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    let detail = status_resp.get_region_detail();
    let region = detail.get_region().clone();
    assert_eq!(region.get_start_key(), &split_key);
    let snap_req = new_snap_request(store_id, 10, 6, region);
    let snap = block_on(async { router.get_snapshot(snap_req).await.unwrap() });
    assert_eq!(snap.get_value(b"k33").unwrap().unwrap(), b"v3");
    snap.get_value(b"k11").unwrap_err();

    let tablet = factory
        .open_tablet(
            region_id2,
            None,
            OpenOptions::default().set_cache_only(true),
        )
        .unwrap();
    println!("{:?}", tablet.get_value(b"zk11"));
    println!("{:?}", tablet.get_value(b"zk33"));
}

use std::{thread, time::Duration};

use engine_traits::Peekable;
use futures::executor::block_on;
// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use kvproto::{
    metapb, pdpb,
    raft_cmdpb::{AdminCmdType, AdminRequest, CmdType, RaftCmdRequest, Request, SplitRequest},
};
use raftstore_v2::router::PeerMsg;
use tikv_util::store::new_peer;

use crate::cluster::{Cluster, TestRouter};

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

// Split the region according to the parameters
// return the updated original region
fn split_region(
    router: &mut TestRouter,
    region: metapb::Region,
    peer: metapb::Peer,
    split_region_id: u64,
    split_peer: metapb::Peer,
    left_key: &[u8],
    right_key: &[u8],
    split_key: &[u8],
    right_derive: bool,
) -> (metapb::Region, metapb::Region) {
    let region_id = region.id;
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_region_id(region_id);
    req.mut_header()
        .set_region_epoch(region.get_region_epoch().clone());
    req.mut_header().set_peer(peer.clone());

    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(left_key.to_vec());
    put_req.mut_put().set_value(b"v1".to_vec());
    req.mut_requests().push(put_req);

    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(right_key.to_vec());
    put_req.mut_put().set_value(b"v3".to_vec());
    req.mut_requests().push(put_req);

    let (msg, mut sub) = PeerMsg::raft_command(req.clone());
    router.send(region_id, msg).unwrap();
    assert!(block_on(sub.wait_proposed()));
    assert!(block_on(sub.wait_committed()));
    let resp = block_on(sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    let mut split_id = pdpb::SplitId::new();
    split_id.new_region_id = split_region_id;
    split_id.new_peer_ids = vec![split_peer.id];
    let admin_req =
        new_batch_split_region_request(vec![split_key.to_vec()], vec![split_id], right_derive);
    req.mut_requests().clear();
    req.set_admin_request(admin_req);
    let (msg, sub) = PeerMsg::raft_command(req.clone());
    router.send(region_id, msg).unwrap();
    block_on(sub.result()).unwrap();

    // Wait for apply exect result
    thread::sleep(Duration::from_secs(1));

    let (left, right) = if !right_derive {
        (
            router.region_detail(region_id),
            router.region_detail(split_region_id),
        )
    } else {
        (
            router.region_detail(split_region_id),
            router.region_detail(region_id),
        )
    };

    let snap = router.snapshot(left.id).unwrap();
    assert_eq!(snap.get_value(left_key).unwrap().unwrap(), b"v1");
    snap.get_value(right_key).unwrap_err();

    let snap = router.snapshot(right.id).unwrap();
    assert_eq!(snap.get_value(right_key).unwrap().unwrap(), b"v3");
    snap.get_value(left_key).unwrap_err();

    assert_eq!(left.get_end_key(), split_key);
    assert_eq!(right.get_start_key(), split_key);
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());

    (left, right)
}

#[test]
fn test_split() {
    let cluster = Cluster::default();
    let store_id = cluster.node(0).id();
    let mut router = cluster.router(0);
    // let factory = cluster.node(0).tablet_factory();

    let region_id = 2;
    let peer = new_peer(store_id, 3);
    let region = router.region_detail(region_id);

    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    // Region 2 ["", ""] peer(1, 3)
    //   -> Region 2    ["", "k22"] peer(1, 3)
    //      Region 1000 ["k22", ""] peer(1, 10)
    let (left, right) = split_region(
        &mut router,
        region,
        peer.clone(),
        1000,
        new_peer(store_id, 10),
        b"k11",
        b"k33",
        b"k22",
        false,
    );

    // Region 2 ["", "k22"] peer(1, 3)
    //   -> Region 2    ["", "k11"]    peer(1, 3)
    //      Region 1001 ["k11", "k22"] peer(1, 11)
    let _ = split_region(
        &mut router,
        left,
        peer,
        1001,
        new_peer(store_id, 11),
        b"k00",
        b"k11",
        b"k11",
        false,
    );

    // Region 1000 ["k22", ""] peer(1, 10)
    //   -> Region 1000 ["k22", "k33"] peer(1, 10)
    //      Region 1002 ["k33", ""]    peer(1, 12)
    let _ = split_region(
        &mut router,
        right,
        new_peer(store_id, 10),
        1002,
        new_peer(store_id, 12),
        b"k22",
        b"k33",
        b"k33",
        false,
    );
}

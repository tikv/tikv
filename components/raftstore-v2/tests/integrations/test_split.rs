// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use engine_traits::{RaftEngineReadOnly, CF_DEFAULT, CF_RAFT};
use futures::executor::block_on;
use kvproto::{
    metapb, pdpb,
    raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest, RaftCmdResponse, SplitRequest},
};
use raftstore::store::{INIT_EPOCH_VER, RAFT_INIT_LOG_INDEX};
use raftstore_v2::{router::PeerMsg, SimpleWriteEncoder};
use tikv_util::store::new_peer;
use txn_types::{Key, TimeStamp};

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

fn must_split(region_id: u64, req: RaftCmdRequest, router: &mut TestRouter) {
    let (msg, sub) = PeerMsg::admin_command(req);
    router.send(region_id, msg).unwrap();
    block_on(sub.result()).unwrap();

    // TODO: when persistent implementation is ready, we can use tablet index of
    // the parent to check whether the split is done. Now, just sleep a second.
    thread::sleep(Duration::from_secs(1));
}

fn put(router: &mut TestRouter, region_id: u64, key: &[u8]) -> RaftCmdResponse {
    let header = Box::new(router.new_request_for(region_id).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, key, b"v1");
    router.simple_write(region_id, header, put).unwrap()
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
    propose_key: &[u8],
    split_key: &[u8],
    right_derive: bool,
) -> (metapb::Region, metapb::Region) {
    let region_id = region.id;
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_region_id(region_id);
    req.mut_header()
        .set_region_epoch(region.get_region_epoch().clone());
    req.mut_header().set_peer(peer);

    let mut split_id = pdpb::SplitId::new();
    split_id.new_region_id = split_region_id;
    split_id.new_peer_ids = vec![split_peer.id];
    let admin_req =
        new_batch_split_region_request(vec![propose_key.to_vec()], vec![split_id], right_derive);
    req.mut_requests().clear();
    req.set_admin_request(admin_req);

    must_split(region_id, req, router);

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

    // The end key of left region is `split_key`
    // So writing `right_key` will fail
    let resp = put(router, left.id, right_key);
    assert!(resp.get_header().has_error(), "{:?}", resp);
    // But `left_key` should succeed
    let resp = put(router, left.id, left_key);
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    // Mirror of above case
    let resp = put(router, right.id, left_key);
    assert!(resp.get_header().has_error(), "{:?}", resp);
    let resp = put(router, right.id, right_key);
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    assert_eq!(left.get_end_key(), split_key);
    assert_eq!(right.get_start_key(), split_key);
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());

    (left, right)
}

#[test]
fn test_split() {
    let mut cluster = Cluster::default();
    let store_id = cluster.node(0).id();
    let raft_engine = cluster.node(0).running_state().unwrap().raft_engine.clone();
    let router = &mut cluster.routers[0];
    // let factory = cluster.node(0).tablet_factory();

    let region_id = 2;
    let peer = new_peer(store_id, 3);
    let region = router.region_detail(region_id);
    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    // Region 2 ["", ""] peer(1, 3)
    //   -> Region 2    ["", "k22"] peer(1, 3)
    //      Region 1000 ["k22", ""] peer(1, 10)
    let region_state = raft_engine.get_region_state(2, u64::MAX).unwrap().unwrap();
    assert_eq!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    let (left, mut right) = split_region(
        router,
        region,
        peer.clone(),
        1000,
        new_peer(store_id, 10),
        b"k11",
        b"k33",
        b"k22",
        b"k22",
        false,
    );
    let region_state = raft_engine.get_region_state(2, u64::MAX).unwrap().unwrap();
    assert_ne!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    assert_eq!(
        region_state.get_region().get_region_epoch().get_version(),
        INIT_EPOCH_VER + 1
    );
    let region_state0 = raft_engine
        .get_region_state(2, region_state.get_tablet_index())
        .unwrap()
        .unwrap();
    assert_eq!(region_state, region_state0);
    let flushed_index = raft_engine.get_flushed_index(2, CF_RAFT).unwrap().unwrap();
    assert!(
        flushed_index >= region_state.get_tablet_index(),
        "{flushed_index} >= {}",
        region_state.get_tablet_index()
    );

    // Region 2 ["", "k22"] peer(1, 3)
    //   -> Region 2    ["", "k11"]    peer(1, 3)
    //      Region 1001 ["k11", "k22"] peer(1, 11)
    let _ = split_region(
        router,
        left,
        peer,
        1001,
        new_peer(store_id, 11),
        b"k00",
        b"k11",
        b"k11",
        b"k11",
        false,
    );
    let region_state = raft_engine.get_region_state(2, u64::MAX).unwrap().unwrap();
    assert_ne!(
        region_state.get_tablet_index(),
        region_state0.get_tablet_index()
    );
    assert_eq!(
        region_state.get_region().get_region_epoch().get_version(),
        INIT_EPOCH_VER + 2
    );
    let region_state1 = raft_engine
        .get_region_state(2, region_state.get_tablet_index())
        .unwrap()
        .unwrap();
    assert_eq!(region_state, region_state1);
    let flushed_index = raft_engine.get_flushed_index(2, CF_RAFT).unwrap().unwrap();
    assert!(
        flushed_index >= region_state.get_tablet_index(),
        "{flushed_index} >= {}",
        region_state.get_tablet_index()
    );

    // Region 1000 ["k22", ""] peer(1, 10)
    //   -> Region 1000 ["k22", "k33"] peer(1, 10)
    //      Region 1002 ["k33", ""]    peer(1, 12)
    let region_state = raft_engine
        .get_region_state(1000, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    right = split_region(
        router,
        right,
        new_peer(store_id, 10),
        1002,
        new_peer(store_id, 12),
        b"k22",
        b"k33",
        b"k33",
        b"k33",
        false,
    )
    .1;
    let region_state = raft_engine
        .get_region_state(1000, u64::MAX)
        .unwrap()
        .unwrap();
    assert_ne!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);
    assert_eq!(
        region_state.get_region().get_region_epoch().get_version(),
        INIT_EPOCH_VER + 2
    );
    let region_state2 = raft_engine
        .get_region_state(1000, region_state.get_tablet_index())
        .unwrap()
        .unwrap();
    assert_eq!(region_state, region_state2);
    let flushed_index = raft_engine.get_flushed_index(2, CF_RAFT).unwrap().unwrap();
    assert!(
        flushed_index >= region_state.get_tablet_index(),
        "{flushed_index} >= {}",
        region_state.get_tablet_index()
    );

    let split_key = Key::from_raw(b"k44").append_ts(TimeStamp::zero());
    let actual_split_key = split_key.clone().truncate_ts().unwrap();
    split_region(
        router,
        right,
        new_peer(store_id, 12),
        1003,
        new_peer(store_id, 13),
        b"k33",
        b"k55",
        split_key.as_encoded(),
        actual_split_key.as_encoded(),
        false,
    );
}

// TODO: test split race with
// - created peer
// - created peer with pending snapshot
// - created peer with persisting snapshot
// - created peer with persisted snapshot

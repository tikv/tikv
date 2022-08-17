// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::assert_matches::assert_matches;

use futures::executor::block_on;
use kvproto::raft_cmdpb::{RaftCmdRequest, StatusCmdType};
use raftstore::store::util::new_peer;
use raftstore_v2::router::{
    PeerMsg, PeerTick, QueryResChannel, QueryResult, RaftQuery, RaftRequest,
};

#[test]
fn test_status() {
    let (_node, _transport, router) = super::setup_default_cluster();
    // When there is only one peer, it should campaign immediately.
    let mut req = RaftCmdRequest::default();
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionLeader);
    let req = RaftRequest::new(req);
    let (ch, sub) = QueryResChannel::pair();
    let msg = PeerMsg::RaftQuery(RaftQuery { req, ch });
    router.send(2, msg).unwrap();
    let res = block_on(sub.result()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    assert_eq!(
        *status_resp.get_region_leader().get_leader(),
        new_peer(1, 3)
    );

    // TODO: add a peer then check for region change and leadership change.
}

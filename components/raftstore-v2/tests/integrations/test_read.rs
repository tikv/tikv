// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::assert_matches::assert_matches;

use futures::executor::block_on;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, ReadIndexRequest, Request};
use raftstore::store::util::new_peer;
use tikv_util::thread_group::{set_properties, GroupProperties};

#[test]
fn test_read_index() {
    set_properties(Some(GroupProperties::default()));
    let (_node, _transport, router) = super::setup_default_cluster();

    let mut read_index_req = ReadIndexRequest::default();
    read_index_req.set_start_ts(100);
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_header().set_term(6);
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::ReadIndex);
    request_inner.set_read_index(read_index_req);
    req.mut_requests().push(request_inner);
    let res = router.query(2, req.clone()).unwrap();
    let resps = res.response().unwrap().get_responses();

    // because the leader has not commited any log, the readindex will be ignored.
    assert_eq!(resps.len(), 0);

    // TODO: add more test when write is implemented.
}

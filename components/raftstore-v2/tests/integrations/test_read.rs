// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::assert_matches::assert_matches;

use futures::executor::block_on;
use kvproto::{
    kvrpcpb::Context,
    raft_cmdpb::{CmdType, GetRequest, RaftCmdRequest, ReadIndexRequest, Request, StatusCmdType},
};
use raftstore::store::util::new_peer;
use tikv_util::{
    codec::number::NumberEncoder,
    thread_group::{set_properties, GroupProperties},
};
use txn_types::WriteBatchFlags;

#[test]
fn test_read_index() {
    set_properties(Some(GroupProperties::default()));
    let (_node, _transport, router) = super::setup_default_cluster();

    let region_id = 2;
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionDetail);
    let res = router.query(region_id, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    let detail = status_resp.get_region_detail();
    let mut region = detail.get_region().clone();

    let mut read_index_req = ReadIndexRequest::default();
    read_index_req.set_start_ts(100);
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_header().set_term(6);
    req.mut_header().set_region_id(region_id);
    req.mut_header()
        .set_region_epoch(region.take_region_epoch());
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::ReadIndex);
    request_inner.set_read_index(read_index_req);
    req.mut_requests().push(request_inner);
    let res = router.query(region_id, req.clone()).unwrap();
    // because the leader has not commited any log, the readindex will be ignored.
    assert!(res.read().is_none());

    // TODO: add more test when write is implemented.
}

#[test]
fn test_read_stale() {
    set_properties(Some(GroupProperties::default()));
    let (_node, _transport, router) = super::setup_default_cluster();

    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_status_request()
        .set_cmd_type(StatusCmdType::RegionDetail);
    let res = router.query(2, req.clone()).unwrap();
    let status_resp = res.response().unwrap().get_status_response();
    let detail = status_resp.get_region_detail();
    let mut region = detail.get_region().clone();

    let read_req = GetRequest::default();
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::Get);
    request_inner.set_get(read_req);
    req.mut_header()
        .set_flags(WriteBatchFlags::STALE_READ.bits());
    let start_ts = {
        let mut d = [0u8; 8];
        (&mut d[..]).encode_u64(0).unwrap();
        d
    };
    req.mut_header().set_flag_data(start_ts.into());
    req.mut_header()
        .set_region_epoch(region.take_region_epoch());
    req.mut_requests().push(request_inner);
    let res = router.query(region.get_id(), req.clone()).unwrap();
    let read_index = res.read().unwrap().read_index;
    assert_eq!(read_index, 0);
}

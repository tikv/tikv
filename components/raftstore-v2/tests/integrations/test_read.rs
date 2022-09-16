// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::assert_matches::assert_matches;

use futures::executor::block_on;
use kvproto::{
    kvrpcpb::Context,
    raft_cmdpb::{CmdType, GetRequest, RaftCmdRequest, ReadIndexRequest, Request, StatusCmdType},
};
use raftstore::store::util::new_peer;
use tikv_util::codec::number::NumberEncoder;
use txn_types::WriteBatchFlags;

#[test]
fn test_read_index() {
    let (_node, _transport, router) = super::setup_default_cluster();
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

    let read_index_req = ReadIndexRequest::default();
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_header().set_term(7);
    req.mut_header().set_region_id(region_id);
    req.mut_header()
        .set_region_epoch(region.take_region_epoch());
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::Snap);
    request_inner.set_read_index(read_index_req);
    req.mut_requests().push(request_inner);
    let res = router.query(region_id, req.clone()).unwrap();
    let resp = res.read().unwrap();
    assert_eq!(resp.read_index, 6); // single node commited index should be 6.

    // TODO: add more test when write is implemented.
}

#[test]
fn test_snap_without_read_index() {
    let (_node, _transport, router) = super::setup_default_cluster();
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

    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_header().set_term(6);
    req.mut_header().set_region_id(region_id);
    req.mut_header()
        .set_region_epoch(region.take_region_epoch());
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::Snap);
    req.mut_requests().push(request_inner);
    let res = router.query(region_id, req.clone()).unwrap();
    let resp = res.read().unwrap();
    // single node commited index should be 6.
    assert_eq!(resp.read_index, 6);

    // run again, this time we expect the lease is not expired and the read index
    // should be 0.
    let res = router.query(region_id, req.clone()).unwrap();
    let resp = res.read().unwrap();
    // the request can be processed locally, read index should be 0.
    assert_eq!(resp.read_index, 0);

    // run with header read_quorum
    req.mut_header().set_read_quorum(true);
    let res = router.query(region_id, req.clone()).unwrap();
    let resp = res.read().unwrap();
    // even the lease is valid, it should run read index
    assert_eq!(resp.read_index, 6);

    // TODO: add more test when write is implemented.
}

#[test]
fn test_query_with_write_cmd() {
    let (_node, _transport, router) = super::setup_default_cluster();
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

    let mut req = RaftCmdRequest::default();
    req.mut_header().set_peer(new_peer(1, 3));
    req.mut_header().set_term(6);
    req.mut_header().set_region_id(region_id);
    req.mut_header()
        .set_region_epoch(region.take_region_epoch());

    for write_cmd in [
        CmdType::Prewrite,
        CmdType::Delete,
        CmdType::DeleteRange,
        CmdType::Put,
        CmdType::IngestSst,
    ] {
        let mut request_inner = Request::default();
        request_inner.set_cmd_type(write_cmd);
        req.mut_requests().push(request_inner);
        let res = router.query(region_id, req.clone()).unwrap();
        let resp = res.read();
        assert!(resp.is_none());
        let error_resp = res.response().unwrap();
        assert!(error_resp.get_header().has_error());
    }
}

#[test]
fn test_snap_with_invalid_parameter() {
    let (_node, _transport, router) = super::setup_default_cluster();
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
    let mut region_epoch = region.take_region_epoch();

    let mut req = RaftCmdRequest::default();
    // store_id is incorrect;
    req.mut_header().set_peer(new_peer(2, 3));
    req.mut_header().set_term(6);
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_region_epoch(region_epoch.clone());
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::Snap);
    req.mut_requests().push(request_inner);
    let res = router.query(region_id, req.clone()).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());

    // run again, with incorrect peer_id
    req.mut_header().set_peer(new_peer(1, 4));
    let res = router.query(region_id, req.clone()).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());

    // run with stale term
    req.mut_header().set_term(1);
    req.mut_header().set_peer(new_peer(1, 3));
    let res = router.query(region_id, req.clone()).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());

    // run with stale read
    req.mut_header().set_term(7);
    req.mut_header()
        .set_flags(WriteBatchFlags::STALE_READ.bits());
    let res = router.query(region_id, req.clone()).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());

    // run again with invalid region_epoch
    req.mut_header().set_flags(0);
    region_epoch.set_version(region_epoch.get_version() + 1);
    req.mut_header().set_region_epoch(region_epoch.clone());
    req.mut_header().set_term(6);
    let res = router.query(region_id, req.clone()).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());
}


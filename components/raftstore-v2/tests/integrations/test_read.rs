// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use kvproto::raft_cmdpb::{CmdType, Request};
use raftstore_v2::router::PeerMsg;
use tikv_util::{config::ReadableDuration, store::new_peer};
use txn_types::WriteBatchFlags;

use crate::cluster::{v2_default_config, Cluster};

#[test]
fn test_read_index() {
    let mut config = v2_default_config();
    config.raft_store_max_leader_lease = ReadableDuration::millis(150);
    let cluster = Cluster::with_config(config);
    let router = cluster.router(0);
    std::thread::sleep(std::time::Duration::from_millis(200));
    let region_id = 2;
    let mut req = router.new_request_for(region_id);
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::Snap);
    request_inner.mut_read_index();
    req.mut_requests().push(request_inner);
    let res = router.query(region_id, req.clone()).unwrap();
    let resp = res.read().unwrap();
    assert_eq!(resp.read_index, 6); // single node commited index should be 6.

    let res = router.query(region_id, req.clone()).unwrap();
    let resp = res.read().unwrap();
    // Since it's still with the lease, read index will be skipped.
    assert_eq!(resp.read_index, 0);

    std::thread::sleep(std::time::Duration::from_millis(200));
    // the read lease should be expired
    let res = router.query(region_id, req.clone()).unwrap();
    let resp = res.read().unwrap();
    assert_eq!(resp.read_index, 6);

    std::thread::sleep(std::time::Duration::from_millis(200));
    let read_req = req.clone();
    // the read lease should be expired and renewed by write
    let mut req = router.new_request_for(region_id);
    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(b"key".to_vec());
    put_req.mut_put().set_value(b"value".to_vec());
    req.mut_requests().push(put_req);

    let (msg, sub) = PeerMsg::raft_command(req.clone());
    router.send(region_id, msg).unwrap();
    block_on(sub.result()).unwrap();

    let res = router.query(region_id, read_req).unwrap();
    let resp = res.read().unwrap();
    assert_eq!(resp.read_index, 0);
}

#[test]
fn test_snap_without_read_index() {
    let cluster = Cluster::default();
    let router = cluster.router(0);
    std::thread::sleep(std::time::Duration::from_millis(200));
    let region_id = 2;
    let mut req = router.new_request_for(region_id);
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::Snap);
    req.mut_requests().push(request_inner);
    let res = router.query(region_id, req.clone()).unwrap();
    let resp = res.read().unwrap();
    // When it becomes leader, it will get a lease automatically because of empty
    // entry.
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
    let cluster = Cluster::default();
    let router = cluster.router(0);
    std::thread::sleep(std::time::Duration::from_millis(200));
    let region_id = 2;
    let mut req = router.new_request_for(2);

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
        req.clear_requests();
    }
}

#[test]
fn test_snap_with_invalid_parameter() {
    let cluster = Cluster::default();
    let router = cluster.router(0);
    std::thread::sleep(std::time::Duration::from_millis(200));
    let region_id = 2;
    let mut req = router.new_request_for(region_id);
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::Snap);
    req.mut_requests().push(request_inner);

    // store_id is incorrect;
    let mut invalid_req = req.clone();
    invalid_req.mut_header().set_peer(new_peer(2, 3));
    let res = router.query(region_id, invalid_req).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());

    // run again, with incorrect peer_id
    let mut invalid_req = req.clone();
    invalid_req.mut_header().set_peer(new_peer(1, 4));
    let res = router.query(region_id, invalid_req).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());

    // run with stale term
    let mut invalid_req = req.clone();
    invalid_req.mut_header().set_term(1);
    let res = router.query(region_id, invalid_req).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());

    // run with stale read
    let mut invalid_req = req.clone();
    invalid_req
        .mut_header()
        .set_flags(WriteBatchFlags::STALE_READ.bits());
    let res = router.query(region_id, invalid_req).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());

    // run again with invalid region_epoch
    let mut invalid_req = req.clone();
    let invalid_ver = req.get_header().get_region_epoch().get_version() + 1;
    invalid_req
        .mut_header()
        .mut_region_epoch()
        .set_version(invalid_ver);
    let res = router.query(region_id, invalid_req).unwrap();
    let error_resp = res.response().unwrap();
    assert!(error_resp.get_header().has_error());
}

#[test]
fn test_local_read() {
    let cluster = Cluster::default();
    let mut router = cluster.router(0);
    std::thread::sleep(std::time::Duration::from_millis(200));
    let region_id = 2;
    let mut req = router.new_request_for(region_id);
    let mut request_inner = Request::default();
    request_inner.set_cmd_type(CmdType::Snap);
    req.mut_requests().push(request_inner);

    block_on(async { router.get_snapshot(req.clone()).await.unwrap() });
    let res = router.query(region_id, req.clone()).unwrap();
    let resp = res.read().unwrap();
    // The read index will be 0 as the retry process in the `get_snapshot` will
    // renew the lease.
    assert_eq!(resp.read_index, 0);
}

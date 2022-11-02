// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, time::Duration};

use engine_traits::{OpenOptions, Peekable, TabletFactory};
use futures::executor::block_on;
use kvproto::raft_cmdpb::{CmdType, Request};
use raftstore_v2::router::PeerMsg;

use crate::cluster::Cluster;

/// Check if write batch is correctly maintained during apply.
#[test]
fn test_write_batch_rollback() {
    let cluster = Cluster::default();
    let router = cluster.router(0);
    let mut req = router.new_request_for(2);
    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(b"key".to_vec());
    put_req.mut_put().set_value(b"value".to_vec());
    req.mut_requests().push(put_req.clone());

    router.wait_applied_to_current_term(2, Duration::from_secs(3));
    // Make several entries to batch in apply thread.
    fail::cfg("APPLY_COMMITTED_ENTRIES", "pause").unwrap();

    let tablet_factory = cluster.node(0).tablet_factory();
    let tablet = tablet_factory
        .open_tablet(2, None, OpenOptions::default().set_cache_only(true))
        .unwrap();

    // Good proposal should be committed.
    let (msg, mut sub0) = PeerMsg::raft_command(req.clone());
    router.send(2, msg).unwrap();
    assert!(block_on(sub0.wait_proposed()));
    assert!(block_on(sub0.wait_committed()));

    // If the write batch is correctly initialized, next write should not contain
    // last result.
    req.mut_requests()[0].mut_put().set_key(b"key1".to_vec());
    let (msg, mut sub1) = PeerMsg::raft_command(req.clone());
    router.send(2, msg).unwrap();
    assert!(block_on(sub1.wait_proposed()));
    assert!(block_on(sub1.wait_committed()));

    fail::cfg("APPLY_PUT", "1*return()").unwrap();
    // Wake up and sleep in next committed entry.
    fail::remove("APPLY_COMMITTED_ENTRIES");
    // First apply will fail due to aborted. If write batch is initialized
    // correctly, correct response can be returned.
    let resp = block_on(sub0.result()).unwrap();
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("aborted"),
        "{:?}",
        resp
    );
    let resp = block_on(sub1.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_matches!(tablet.get_value(b"key"), Ok(None));
    assert_eq!(tablet.get_value(b"key1").unwrap().unwrap(), b"value");

    fail::cfg("APPLY_COMMITTED_ENTRIES", "pause").unwrap();

    // Trigger error again, so an initialized write batch should be rolled back.
    req.mut_requests()[0].mut_put().set_key(b"key2".to_vec());
    let (msg, mut sub0) = PeerMsg::raft_command(req.clone());
    router.send(2, msg).unwrap();
    assert!(block_on(sub0.wait_proposed()));
    assert!(block_on(sub0.wait_committed()));

    // If the write batch is correctly rollbacked, next write should not contain
    // last result.
    req.mut_requests()[0].mut_put().set_key(b"key3".to_vec());
    let (msg, mut sub1) = PeerMsg::raft_command(req.clone());
    router.send(2, msg).unwrap();
    assert!(block_on(sub1.wait_proposed()));
    assert!(block_on(sub1.wait_committed()));

    fail::cfg("APPLY_PUT", "1*return()").unwrap();
    fail::remove("APPLY_COMMITTED_ENTRIES");
    let resp = block_on(sub0.result()).unwrap();
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("aborted"),
        "{:?}",
        resp
    );
    let resp = block_on(sub1.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_matches!(tablet.get_value(b"key2"), Ok(None));
    assert_eq!(tablet.get_value(b"key3").unwrap().unwrap(), b"value");
}

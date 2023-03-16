// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, time::Duration};

use engine_traits::{Peekable, CF_DEFAULT};
use futures::executor::block_on;
use raftstore_v2::{router::PeerMsg, SimpleWriteEncoder};

use crate::cluster::Cluster;

/// Check if write batch is correctly maintained during apply.
#[test]
fn test_write_batch_rollback() {
    let mut cluster = Cluster::default();
    let router = &mut cluster.routers[0];
    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key", b"value");

    router.wait_applied_to_current_term(2, Duration::from_secs(3));
    // Make several entries to batch in apply thread.
    fail::cfg("APPLY_COMMITTED_ENTRIES", "pause").unwrap();

    // Good proposal should be committed.
    let (msg, mut sub0) = PeerMsg::simple_write(header.clone(), put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub0.wait_proposed()));
    assert!(block_on(sub0.wait_committed()));

    // If the write batch is correctly initialized, next write should not contain
    // last result.
    put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key1", b"value");
    let (msg, mut sub1) = PeerMsg::simple_write(header.clone(), put.encode());
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

    let snap = router.stale_snapshot(2);
    assert_matches!(snap.get_value(b"key"), Ok(None));
    assert_eq!(snap.get_value(b"key1").unwrap().unwrap(), b"value");

    fail::cfg("APPLY_COMMITTED_ENTRIES", "pause").unwrap();

    // Trigger error again, so an initialized write batch should be rolled back.
    put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key2", b"value");
    let (msg, mut sub0) = PeerMsg::simple_write(header.clone(), put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub0.wait_proposed()));
    assert!(block_on(sub0.wait_committed()));

    // If the write batch is correctly rollbacked, next write should not contain
    // last result.
    put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key3", b"value");
    let (msg, mut sub1) = PeerMsg::simple_write(header, put.encode());
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
    let snap = router.stale_snapshot(2);
    assert_matches!(snap.get_value(b"key2"), Ok(None));
    assert_eq!(snap.get_value(b"key3").unwrap().unwrap(), b"value");
}

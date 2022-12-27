// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, time::Duration};

use engine_traits::{Peekable, CF_DEFAULT};
use kvproto::raft_serverpb::RaftMessage;
use raftstore::store::{INIT_EPOCH_CONF_VER, INIT_EPOCH_VER};
use raftstore_v2::{router::PeerMsg, SimpleWriteEncoder};
use tikv_util::{config::ReadableSize, store::new_peer};

use crate::cluster::{check_skip_wal, v2_default_config, Cluster};

/// Test basic write flow.
#[test]
fn test_basic_write() {
    let mut config = v2_default_config();
    config.raft_entry_max_size = ReadableSize::kb(1);
    let mut cluster = Cluster::with_config(config);
    let router = &mut cluster.routers[0];
    let region_id = 2;
    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key", b"value");

    router.wait_applied_to_current_term(region_id, Duration::from_secs(3));
    // it will propose this batch write util the buffer size is bigger than the
    // raft_entry_max_size.
    for _ in 0..35 {
        let (msg, _) = PeerMsg::simple_write(header.clone(), put.clone().encode());
        router.send(region_id, msg).unwrap();
    }

    let snap = router.stale_snapshot(region_id);
    assert_eq!(snap.get_value(b"key").unwrap().unwrap(), b"value");

    let resp = router
        .simple_write(region_id, header.clone(), put.clone())
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    // Store id should be checked.
    let mut invalid_header = header.clone();
    invalid_header.set_peer(new_peer(3, 3));
    let resp = router
        .simple_write(region_id, invalid_header, put.clone())
        .unwrap();
    assert!(
        resp.get_header().get_error().has_store_not_match(),
        "{:?}",
        resp
    );

    // Peer id should be checked.
    invalid_header = header.clone();
    invalid_header.set_peer(new_peer(1, 1));
    let resp = router.simple_write(2, invalid_header, put.clone()).unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);

    // Epoch should be checked.
    invalid_header = header.clone();
    invalid_header
        .mut_region_epoch()
        .set_version(INIT_EPOCH_VER - 1);
    let resp = router.simple_write(2, invalid_header, put.clone()).unwrap();
    assert!(
        resp.get_header().get_error().has_epoch_not_match(),
        "{:?}",
        resp
    );

    // Term should be checked if set.
    invalid_header = header.clone();
    invalid_header.set_term(1);
    let resp = router.simple_write(2, invalid_header, put.clone()).unwrap();
    assert!(
        resp.get_header().get_error().has_stale_command(),
        "{:?}",
        resp
    );

    // Too large message can cause regression and should be rejected.
    let mut invalid_put = SimpleWriteEncoder::with_capacity(9 * 1024 * 1024);
    invalid_put.put(CF_DEFAULT, b"key", &vec![0; 8 * 1024 * 1024]);
    let resp = router
        .simple_write(region_id, header.clone(), invalid_put)
        .unwrap();
    assert!(
        resp.get_header().get_error().has_raft_entry_too_large(),
        "{:?}",
        resp
    );

    // Make it step down and follower should reject write.
    let mut msg = Box::<RaftMessage>::default();
    msg.set_region_id(2);
    msg.set_to_peer(new_peer(1, 3));
    msg.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    msg.set_from_peer(new_peer(2, 4));
    let raft_message = msg.mut_message();
    raft_message.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
    raft_message.set_from(4);
    raft_message.set_term(8);
    router.send_raft_message(msg).unwrap();
    let resp = router.simple_write(region_id, header, put).unwrap();
    assert!(resp.get_header().get_error().has_not_leader(), "{:?}", resp);
}

#[test]
fn test_put_delete() {
    let mut cluster = Cluster::default();
    let router = &mut cluster.routers[0];
    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key", b"value");

    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    let snap = router.stale_snapshot(2);
    assert!(snap.get_value(b"key").unwrap().is_none());
    let resp = router.simple_write(2, header.clone(), put.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let snap = router.stale_snapshot(2);
    assert_eq!(snap.get_value(b"key").unwrap().unwrap(), b"value");

    let mut delete = SimpleWriteEncoder::with_capacity(64);
    delete.delete(CF_DEFAULT, b"key");
    let resp = router.simple_write(2, header, delete.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let snap = router.stale_snapshot(2);
    assert_matches!(snap.get_value(b"key"), Ok(None));

    // Check if WAL is skipped for basic writes.
    let mut cached = cluster.node(0).tablet_registry().get(2).unwrap();
    check_skip_wal(cached.latest().unwrap().as_inner().path());
}

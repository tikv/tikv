// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::CF_DEFAULT;
use futures::executor::block_on;
use kvproto::raft_serverpb::RaftMessage;
use raft::prelude::MessageType;
use raftstore_v2::{router::PeerMsg, SimpleWriteEncoder};
use tikv_util::store::new_peer;

use crate::cluster::{life_helper::assert_peer_not_exist, Cluster};

/// Test if a peer can be destroyed when it's applying entries
#[test]
fn test_destroy_by_larger_id_while_applying() {
    let fp = "APPLY_COMMITTED_ENTRIES";
    let mut cluster = Cluster::default();
    let router = &cluster.routers[0];
    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    fail::cfg(fp, "pause").unwrap();

    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key", b"value");
    let (msg, mut sub) = PeerMsg::simple_write(header.clone(), put.clone().encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub.wait_committed()));

    let mut larger_id_msg = Box::<RaftMessage>::default();
    larger_id_msg.set_region_id(2);
    let mut target_peer = header.get_peer().clone();
    target_peer.set_id(target_peer.get_id() + 1);
    larger_id_msg.set_to_peer(target_peer.clone());
    larger_id_msg.set_region_epoch(header.get_region_epoch().clone());
    larger_id_msg
        .mut_region_epoch()
        .set_conf_ver(header.get_region_epoch().get_conf_ver() + 1);
    larger_id_msg.set_from_peer(new_peer(2, 8));
    let raft_message = larger_id_msg.mut_message();
    raft_message.set_msg_type(MessageType::MsgHeartbeat);
    raft_message.set_from(8);
    raft_message.set_to(target_peer.get_id());
    raft_message.set_term(10);

    // Larger ID should trigger destroy.
    router.send_raft_message(larger_id_msg).unwrap();
    fail::remove(fp);
    assert_peer_not_exist(2, header.get_peer().get_id(), router);
    let meta = router
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.id, target_peer.get_id());
    assert_eq!(meta.raft_status.hard_state.term, 10);

    std::thread::sleep(Duration::from_millis(10));

    // New peer should survive restart.
    cluster.restart(0);
    let router = &cluster.routers[0];
    let meta = router
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.id, target_peer.get_id());
    assert_eq!(meta.raft_status.hard_state.term, 10);
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    assert_matches::assert_matches,
    thread,
    time::{Duration, Instant},
};

use crossbeam::channel::TrySendError;
use engine_traits::{RaftEngine, RaftEngineReadOnly};
use futures::executor::block_on;
use kvproto::{
    metapb,
    raft_serverpb::{PeerState, RaftMessage},
};
use raftstore_v2::router::{DebugInfoChannel, PeerMsg};
use tikv_util::store::new_peer;

use crate::cluster::{Cluster, TestRouter};

fn assert_peer_not_exist(region_id: u64, peer_id: u64, router: &TestRouter) {
    let timer = Instant::now();
    loop {
        let (ch, sub) = DebugInfoChannel::pair();
        let msg = PeerMsg::QueryDebugInfo(ch);
        match router.send(region_id, msg) {
            Err(TrySendError::Disconnected(_)) => return,
            Ok(()) => {
                if let Some(m) = block_on(sub.result()) {
                    if m.raft_status.id != peer_id {
                        return;
                    }
                }
            }
            Err(_) => (),
        }
        if timer.elapsed() < Duration::from_secs(3) {
            thread::sleep(Duration::from_millis(10));
        } else {
            panic!("peer of {} still exists", region_id);
        }
    }
}

// TODO: make raft engine support more suitable way to verify range is empty.
/// Verify all states in raft engine are cleared.
fn assert_tombstone(raft_engine: &impl RaftEngine, region_id: u64, peer: &metapb::Peer) {
    let mut buf = vec![];
    raft_engine.get_all_entries_to(region_id, &mut buf).unwrap();
    assert!(buf.is_empty(), "{:?}", buf);
    assert_matches!(raft_engine.get_raft_state(region_id), Ok(None));
    assert_matches!(raft_engine.get_apply_state(region_id), Ok(None));
    let region_state = raft_engine.get_region_state(region_id).unwrap().unwrap();
    assert_matches!(region_state.get_state(), PeerState::Tombstone);
    assert!(
        region_state.get_region().get_peers().contains(peer),
        "{:?}",
        region_state
    );
}

/// Test a peer can be created by general raft message and destroyed tombstone
/// message.
#[test]
fn test_life_by_message() {
    let mut cluster = Cluster::default();
    let router = cluster.router(0);
    let test_region_id = 4;
    let test_peer_id = 5;
    let test_leader_id = 6;
    assert_peer_not_exist(test_region_id, test_peer_id, &router);

    // Build a correct message.
    let mut msg = Box::<RaftMessage>::default();
    msg.set_region_id(test_region_id);
    msg.set_to_peer(new_peer(1, test_peer_id));
    msg.mut_region_epoch().set_conf_ver(1);
    msg.set_from_peer(new_peer(2, test_leader_id));
    let raft_message = msg.mut_message();
    raft_message.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
    raft_message.set_from(6);
    raft_message.set_term(5);

    let assert_wrong = |f: &dyn Fn(&mut RaftMessage)| {
        let mut wrong_msg = msg.clone();
        f(&mut wrong_msg);
        router.send_raft_message(wrong_msg).unwrap();
        assert_peer_not_exist(test_region_id, test_peer_id, &router);
    };

    // Check mismatch store id.
    assert_wrong(&|msg| msg.mut_to_peer().set_store_id(4));

    // Check missing region epoch.
    assert_wrong(&|msg| {
        msg.take_region_epoch();
    });

    // Check tombstone.
    assert_wrong(&|msg| msg.set_is_tombstone(true));

    // Correct message will create a peer, but the peer will not be initialized.
    router.send_raft_message(msg.clone()).unwrap();
    let timeout = Duration::from_secs(3);
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.region_state.id, test_region_id);
    assert_eq!(meta.raft_status.id, test_peer_id);
    assert_eq!(meta.region_state.tablet_index, 0);
    // But leader should be set.
    assert_eq!(meta.raft_status.soft_state.leader_id, test_leader_id);

    // The peer should survive restart.
    cluster.restart(0);
    let router = cluster.router(0);
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id);
    let raft_engine = &cluster.node(0).running_state().unwrap().raft_engine;
    raft_engine.get_raft_state(test_region_id).unwrap().unwrap();
    raft_engine
        .get_apply_state(test_region_id)
        .unwrap()
        .unwrap();

    // The peer should be destroyed by tombstone message.
    let mut tombstone_msg = msg.clone();
    tombstone_msg.set_is_tombstone(true);
    router.send_raft_message(tombstone_msg).unwrap();
    assert_peer_not_exist(test_region_id, test_peer_id, &router);
    assert_tombstone(raft_engine, test_region_id, &new_peer(1, test_peer_id));

    // Restart should not recreate tombstoned peer.
    cluster.restart(0);
    let router = cluster.router(0);
    assert_peer_not_exist(test_region_id, test_peer_id, &router);
    let raft_engine = &cluster.node(0).running_state().unwrap().raft_engine;
    assert_tombstone(raft_engine, test_region_id, &new_peer(1, test_peer_id));
}

#[test]
fn test_destroy_by_larger_id() {
    let mut cluster = Cluster::default();
    let router = cluster.router(0);
    let test_region_id = 4;
    let test_peer_id = 6;
    let init_term = 5;
    let mut msg = Box::<RaftMessage>::default();
    msg.set_region_id(test_region_id);
    msg.set_to_peer(new_peer(1, test_peer_id));
    msg.mut_region_epoch().set_conf_ver(1);
    msg.set_from_peer(new_peer(2, 8));
    let raft_message = msg.mut_message();
    raft_message.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
    raft_message.set_from(6);
    raft_message.set_term(init_term);
    // Create the peer.
    router.send_raft_message(msg.clone()).unwrap();

    let timeout = Duration::from_secs(3);
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id);

    // Smaller ID should be ignored.
    let mut smaller_id_msg = msg;
    smaller_id_msg.set_to_peer(new_peer(1, test_peer_id - 1));
    smaller_id_msg.mut_message().set_term(init_term + 1);
    router.send_raft_message(smaller_id_msg.clone()).unwrap();
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id);
    assert_eq!(meta.raft_status.hard_state.term, init_term);

    // Larger ID should trigger destroy.
    let mut larger_id_msg = smaller_id_msg;
    larger_id_msg.set_to_peer(new_peer(1, test_peer_id + 1));
    router.send_raft_message(larger_id_msg).unwrap();
    assert_peer_not_exist(test_region_id, test_peer_id, &router);
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id + 1);
    assert_eq!(meta.raft_status.hard_state.term, init_term + 1);

    // New peer should survive restart.
    cluster.restart(0);
    let router = cluster.router(0);
    let meta = router
        .must_query_debug_info(test_region_id, timeout)
        .unwrap();
    assert_eq!(meta.raft_status.id, test_peer_id + 1);
    assert_eq!(meta.raft_status.hard_state.term, init_term + 1);
}

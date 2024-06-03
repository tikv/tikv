// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc::channel, Arc, Mutex},
    time::Duration,
};

use kvproto::{
    metapb::PeerRole::Learner,
    raft_serverpb::{ExtraMessageType, PeerState, RaftMessage},
};
use raft::{eraftpb::ConfChangeType, prelude::MessageType};
use raftstore::errors::Result;
use test_raftstore::{
    new_admin_request, new_change_peer_request, new_learner_peer, new_peer, Direction, Filter,
    FilterFactory, RegionPacketFilter,
};
use tikv_util::{config::ReadableDuration, time::Instant, HandyRwLock};

#[test]
fn test_gc_peer_with_conf_change() {
    let mut cluster = test_raftstore::new_node_cluster(0, 5);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    cluster.must_transfer_leader(region_id, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    let mut region_epoch = cluster.get_region_epoch(region_id);

    // Create a learner peer 4 on store 4.
    let extra_store_id = 4;
    let extra_peer_id = 4;
    let cc = new_change_peer_request(
        ConfChangeType::AddLearnerNode,
        new_learner_peer(extra_store_id, extra_peer_id),
    );
    let req = new_admin_request(region_id, &region_epoch, cc);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(!res.get_header().has_error(), "{:?}", res);
    region_epoch.conf_ver += 1;
    cluster.wait_peer_state(region_id, 4, PeerState::Normal);

    // Isolate peer 4 from other region peers.
    let left_filter = RegionPacketFilter::new(region_id, extra_store_id)
        .direction(Direction::Recv)
        .skip(MessageType::MsgHup);
    cluster
        .sim
        .wl()
        .add_recv_filter(extra_store_id, Box::new(left_filter));

    // Change peer 4 to voter.
    let cc = new_change_peer_request(
        ConfChangeType::AddNode,
        new_peer(extra_store_id, extra_peer_id),
    );
    let req = new_admin_request(region_id, &region_epoch, cc);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(!res.get_header().has_error(), "{:?}", res);
    region_epoch.conf_ver += 1;

    // Remove peer 4 from region 1.
    let cc = new_change_peer_request(
        ConfChangeType::RemoveNode,
        new_peer(extra_store_id, extra_peer_id),
    );
    let req = new_admin_request(region_id, &region_epoch, cc);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(!res.get_header().has_error(), "{:?}", res);
    region_epoch.conf_ver += 1;

    // GC peer 4 using Voter peer state, peer 4 is learner because it's isolated.
    cluster.wait_peer_role(region_id, extra_store_id, extra_peer_id, Learner);
    let mut gc_msg = RaftMessage::default();
    gc_msg.set_region_id(region_id);
    gc_msg.set_from_peer(new_peer(1, 1));
    gc_msg.set_to_peer(new_peer(4, 4));
    gc_msg.set_region_epoch(region_epoch);
    gc_msg.set_is_tombstone(true);
    cluster.send_raft_msg(gc_msg).unwrap();
    cluster.wait_peer_state(region_id, 4, PeerState::Tombstone);
}

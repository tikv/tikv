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
<<<<<<< HEAD
    new_learner_peer, new_peer, sleep_ms, Filter, FilterFactory, Simulator as S1,
=======
    new_admin_request, new_change_peer_request, new_learner_peer, new_peer, Direction, Filter,
    FilterFactory, RegionPacketFilter, Simulator as S1,
>>>>>>> 6ec0b703e7 (*: fix issue of stale peer block resolve-ts cause by ignore gc message (#16505))
};
use test_raftstore_v2::Simulator as S2;
use tikv_util::{time::Instant, HandyRwLock};

struct ForwardFactory {
    node_id: u64,
    chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
    keep_msg: bool,
}

impl FilterFactory for ForwardFactory {
    fn generate(&self, _: u64) -> Vec<Box<dyn Filter>> {
        vec![Box::new(ForwardFilter {
            node_id: self.node_id,
            chain_send: self.chain_send.clone(),
            keep_msg: self.keep_msg,
        })]
    }
}

struct ForwardFilter {
    node_id: u64,
    chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
    keep_msg: bool,
}

impl Filter for ForwardFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        if self.keep_msg {
            for m in msgs {
                if self.node_id == m.get_to_peer().get_store_id() {
                    (self.chain_send)(m.clone());
                }
            }
        } else {
            for m in msgs.drain(..) {
                if self.node_id == m.get_to_peer().get_store_id() {
                    (self.chain_send)(m);
                }
            }
        }
        Ok(())
    }
}

// Create two clusters in v1 and v2, mock tiflash engine by adding tiflash
// labels to v1 cluster. Forwards v2 leader messages to v1 learner, and v1
// learner messages to v2 leaders.
// Make sure when removing learner, v2 leader can clean up removed_record and
// merged_record eventually.
#[test]
fn test_gc_peer_tiflash_engine() {
    let mut cluster_v1 = test_raftstore::new_node_cluster(1, 2);
    let mut cluster_v2 = test_raftstore_v2::new_node_cluster(1, 2);
    cluster_v1.cfg.raft_store.enable_v2_compatible_learner = true;
    cluster_v1.pd_client.disable_default_operator();
    cluster_v2.pd_client.disable_default_operator();
    let r11 = cluster_v1.run_conf_change();
    let r21 = cluster_v2.run_conf_change();

    // Add learner (2, 10).
    cluster_v1
        .pd_client
        .must_add_peer(r11, new_learner_peer(2, 10));
    cluster_v2
        .pd_client
        .must_add_peer(r21, new_learner_peer(2, 10));
    // Make sure learner states are match.
    let start = Instant::now();
    loop {
        if cluster_v1.get_raft_local_state(r11, 2).is_some()
            && cluster_v1.get_raft_local_state(r11, 2) == cluster_v2.get_raft_local_state(r21, 2)
            && cluster_v1.region_local_state(r11, 2).state == PeerState::Normal
            && cluster_v2.region_local_state(r21, 2).state == PeerState::Normal
            && cluster_v1.apply_state(r11, 2).truncated_state
                == cluster_v2.apply_state(r21, 2).truncated_state
        {
            break;
        }
        if start.saturating_elapsed() > Duration::from_secs(5) {
            panic!("timeout");
        }
    }

    let trans1 = Mutex::new(cluster_v1.sim.read().unwrap().get_router(2).unwrap());
    let trans2 = Mutex::new(cluster_v2.sim.read().unwrap().get_router(1).unwrap());

    // For cluster 1, it intercepts msgs sent to leader node, and then
    // forwards to cluster 2 leader node.
    let factory1 = ForwardFactory {
        node_id: 1,
        chain_send: Arc::new(move |m| {
            info!("send to trans2"; "msg" => ?m);
            let _ = trans2.lock().unwrap().send_raft_message(Box::new(m));
        }),
        keep_msg: false,
    };
    cluster_v1.add_send_filter(factory1);
    // For cluster 2, it intercepts msgs sent to learner node, and then
    // forwards to cluster 1 learner node.
    let factory2 = ForwardFactory {
        node_id: 2,
        chain_send: Arc::new(move |m| {
            info!("send to trans1"; "msg" => ?m);
            let _ = trans1.lock().unwrap().send_raft_message(m);
        }),
        keep_msg: false,
    };
    cluster_v2.add_send_filter(factory2);

    cluster_v2
        .pd_client
        .must_remove_peer(r21, new_learner_peer(2, 10));

    // Make sure leader cleans up removed_records.
    let start = Instant::now();
    loop {
        sleep_ms(500);
        if cluster_v2
            .region_local_state(r21, 1)
            .get_removed_records()
            .is_empty()
        {
            break;
        }
        if start.saturating_elapsed() > Duration::from_secs(5) {
            panic!("timeout");
        }
    }
}

#[test]
fn test_gc_removed_peer() {
    let mut cluster = test_raftstore::new_node_cluster(1, 2);
    cluster.cfg.raft_store.enable_v2_compatible_learner = true;
    cluster.pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();

    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    let factory = ForwardFactory {
        node_id: 1,
        chain_send: Arc::new(move |m| {
            if m.get_extra_msg().get_type() == ExtraMessageType::MsgGcPeerResponse {
                let _ = tx.lock().unwrap().send(m);
            }
        }),
        keep_msg: true,
    };
    cluster.add_send_filter(factory);

    let check_gc_peer = |to_peer: kvproto::metapb::Peer, timeout| -> bool {
        let epoch = cluster.get_region_epoch(region_id);
        let mut msg = RaftMessage::default();
        msg.set_is_tombstone(true);
        msg.set_region_id(region_id);
        msg.set_from_peer(new_peer(1, 1));
        msg.set_to_peer(to_peer.clone());
        msg.set_region_epoch(epoch.clone());
        let extra_msg = msg.mut_extra_msg();
        extra_msg.set_type(ExtraMessageType::MsgGcPeerRequest);
        let check_peer = extra_msg.mut_check_gc_peer();
        check_peer.set_from_region_id(region_id);
        check_peer.set_check_region_id(region_id);
        check_peer.set_check_peer(to_peer.clone());
        check_peer.set_check_region_epoch(epoch);

        cluster.sim.wl().send_raft_msg(msg.clone()).unwrap();
        let Ok(gc_resp) = rx.recv_timeout(timeout) else {
            return false;
        };
        assert_eq!(gc_resp.get_region_id(), region_id);
        assert_eq!(*gc_resp.get_from_peer(), to_peer);
        true
    };

    // Mock gc a peer that has been removed before creation.
    assert!(check_gc_peer(
        new_learner_peer(2, 5),
        Duration::from_secs(5)
    ));

    cluster
        .pd_client
        .must_add_peer(region_id, new_learner_peer(2, 4));
    // Make sure learner is created.
    cluster.wait_peer_state(region_id, 2, PeerState::Normal);

    cluster
        .pd_client
        .must_remove_peer(region_id, new_learner_peer(2, 4));
    // Make sure learner is removed.
    cluster.wait_peer_state(region_id, 2, PeerState::Tombstone);

    // Mock gc peer request. GC learner(2, 4).
    let start = Instant::now();
    loop {
        if check_gc_peer(new_learner_peer(2, 4), Duration::from_millis(200)) {
            return;
        }
        if start.saturating_elapsed() > Duration::from_secs(5) {
            break;
        }
    }
    assert!(check_gc_peer(
        new_learner_peer(2, 4),
        Duration::from_millis(200)
    ));
}

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

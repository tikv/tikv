// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use kvproto::raft_serverpb::{PeerState, RaftMessage};
use raftstore::errors::Result;
use test_raftstore::{new_learner_peer, sleep_ms, Filter, FilterFactory, Simulator as S1};
use test_raftstore_v2::Simulator as S2;
use tikv_util::time::Instant;

struct ForwardFactory {
    node_id: u64,
    chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
}

impl FilterFactory for ForwardFactory {
    fn generate(&self, _: u64) -> Vec<Box<dyn Filter>> {
        vec![Box::new(ForwardFilter {
            node_id: self.node_id,
            chain_send: self.chain_send.clone(),
        })]
    }
}

struct ForwardFilter {
    node_id: u64,
    chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
}

impl Filter for ForwardFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        for m in msgs.drain(..) {
            if self.node_id == m.get_to_peer().get_store_id() {
                (self.chain_send)(m);
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

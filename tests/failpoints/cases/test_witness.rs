// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use collections::HashMap;
use futures::executor::block_on;
use kvproto::{metapb, raft_serverpb::RaftApplyState};
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::{config::ReadableDuration, store::find_peer};

fn become_witness(cluster: &Cluster<ServerCluster>, region_id: u64, peer: &mut metapb::Peer) {
    peer.set_role(metapb::PeerRole::Learner);
    cluster.pd_client.must_add_peer(region_id, peer.clone());
    cluster.pd_client.must_remove_peer(region_id, peer.clone());
    peer.set_is_witness(true);
    peer.set_id(peer.get_id() + 10);
    cluster.pd_client.must_add_peer(region_id, peer.clone());
    peer.set_role(metapb::PeerRole::Voter);
    cluster.pd_client.must_add_peer(region_id, peer.clone());
}

// Test the case local reader works well with witness peer.
#[test]
fn test_witness_update_region_in_local_reader() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    become_witness(&cluster, region.get_id(), &mut peer_on_store3);

    cluster.must_put(b"k0", b"v0");

    // update region but the peer is not destroyed yet
    fail::cfg("change_peer_after_update_region_store_3", "pause").unwrap();

    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store3.clone());

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(b"k0")],
        false,
    );
    request.mut_header().set_peer(peer_on_store3);
    request.mut_header().set_replica_read(true);

    let resp = cluster
        .read(None, request.clone(), Duration::from_millis(100))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );

    fail::remove("change_peer_after_update_region_store_3");
}

// Test the case witness pull voter_replicated_index when has pending compact
// cmd.
#[test]
fn test_witness_raftlog_gc_pull_voter_replicated_index() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k0", b"v0");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    become_witness(&cluster, region.get_id(), &mut peer_on_store3);

    // make sure raft log gc is triggered
    std::thread::sleep(Duration::from_millis(200));
    let mut before_states = HashMap::default();
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        before_states.insert(id, state.take_truncated_state());
    }

    // one follower is down
    cluster.stop_node(nodes[1]);

    // write some data to make log gap exceeds the gc limit
    for i in 1..1000 {
        let (k, v) = (format!("k{}", i), format!("v{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
    }

    // the witness truncated index is not advanced
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        if id == 2 {
            assert_eq!(
                state.get_truncated_state().get_index() - before_states[&id].get_index(),
                0
            );
        } else {
            assert_ne!(
                900,
                state.get_truncated_state().get_index() - before_states[&id].get_index()
            );
        }
    }

    fail::cfg("on_raft_gc_log_tick", "return").unwrap();

    // the follower is back online
    cluster.run_node(nodes[1]).unwrap();
    cluster.must_put(b"k00", b"v00");
    must_get_equal(&cluster.get_engine(nodes[1]), b"k00", b"v00");
    // make sure raft log gc is triggered
    std::thread::sleep(Duration::from_millis(300));

    // the truncated index is advanced now, as all the peers has replicated
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        assert_ne!(
            900,
            state.get_truncated_state().get_index() - before_states[&id].get_index()
        );
    }
    fail::remove("on_raft_gc_log_tick");
}

// Test the case witness gc raftlog after reboot.
#[test]
fn test_witness_raftlog_gc_after_reboot() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k0", b"v0");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    become_witness(&cluster, region.get_id(), &mut peer_on_store3);

    // make sure raft log gc is triggered
    std::thread::sleep(Duration::from_millis(200));
    let mut before_states = HashMap::default();
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        before_states.insert(id, state.take_truncated_state());
    }

    // one follower is down
    cluster.stop_node(nodes[1]);

    // write some data to make log gap exceeds the gc limit
    for i in 1..1000 {
        let (k, v) = (format!("k{}", i), format!("v{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
    }

    // the witness truncated index is not advanced
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        if id == 2 {
            assert_eq!(
                state.get_truncated_state().get_index() - before_states[&id].get_index(),
                0
            );
        } else {
            assert_ne!(
                900,
                state.get_truncated_state().get_index() - before_states[&id].get_index()
            );
        }
    }

    fail::cfg("on_raft_gc_log_tick", "return").unwrap();

    // the follower is back online
    cluster.run_node(nodes[1]).unwrap();
    cluster.must_put(b"k00", b"v00");
    must_get_equal(&cluster.get_engine(nodes[1]), b"k00", b"v00");

    // the witness is down
    cluster.stop_node(nodes[2]);
    std::thread::sleep(Duration::from_millis(100));
    // the witness is back online
    cluster.run_node(nodes[2]).unwrap();

    // make sure raft log gc is triggered
    std::thread::sleep(Duration::from_millis(300));

    // the truncated index is advanced now, as all the peers has replicated
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        assert_ne!(
            900,
            state.get_truncated_state().get_index() - before_states[&id].get_index()
        );
    }
    fail::remove("on_raft_gc_log_tick");
}

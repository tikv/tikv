// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use collections::HashMap;
use futures::executor::block_on;
use kvproto::{metapb, raft_serverpb::RaftApplyState};
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::store::find_peer;

// Test the case that region split or merge with witness peer
#[test]
fn test_witness_split_merge() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    peer_on_store3.set_is_witness(true);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_split(&region, b"k2");

    // the newly split peer should be witness as well
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k2");
    assert_ne!(left.get_id(), right.get_id());
    assert!(find_peer(&left, nodes[2]).unwrap().is_witness);
    assert!(find_peer(&right, nodes[2]).unwrap().is_witness);

    pd_client.must_merge(left.get_id(), right.get_id());
    let after_merge = cluster.get_region(b"k1");
    assert!(find_peer(&after_merge, nodes[2]).unwrap().is_witness);
}

// Test flow of witness conf change
#[test]
fn test_witness_conf_change() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k1", b"v1");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    peer_on_store3.set_is_witness(true);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());

    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");

    // witness -> nonwitness
    peer_on_store3.set_role(metapb::PeerRole::Learner);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store3.clone());
    peer_on_store3.set_is_witness(false);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    std::thread::sleep(Duration::from_millis(100));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // add a new witness peer
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store3.clone());
    peer_on_store3.set_is_witness(true);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");
}

// Test the case that leader is forbidden to become witness
#[test]
fn test_witness_leader() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k1", b"v1");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // can't make leader to witness
    peer_on_store1.set_is_witness(true);
    cluster
        .pd_client
        .add_peer(region.get_id(), peer_on_store1.clone());

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        cluster.leader_of_region(region.get_id()).unwrap().store_id,
        1
    );
    // leader changes to witness failed, so still can get the value
    must_get_equal(&cluster.get_engine(nodes[0]), b"k1", b"v1");

    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    // can't transfer leader to witness
    cluster.transfer_leader(region.get_id(), peer_on_store3);
    assert_eq!(
        cluster.leader_of_region(region.get_id()).unwrap().store_id,
        nodes[0],
    );
}

// Test the case that witness can't be elected as leader based on election
// priority when there is no log gap
#[test]
fn test_witness_election_priority() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k0", b"v0");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    peer_on_store3.set_is_witness(true);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    for _ in 1..10 {
        let node = cluster.leader_of_region(region.get_id()).unwrap().store_id;
        cluster.stop_node(node);
        // the witness can't be elected as the leader when there is no log gap
        assert_ne!(
            cluster.leader_of_region(region.get_id()).unwrap().store_id,
            nodes[2],
        );
        cluster.run_node(node).unwrap();
    }
}

// Test the case that truncated index won't advance when there is a witness even
// if the gap gap exceeds the gc count limit
#[test]
fn test_witness_raftlog_gc_lagged_follower() {
    let mut cluster = new_server_cluster(0, 3);
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
    peer_on_store3.set_is_witness(true);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());

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

    // the truncated index is not advanced
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        assert!(state.get_truncated_state().get_index() - before_states[&id].get_index() < 10);
    }

    // the follower is back online
    cluster.run_node(nodes[1]).unwrap();

    cluster.must_put(b"k00", b"v00");
    // make sure raft log gc is triggered
    std::thread::sleep(Duration::from_millis(100));

    // the truncated index is advanced now, as all the peers has replicated
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        assert!(state.get_truncated_state().get_index() - before_states[&id].get_index() > 1000);
    }
}

// TODO: test witness is lagging behind, the gc is advanced

// Test the case that truncated index is advance when there is a witness even
// if the gap gap exceeds the gc count limit
#[test]
fn test_witness_raftlog_gc_lagged_witness() {
    let mut cluster = new_server_cluster(0, 3);
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
    peer_on_store3.set_is_witness(true);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());

    // make sure raft log gc is triggered
    std::thread::sleep(Duration::from_millis(200));
    let mut before_states = HashMap::default();
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        before_states.insert(id, state.take_truncated_state());
    }

    // the witness is down
    cluster.stop_node(nodes[2]);

    // write some data to make log gap exceeds the gc limit
    for i in 1..1000 {
        let (k, v) = (format!("k{}", i), format!("v{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
    }

    // make sure raft log gc is triggered
    std::thread::sleep(Duration::from_millis(100));

    // the truncated index is advanced
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        assert!(state.get_truncated_state().get_index() - before_states[&id].get_index() > 1000);
    }

    // the witness is back online
    cluster.run_node(nodes[2]).unwrap();

    cluster.must_put(b"k00", b"v00");
    must_get_equal(&cluster.get_engine(nodes[2]), b"k00", b"v00");
}

// TODO: test forbid stale read

// TODO: test with cdc

// TODO: test witness hasn't the log

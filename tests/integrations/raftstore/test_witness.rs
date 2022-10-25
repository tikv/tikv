// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use collections::HashMap;
use futures::executor::block_on;
use kvproto::{metapb, raft_serverpb::RaftApplyState};
use more_asserts::assert_ge;
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::store::find_peer;

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
    become_witness(&cluster, region.get_id(), &mut peer_on_store3);

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_split(&region, b"k2");
    must_get_none(&cluster.get_engine(3), b"k1");
    must_get_none(&cluster.get_engine(3), b"k2");

    // the newly split peer should be witness as well
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k2");
    assert_ne!(left.get_id(), right.get_id());
    assert!(find_peer(&left, nodes[2]).unwrap().is_witness);
    assert!(find_peer(&right, nodes[2]).unwrap().is_witness);

    pd_client.must_merge(left.get_id(), right.get_id());
    let after_merge = cluster.get_region(b"k1");
    assert!(find_peer(&after_merge, nodes[2]).unwrap().is_witness);
    must_get_none(&cluster.get_engine(3), b"k1");
    must_get_none(&cluster.get_engine(3), b"k2");
}

// TODO: add back when switch witness is supported
// // Test flow of witness conf change
// #[test]
// fn test_witness_conf_change() {
//     let mut cluster = new_server_cluster(0, 3);
//     cluster.run();
//     let nodes = Vec::from_iter(cluster.get_node_ids());
//     assert_eq!(nodes.len(), 3);

//     let pd_client = Arc::clone(&cluster.pd_client);
//     pd_client.disable_default_operator();

//     cluster.must_put(b"k1", b"v1");

//     let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
//     let peer_on_store1 = find_peer(&region, nodes[0]).unwrap();
//     cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

//     // nonwitness -> witness
//     let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
//     become_witness(&cluster, region.get_id(), &mut peer_on_store3);

//     std::thread::sleep(Duration::from_millis(100));
//     must_get_none(&cluster.get_engine(3), b"k1");

//     // witness -> nonwitness
//     peer_on_store3.set_role(metapb::PeerRole::Learner);
//     cluster
//         .pd_client
//         .must_add_peer(region.get_id(), peer_on_store3.clone());
//     cluster
//         .pd_client
//         .must_remove_peer(region.get_id(), peer_on_store3.clone());
//     peer_on_store3.set_is_witness(false);
//     cluster
//         .pd_client
//         .must_add_peer(region.get_id(), peer_on_store3.clone());
//     std::thread::sleep(Duration::from_millis(100));
//     must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

//     // add a new witness peer
//     cluster
//         .pd_client
//         .must_remove_peer(region.get_id(), peer_on_store3.clone());
//     peer_on_store3.set_is_witness(true);
//     cluster
//         .pd_client
//         .must_add_peer(region.get_id(), peer_on_store3.clone());
//     std::thread::sleep(Duration::from_millis(100));
//     must_get_none(&cluster.get_engine(3), b"k1");
// }

// TODO: add back when switch witness is supported
// // Test the case that leader is forbidden to become witness
// #[test]
// fn test_witness_leader() {
//     let mut cluster = new_server_cluster(0, 3);
//     cluster.run();
//     let nodes = Vec::from_iter(cluster.get_node_ids());
//     assert_eq!(nodes.len(), 3);

//     let pd_client = Arc::clone(&cluster.pd_client);
//     pd_client.disable_default_operator();

//     cluster.must_put(b"k1", b"v1");

//     let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
//     let mut peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
//     cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

//     // can't make leader to witness
//     peer_on_store1.set_is_witness(true);
//     cluster
//         .pd_client
//         .add_peer(region.get_id(), peer_on_store1.clone());

//     std::thread::sleep(Duration::from_millis(100));
//     assert_eq!(
//         cluster.leader_of_region(region.get_id()).unwrap().store_id,
//         1
//     );
//     // leader changes to witness failed, so still can get the value
//     must_get_equal(&cluster.get_engine(nodes[0]), b"k1", b"v1");

//     let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
//     // can't transfer leader to witness
//     cluster.transfer_leader(region.get_id(), &mut peer_on_store3);
//     assert_eq!(
//         cluster.leader_of_region(region.get_id()).unwrap().store_id,
//         nodes[0],
//     );
// }

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
    become_witness(&cluster, region.get_id(), &mut peer_on_store3);
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

    // the truncated index is not advanced
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        assert!(state.get_truncated_state().get_index() - before_states[&id].get_index() < 10);
    }

    // the follower is back online
    cluster.run_node(nodes[1]).unwrap();

    cluster.must_put(b"k00", b"v00");
    // make sure raft log gc is triggered
    std::thread::sleep(Duration::from_millis(200));

    // the truncated index is advanced now, as all the peers has replicated
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        assert_ge!(
            state.get_truncated_state().get_index() - before_states[&id].get_index(),
            900
        );
    }
}

// Test the case that truncated index is advance when there is a lagged witness
#[test]
fn test_witness_raftlog_gc_lagged_witness() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100);
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

    // the witness is back online
    cluster.run_node(nodes[2]).unwrap();

    cluster.must_put(b"k00", b"v00");
    std::thread::sleep(Duration::from_millis(200));

    // the truncated index is advanced
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        println!("{} {}", id, state.get_truncated_state().get_index());
        assert_ge!(
            state.get_truncated_state().get_index() - before_states[&id].get_index(),
            900
        );
    }
}

// Test the case replica read can't be performed on witness peer.
#[test]
fn test_witness_replica_read() {
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
    become_witness(&cluster, region.get_id(), &mut peer_on_store3);

    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(b"k0")],
        false,
    );
    request.mut_header().set_peer(peer_on_store3);
    request.mut_header().set_replica_read(true);

    let resp = cluster
        .call_command_on_node(nodes[2], request, Duration::from_millis(100))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );
}

fn must_get_error_recovery_in_progress<T: Simulator>(
    cluster: &mut Cluster<T>,
    region: &metapb::Region,
    cmd: kvproto::raft_cmdpb::Request,
) {
    let req = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![cmd],
        true,
    );
    let resp = cluster
        .call_command_on_leader(req, Duration::from_millis(100))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );
}

// Test the case that witness replicate logs to lagging behind follower when
// leader is down
#[test]
fn test_witness_leader_down() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k0", b"v0");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);

    let mut peer_on_store2 = find_peer(&region, nodes[1]).unwrap().clone();
    // nonwitness -> witness
    become_witness(&cluster, region.get_id(), &mut peer_on_store2);

    // the other follower is isolated
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    for i in 1..100 {
        cluster.must_put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes());
    }
    // the leader is down
    cluster.stop_node(1);

    // witness would help to replicate the logs
    cluster.clear_send_filters();

    // forbid writes
    let put = new_put_cmd(b"k3", b"v3");
    must_get_error_recovery_in_progress(&mut cluster, &region, put);
    // forbid reads
    let get = new_get_cmd(b"k1");
    must_get_error_recovery_in_progress(&mut cluster, &region, get);
    // forbid read index
    let read_index = new_read_index_cmd();
    must_get_error_recovery_in_progress(&mut cluster, &region, read_index);

    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store3);
    cluster.must_put(b"k1", b"v1");
    assert_eq!(
        cluster.leader_of_region(region.get_id()).unwrap().store_id,
        nodes[2],
    );
    assert_eq!(cluster.must_get(b"k99"), Some(b"v99".to_vec()));
}

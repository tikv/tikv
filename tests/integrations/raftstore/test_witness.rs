// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use futures::executor::block_on;
use kvproto::{metapb, pdpb};
use pd_client::PdClient;
use raft::eraftpb::{ConfChangeType, MessageType};
use raftstore::store::util::find_peer;
use test_raftstore::*;

#[test]
fn test_witness() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    cluster.must_put(b"k1", b"v1");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[1]).unwrap();
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

#[test]
fn test_witness_leader() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    cluster.must_put(b"k1", b"v1");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut peer_on_store1 = find_peer(&region, nodes[1]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // nonwitness -> witness
    peer_on_store1.set_is_witness(true);
    cluster
        .pd_client
        .add_peer(region.get_id(), peer_on_store1.clone());

    // change to witness failed
    std::thread::sleep(Duration::from_millis(100));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
}

#[test]
fn test_witness_auto() {
    test_util::init_log_for_test();
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    cluster.must_put(b"k1", b"v1");

    std::thread::sleep(Duration::from_millis(1000));
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let has_witness = region.get_peers().iter().any(|p| p.get_is_witness());
    assert!(has_witness);
    // must_get_none(&cluster.get_engine(3), b"k1");
}

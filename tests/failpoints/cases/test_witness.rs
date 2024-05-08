// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use futures::executor::block_on;
use kvproto::metapb;
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

// Test the case local reader works well with witness peer.
#[test]
fn test_witness_update_region_in_local_reader() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[2], 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    become_witness(&cluster, region.get_id(), &mut peer_on_store3);

    cluster.must_put(b"k0", b"v0");

    // witness region is about to be updated and destroyed but not yet
    fail::cfg("change_peer_before_update_region_store_3", "pause").unwrap();

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

    fail::remove("change_peer_before_update_region_store_3");
}

// This case is almost the same as `test_witness_update_region_in_local_reader`,
// but this omitted changing the peer to witness, for ensuring `peer_is_witness`
// won't be returned in a cluster without witnesses.
#[test]
fn test_witness_not_reported_while_disabled() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[2], 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();

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
    assert!(resp.get_header().has_error());
    assert!(!resp.get_header().get_error().has_recovery_in_progress());
    fail::remove("change_peer_after_update_region_store_3");
}

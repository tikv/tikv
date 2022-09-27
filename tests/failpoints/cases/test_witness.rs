// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use futures::executor::block_on;
use kvproto::metapb;
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::store::find_peer;

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
    peer_on_store2.set_is_witness(true);
    cluster
        .pd_client
        .add_peer(region.get_id(), peer_on_store2.clone());

    // the other follower is isolated
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    for i in 1..100 {
        cluster.must_put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes());
    }
    // the leader is down
    cluster.stop_node(1);

    fail::cfg("witness_transfer_leader", "return()").unwrap();
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

    fail::remove("witness_transfer_leader");
    std::thread::sleep(Duration::from_millis(500));
    cluster.must_put(b"k1", b"v1");
    assert_eq!(
        cluster.leader_of_region(region.get_id()).unwrap().store_id,
        nodes[2],
    );
    assert_eq!(cluster.must_get(b"k99"), Some(b"v99".to_vec()));
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Duration;

use futures::executor::block_on;
use kvproto::metapb;
use pd_client::PdClient;
use test_raftstore::*;

#[test]
fn test_unsafe_recover_update_region() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    configure_for_lease_read(&mut cluster, None, None);
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    cluster.must_wait_for_leader_expire(nodes[0], region.get_id());

    let mut update = metapb::Region::default();
    update.set_id(1);
    update.set_end_key(b"anykey2".to_vec());
    for p in region.get_peers() {
        if p.get_store_id() == nodes[0] {
            update.mut_peers().push(p.clone());
        }
    }
    update.mut_region_epoch().set_version(1);
    update.mut_region_epoch().set_conf_ver(1);
    // Removes the boostrap region, since it overlaps with any regions we create.
    cluster.must_update_region_for_unsafe_recover(nodes[0], &update);
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    assert_eq!(region.get_end_key(), b"anykey2");
}

#[test]
fn test_unsafe_recover_create_region() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    configure_for_lease_read(&mut cluster, None, None);
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    cluster.must_wait_for_leader_expire(nodes[0], region.get_id());

    let mut update = metapb::Region::default();
    update.set_id(1);
    update.set_end_key(b"anykey".to_vec());
    for p in region.get_peers() {
        if p.get_store_id() == nodes[0] {
            update.mut_peers().push(p.clone());
        }
    }
    update.mut_region_epoch().set_version(1);
    update.mut_region_epoch().set_conf_ver(1);
    // Removes the bootstrap region, since it overlaps with any regions we create.
    cluster.must_update_region_for_unsafe_recover(nodes[0], &update);
    block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut create = metapb::Region::default();
    create.set_id(101);
    create.set_start_key(b"anykey".to_vec());
    let mut peer = metapb::Peer::default();
    peer.set_id(102);
    peer.set_store_id(nodes[0]);
    create.mut_peers().push(peer);
    cluster.must_recreate_region_for_unsafe_recover(nodes[0], &create);
    let region = pd_client.get_region(b"anykey1").unwrap();
    assert_eq!(create.get_id(), region.get_id());
}

#[test]
fn test_force_leader() {
    let mut cluster = new_node_cluster(0, 5);

    cluster.run();
    cluster.must_put(b"k1", b"v1");
    cluster.must_transfer_leader(1, new_peer(5, 5));

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    let put = new_put_cmd(b"k2", b"v2");
    let mut region = cluster.get_region(b"k2");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    // marjority is lost, can't propose command successfully.
    assert!(
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .is_err()
    );

    cluster.enter_force_leader(1, 1);
    // remove the peers on failed nodes
    cluster.pd_client.must_remove_peer(1, new_peer(3, 3));
    cluster.pd_client.must_remove_peer(1, new_peer(4, 4));
    cluster.pd_client.must_remove_peer(1, new_peer(5, 5));
    let put = new_put_cmd(b"k3", b"v3");
    let mut region = cluster.get_region(b"k2");
    // forbid writes in force leader state
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    let resp = cluster
        .call_command_on_leader(req, Duration::from_millis(10))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: 1,
            ..Default::default()
        }
    );
    cluster.exit_force_leader(1, 1);

    // majority is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

#[test]
fn test_force_leader_for_learner() {
    let mut cluster = new_node_cluster(0, 5);

    cluster.run();
    cluster.must_put(b"k1", b"v1");
    cluster.must_transfer_leader(1, new_peer(5, 5));

    let region = cluster.get_region(b"k1");
    // replace one peer with learner
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), new_peer(1, 1));
    cluster
        .pd_client
        .must_add_peer(region.get_id(), new_learner_peer(1, 1));

    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    let put = new_put_cmd(b"k2", b"v2");
    let mut region = cluster.get_region(b"k2");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    // marjority is lost, can't propose command successfully.
    assert!(
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .is_err()
    );

    cluster.enter_force_leader(1, 1);
    // promote the learner first and remove the peers on failed nodes
    cluster.pd_client.must_add_peer(1, new_peer(1, 1));
    cluster.pd_client.must_remove_peer(1, new_peer(3, 3));
    cluster.pd_client.must_remove_peer(1, new_peer(4, 4));
    cluster.pd_client.must_remove_peer(1, new_peer(5, 5));
    let put = new_put_cmd(b"k3", b"v3");
    let mut region = cluster.get_region(b"k2");
    // forbid writes in force leader state
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    let resp = cluster
        .call_command_on_leader(req, Duration::from_millis(10))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: 1,
            ..Default::default()
        }
    );
    cluster.exit_force_leader(1, 1);

    // marjority is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
    cluster.must_transfer_leader(1, new_peer(1, 1));
}

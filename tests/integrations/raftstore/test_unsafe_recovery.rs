// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Duration;

use collections::HashSet;
use futures::executor::block_on;
use kvproto::{metapb, pdpb};
use pd_client::PdClient;
use raft::eraftpb::ConfChangeType;
use raftstore::store::util::find_peer;
use test_raftstore::*;

#[test]
fn test_unsafe_recovery_update_region() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let store0_peer = find_peer(&region, nodes[0]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store0_peer);
    configure_for_lease_read(&mut cluster, None, None);
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);

    let to_be_removed: Vec<metapb::Peer> = region
        .get_peers()
        .iter()
        .filter(|&peer| peer.get_store_id() != nodes[0])
        .map(|peer| peer.clone())
        .collect();
    let mut plan = pdpb::RecoveryPlan::default();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(region.get_id());
    demote.set_failed_voters(to_be_removed.into());
    plan.mut_demotes().push(demote);
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    // Removes the boostrap region, since it overlaps with any regions we create.
    let mut removed = false;
    for _ in 1..11 {
        let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == nodes[0] {
            removed = true;
            break;
        }
        sleep_ms(200);
    }
    assert_eq!(removed, true);
}

#[test]
fn test_unsafe_recovery_create_region() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let store0_peer = find_peer(&region, nodes[0]).unwrap().to_owned();

    // Removes the boostrap region, since it overlaps with any regions we create.
    pd_client.must_remove_peer(region.get_id(), store0_peer);
    cluster.must_remove_region(nodes[0], region.get_id());

    configure_for_lease_read(&mut cluster, None, None);
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    cluster.must_wait_for_leader_expire(nodes[0], region.get_id());

    let mut create = metapb::Region::default();
    create.set_id(101);
    create.set_start_key(b"anykey".to_vec());
    let mut peer = metapb::Peer::default();
    peer.set_id(102);
    peer.set_store_id(nodes[0]);
    create.mut_peers().push(peer);
    let mut plan = pdpb::RecoveryPlan::default();
    plan.mut_creates().push(create);
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);
    let mut created = false;
    for _ in 1..11 {
        let region = pd_client.get_region(b"anykey1").unwrap();
        if region.get_id() == 101 {
            created = true;
        }
        sleep_ms(200);
    }
    assert_eq!(created, true);
}

#[test]
fn test_force_leader_three_nodes() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let mut region = cluster.get_region(b"k2");
    let peer_on_store3 = find_peer(&region, 3).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store3.clone());

    cluster.stop_node(2);
    cluster.stop_node(3);

    let put = new_put_cmd(b"k2", b"v2");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    // marjority is lost, can't propose command successfully.
    assert!(
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .is_err()
    );

    cluster.enter_force_leader(region.get_id(), 1, HashSet::from_iter(vec![2, 3]));
    // remove the peers on failed nodes
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 2).unwrap().clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 3).unwrap().clone());
    // forbid writes in force leader state
    let put = new_put_cmd(b"k3", b"v3");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    let resp = cluster
        .call_command_on_leader(req, Duration::from_millis(10))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );
    // forbid reads in force leader state
    let get = new_get_cmd(b"k1");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![get], true);
    let resp = cluster
        .call_command_on_leader(req, Duration::from_millis(10))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );
    cluster.exit_force_leader(region.get_id(), 1);

    // majority is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

#[test]
fn test_force_leader_five_nodes() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let mut region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    let put = new_put_cmd(b"k2", b"v2");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    // marjority is lost, can't propose command successfully.
    assert!(
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .is_err()
    );

    cluster.enter_force_leader(region.get_id(), 1, HashSet::from_iter(vec![3, 4, 5]));
    // remove the peers on failed nodes
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 3).unwrap().clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 4).unwrap().clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 5).unwrap().clone());
    // forbid writes in force leader state
    let put = new_put_cmd(b"k3", b"v3");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    let resp = cluster
        .call_command_on_leader(req, Duration::from_millis(10))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );
    // forbid reads in force leader state
    let get = new_get_cmd(b"k1");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![get], true);
    let resp = cluster
        .call_command_on_leader(req, Duration::from_millis(10))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );
    cluster.exit_force_leader(region.get_id(), 1);

    // majority is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

#[test]
fn test_force_leader_for_learner() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let mut region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    let peer_on_store1 = find_peer(&region, 5).unwrap();
    // replace one peer with learner
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store1.clone());
    cluster.pd_client.must_add_peer(
        region.get_id(),
        new_learner_peer(peer_on_store1.get_id(), 1),
    );

    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    let put = new_put_cmd(b"k2", b"v2");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    // majority is lost, can't propose command successfully.
    assert!(
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .is_err()
    );

    cluster.enter_force_leader(region.get_id(), 1, HashSet::from_iter(vec![3, 4, 5]));
    // promote the learner first and remove the peers on failed nodes
    cluster
        .pd_client
        .must_add_peer(region.get_id(), find_peer(&region, 1).unwrap().clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 3).unwrap().clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 4).unwrap().clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 5).unwrap().clone());
    cluster.exit_force_leader(region.get_id(), 1);

    // marjority is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
    cluster.must_transfer_leader(region.get_id(), find_peer(&region, 1).unwrap().clone());
}

#[test]
fn test_force_leader_on_healthy_region() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    // try to enter force leader, it can't succeed due to majority isn't lost
    cluster.enter_force_leader(region.get_id(), 1, HashSet::from_iter(vec![3, 4, 5]));

    // put and get can propose successfully.
    assert_eq!(cluster.must_get(b"k1"), Some(b"v1".to_vec()));
    cluster.must_put(b"k2", b"v2");

    // try to exit force leader, it will be ignored silently as it's not in the force leader state
    cluster.exit_force_leader(region.get_id(), 1);

    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

#[test]
fn test_force_leader_on_wrong_leader() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let mut region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    // peer on node2 doesn't have latest committed log
    cluster.stop_node(2);
    cluster.must_put(b"k2", b"v2");

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);
    cluster.run_node(2).unwrap();

    // restart to clean lease
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();

    let put = new_put_cmd(b"k3", b"v3");
    let req = new_request(region.get_id(), region.take_region_epoch(), vec![put], true);
    // majority is lost, can't propose command successfully.
    assert!(
        cluster
            .call_command_on_leader(req, Duration::from_millis(100))
            .is_err()
    );

    // try to force leader on peer of node2 which is stale
    cluster.enter_force_leader(region.get_id(), 2, HashSet::from_iter(vec![3, 4, 5]));
    // can't propose confchange as it's not in force leader state
    let cmd = new_change_peer_request(
        ConfChangeType::RemoveNode,
        find_peer(&region, 3).unwrap().clone(),
    );
    let req = new_admin_request(region.get_id(), region.get_region_epoch(), cmd);
    assert!(
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .is_err()
    );
    cluster.exit_force_leader(region.get_id(), 2);

    // peer on node2 still doesn't have the latest committed log.
    must_get_none(&cluster.get_engine(2), b"k2");
}

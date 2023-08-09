// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use futures::executor::block_on;
use kvproto::{metapb, pdpb};
use pd_client::PdClient;
use raft::eraftpb::{ConfChangeType, MessageType};
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv_util::{config::ReadableDuration, store::find_peer, HandyRwLock};

macro_rules! confirm_quorum_is_lost {
    ($cluster:expr, $region:expr) => {{
        let put = new_put_cmd(b"k2", b"v2");
        let req = new_request(
            $region.get_id(),
            $region.get_region_epoch().clone(),
            vec![put],
            true,
        );
        // majority is lost, can't propose command successfully.
        $cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .unwrap_err();
    }};
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_demote_failed_voters() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    let peer_on_store2 = find_peer(&region, nodes[2]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store2.clone());
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);

    confirm_quorum_is_lost!(cluster, region);

    cluster.must_enter_force_leader(region.get_id(), nodes[0], vec![nodes[1], nodes[2]]);

    let to_be_removed: Vec<metapb::Peer> = region
        .get_peers()
        .iter()
        .filter(|&peer| peer.get_store_id() != nodes[0])
        .cloned()
        .collect();
    let mut plan = pdpb::RecoveryPlan::default();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(region.get_id());
    demote.set_failed_voters(to_be_removed.into());
    plan.mut_demotes().push(demote);
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    let mut demoted = true;
    for _ in 0..10 {
        let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

        demoted = true;
        for peer in region.get_peers() {
            if peer.get_id() != nodes[0] && peer.get_role() == metapb::PeerRole::Voter {
                demoted = false;
            }
        }
        if demoted {
            break;
        }
        sleep_ms(200);
    }
    assert!(demoted);
}

// Demote non-exist voters will not work, but TiKV should still report to PD.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_demote_non_exist_voters() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    let peer_on_store2 = find_peer(&region, nodes[2]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store2.clone());
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);

    confirm_quorum_is_lost!(cluster, region);
    cluster.must_enter_force_leader(region.get_id(), nodes[0], vec![nodes[1], nodes[2]]);

    let mut plan = pdpb::RecoveryPlan::default();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(region.get_id());
    let mut peer = metapb::Peer::default();
    peer.set_id(12345);
    peer.set_store_id(region.get_id());
    peer.set_role(metapb::PeerRole::Voter);
    demote.mut_failed_voters().push(peer);
    plan.mut_demotes().push(demote);
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    let mut store_report = None;
    for _ in 0..20 {
        store_report = pd_client.must_get_store_report(nodes[0]);
        if store_report.is_some() {
            break;
        }
        sleep_ms(100);
    }
    assert_ne!(store_report, None);
    let report = store_report.unwrap();
    let peer_reports = report.get_peer_reports();
    assert_eq!(peer_reports.len(), 1);
    let reported_region = peer_reports[0].get_region_state().get_region();
    assert_eq!(reported_region.get_id(), region.get_id());
    assert_eq!(reported_region.get_peers().len(), 3);
    let demoted = reported_region
        .get_peers()
        .iter()
        .any(|peer| peer.get_role() != metapb::PeerRole::Voter);
    assert_eq!(demoted, false);

    let region_in_pd = block_on(pd_client.get_region_by_id(region.get_id()))
        .unwrap()
        .unwrap();
    assert_eq!(region_in_pd.get_peers().len(), 3);
    let demoted = region_in_pd
        .get_peers()
        .iter()
        .any(|peer| peer.get_role() != metapb::PeerRole::Voter);
    assert_eq!(demoted, false);
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_auto_promote_learner() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    let peer_on_store0 = find_peer(&region, nodes[0]).unwrap();
    let peer_on_store2 = find_peer(&region, nodes[2]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store2.clone());
    // replace one peer with learner
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store0.clone());
    cluster.pd_client.must_add_peer(
        region.get_id(),
        new_learner_peer(nodes[0], cluster.pd_client.alloc_id().unwrap()),
    );
    // Sleep 100 ms to wait for the new learner to be initialized.
    sleep_ms(100);
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);

    confirm_quorum_is_lost!(cluster, region);
    cluster.must_enter_force_leader(region.get_id(), nodes[0], vec![nodes[1], nodes[2]]);

    let to_be_removed: Vec<metapb::Peer> = region
        .get_peers()
        .iter()
        .filter(|&peer| peer.get_store_id() != nodes[0])
        .cloned()
        .collect();
    let mut plan = pdpb::RecoveryPlan::default();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(region.get_id());
    demote.set_failed_voters(to_be_removed.into());
    plan.mut_demotes().push(demote);
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    let mut demoted = true;
    let mut promoted = false;
    for _ in 0..10 {
        let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

        promoted = region
            .get_peers()
            .iter()
            .find(|peer| peer.get_store_id() == nodes[0])
            .unwrap()
            .get_role()
            == metapb::PeerRole::Voter;

        demoted = region
            .get_peers()
            .iter()
            .filter(|peer| peer.get_store_id() != nodes[0])
            .all(|peer| peer.get_role() == metapb::PeerRole::Learner);
        if demoted && promoted {
            break;
        }
        sleep_ms(100);
    }
    assert!(demoted);
    assert!(promoted);
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_already_in_joint_state() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    let peer_on_store0 = find_peer(&region, nodes[0]).unwrap();
    let peer_on_store2 = find_peer(&region, nodes[2]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store2.clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store2.clone());
    let new_peer_id = cluster.pd_client.alloc_id().unwrap();
    cluster
        .pd_client
        .must_add_peer(region.get_id(), new_learner_peer(nodes[2], new_peer_id));
    // Wait the new learner to be initialized.
    sleep_ms(100);
    pd_client.must_joint_confchange(
        region.get_id(),
        vec![
            (
                ConfChangeType::AddLearnerNode,
                new_learner_peer(nodes[0], peer_on_store0.get_id()),
            ),
            (ConfChangeType::AddNode, new_peer(nodes[2], new_peer_id)),
        ],
    );
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    cluster.must_wait_for_leader_expire(nodes[0], region.get_id());

    confirm_quorum_is_lost!(cluster, region);
    cluster.must_enter_force_leader(region.get_id(), nodes[0], vec![nodes[1], nodes[2]]);

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let to_be_removed: Vec<metapb::Peer> = region
        .get_peers()
        .iter()
        .filter(|&peer| peer.get_store_id() != nodes[0])
        .cloned()
        .collect();
    let mut plan = pdpb::RecoveryPlan::default();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(region.get_id());
    demote.set_failed_voters(to_be_removed.into());
    plan.mut_demotes().push(demote);
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    let mut demoted = true;
    let mut promoted = false;
    for _ in 0..10 {
        let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

        promoted = region
            .get_peers()
            .iter()
            .find(|peer| peer.get_store_id() == nodes[0])
            .unwrap()
            .get_role()
            == metapb::PeerRole::Voter;

        demoted = region
            .get_peers()
            .iter()
            .filter(|peer| peer.get_store_id() != nodes[0])
            .all(|peer| peer.get_role() == metapb::PeerRole::Learner);
        if demoted && promoted {
            break;
        }
        sleep_ms(100);
    }
    assert!(demoted);
    assert!(promoted);
}

// Tests whether unsafe recovery behaves correctly when the failed region is
// already in the middle of a joint state, once exit, it recovers itself without
// any further demotions.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_early_return_after_exit_joint_state() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    // Changes the group config to
    let peer_on_store0 = find_peer(&region, nodes[0]).unwrap();
    let peer_on_store1 = find_peer(&region, nodes[1]).unwrap();
    let peer_on_store2 = find_peer(&region, nodes[2]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store2.clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store0.clone());
    let new_peer_id_store0 = cluster.pd_client.alloc_id().unwrap();
    cluster.pd_client.must_add_peer(
        region.get_id(),
        new_learner_peer(nodes[0], new_peer_id_store0),
    );
    let new_peer_id_store2 = cluster.pd_client.alloc_id().unwrap();
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store2.clone());
    cluster.pd_client.must_add_peer(
        region.get_id(),
        new_learner_peer(nodes[2], new_peer_id_store2),
    );
    // Wait the new learner to be initialized.
    sleep_ms(100);
    pd_client.must_joint_confchange(
        region.get_id(),
        vec![
            (
                ConfChangeType::AddNode,
                new_peer(nodes[0], new_peer_id_store0),
            ),
            (
                ConfChangeType::AddLearnerNode,
                new_learner_peer(nodes[1], peer_on_store1.get_id()),
            ),
        ],
    );
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    cluster.must_wait_for_leader_expire(nodes[0], region.get_id());

    confirm_quorum_is_lost!(cluster, region);
    cluster.must_enter_force_leader(region.get_id(), nodes[0], vec![nodes[1], nodes[2]]);

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let to_be_removed: Vec<metapb::Peer> = region
        .get_peers()
        .iter()
        .filter(|&peer| peer.get_store_id() != nodes[0])
        .cloned()
        .collect();
    let mut plan = pdpb::RecoveryPlan::default();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(region.get_id());
    demote.set_failed_voters(to_be_removed.into());
    plan.mut_demotes().push(demote);
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan);
    cluster.must_send_store_heartbeat(nodes[0]);

    let mut demoted = true;
    for _ in 0..10 {
        let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

        demoted = region
            .get_peers()
            .iter()
            .filter(|peer| peer.get_store_id() != nodes[0])
            .all(|peer| peer.get_role() == metapb::PeerRole::Learner);
        if demoted {
            break;
        }
        sleep_ms(100);
    }
    assert_eq!(demoted, true);
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_create_region() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let store0_peer = find_peer(&region, nodes[0]).unwrap().to_owned();

    // Removes the bootstrap region, since it overlaps with any regions we create.
    pd_client.must_remove_peer(region.get_id(), store0_peer);
    cluster.must_remove_region(nodes[0], region.get_id());

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

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_create_region_reentrancy() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let store0_peer = find_peer(&region, nodes[0]).unwrap().to_owned();

    // Removes the bootstrap region, since it overlaps with any regions we create.
    pd_client.must_remove_peer(region.get_id(), store0_peer);
    cluster.must_remove_region(nodes[0], region.get_id());

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
    plan.mut_creates().push(create.clone());
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

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_create_destroy_reentrancy() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    // Makes the leadership definite.
    let store2_peer = find_peer(&region, nodes[1]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store2_peer);
    cluster.put(b"random_key1", b"random_val1").unwrap();

    // Split the region into 2, and remove one of them, so that we can test both
    // region peer list update and region creation.
    pd_client.must_split_region(
        region,
        pdpb::CheckPolicy::Usekey,
        vec![b"random_key1".to_vec()],
    );
    let region1 = pd_client.get_region(b"random_key".as_ref()).unwrap();
    let region2 = pd_client.get_region(b"random_key1".as_ref()).unwrap();
    let region1_store0_peer = find_peer(&region1, nodes[0]).unwrap().to_owned();
    pd_client.must_remove_peer(region1.get_id(), region1_store0_peer);
    cluster.must_remove_region(nodes[0], region1.get_id());

    // Makes the group lose its quorum.
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    {
        let put = new_put_cmd(b"k2", b"v2");
        let req = new_request(
            region2.get_id(),
            region2.get_region_epoch().clone(),
            vec![put],
            true,
        );
        // marjority is lost, can't propose command successfully.
        cluster
            .call_command_on_leader(req, Duration::from_millis(10))
            .unwrap_err();
    }

    cluster.must_enter_force_leader(region2.get_id(), nodes[0], vec![nodes[1], nodes[2]]);

    // Construct recovery plan.
    let mut plan = pdpb::RecoveryPlan::default();

    let mut create = metapb::Region::default();
    create.set_id(101);
    create.set_end_key(b"random_key1".to_vec());
    let mut peer = metapb::Peer::default();
    peer.set_id(102);
    peer.set_store_id(nodes[0]);
    create.mut_peers().push(peer);
    plan.mut_creates().push(create);

    plan.mut_tombstones().push(region2.get_id());

    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan.clone());
    cluster.must_send_store_heartbeat(nodes[0]);
    sleep_ms(100);
    pd_client.must_set_unsafe_recovery_plan(nodes[0], plan.clone());
    cluster.must_send_store_heartbeat(nodes[0]);

    // Store reports are sent once the entries are applied.
    let mut store_report = None;
    for _ in 0..20 {
        store_report = pd_client.must_get_store_report(nodes[0]);
        if store_report.is_some() {
            break;
        }
        sleep_ms(100);
    }
    assert_ne!(store_report, None);
    let report = store_report.unwrap();
    let peer_reports = report.get_peer_reports();
    assert_eq!(peer_reports.len(), 1);
    let reported_region = peer_reports[0].get_region_state().get_region();
    assert_eq!(reported_region.get_id(), 101);
    assert_eq!(reported_region.get_peers().len(), 1);
    assert_eq!(reported_region.get_peers()[0].get_id(), 102);
}

macro_rules! must_get_error_recovery_in_progress {
    ($cluster:expr, $region:expr, $cmd:expr) => {
        let req = new_request(
            $region.get_id(),
            $region.get_region_epoch().clone(),
            vec![$cmd],
            true,
        );
        let resp = $cluster
            .call_command_on_leader(req, Duration::from_millis(100))
            .unwrap();
        assert_eq!(
            resp.get_header().get_error().get_recovery_in_progress(),
            &kvproto::errorpb::RecoveryInProgress {
                region_id: $region.get_id(),
                ..Default::default()
            }
        );
    };
}

// Test the case that two of three nodes fail and force leader on the rest node.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_three_nodes() {
    let mut cluster = new_cluster(0, 3);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store3 = find_peer(&region, 3).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store3.clone());

    cluster.stop_node(2);
    cluster.stop_node(3);

    // quorum is lost, can't propose command successfully.
    confirm_quorum_is_lost!(cluster, region);

    cluster.must_enter_force_leader(region.get_id(), 1, vec![2, 3]);
    // remove the peers on failed nodes
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 2).unwrap().clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), find_peer(&region, 3).unwrap().clone());
    // forbid writes in force leader state
    let put = new_put_cmd(b"k3", b"v3");
    must_get_error_recovery_in_progress!(cluster, region, put);
    // forbid reads in force leader state
    let get = new_get_cmd(b"k1");
    must_get_error_recovery_in_progress!(cluster, region, get);
    // forbid read index in force leader state
    let read_index = new_read_index_cmd();
    must_get_error_recovery_in_progress!(cluster, region, read_index);
    cluster.exit_force_leader(region.get_id(), 1);

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

// Test the case that three of five nodes fail and force leader on one of the
// rest nodes.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_five_nodes() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    // quorum is lost, can't propose command successfully.
    confirm_quorum_is_lost!(cluster, region);

    cluster.must_enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
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
    must_get_error_recovery_in_progress!(cluster, region, put);
    // forbid reads in force leader state
    let get = new_get_cmd(b"k1");
    must_get_error_recovery_in_progress!(cluster, region, get);
    // forbid read index in force leader state
    let read_index = new_read_index_cmd();
    must_get_error_recovery_in_progress!(cluster, region, read_index);

    cluster.exit_force_leader(region.get_id(), 1);

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

// Test the case that three of five nodes fail and force leader on the rest node
// which is a learner.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_for_learner() {
    let mut cluster = new_cluster(0, 5);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 5;
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(40);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    let peer_on_store1 = find_peer(&region, 1).unwrap();
    let new_learner = new_learner_peer(
        peer_on_store1.get_store_id(),
        cluster.pd_client.alloc_id().unwrap(),
    );
    // replace one peer with learner
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store1.clone());
    cluster
        .pd_client
        .must_add_peer(region.get_id(), new_learner.clone());
    // Sleep 100 ms to wait for the new learner to be initialized.
    sleep_ms(100);

    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    confirm_quorum_is_lost!(cluster, region);

    // wait election timeout
    std::thread::sleep(Duration::from_millis(
        cluster.cfg.raft_store.raft_election_timeout_ticks as u64
            * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * 2,
    ));
    cluster.must_enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
    // promote the learner first and remove the peers on failed nodes
    let new_peer = new_peer(new_learner.get_store_id(), new_learner.get_id());
    cluster
        .pd_client
        .must_add_peer(region.get_id(), new_peer.clone());
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

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
    cluster.must_transfer_leader(region.get_id(), new_peer);
}

// Test the case that three of five nodes fail and force leader on a hibernated
// previous leader.
#[test]
fn test_force_leader_on_hibernated_leader() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store1 = find_peer(&region, 1).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // wait a while to hibernate
    std::thread::sleep(Duration::from_millis(
        cluster.cfg.raft_store.raft_election_timeout_ticks as u64
            * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * 3,
    ));

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    cluster.must_enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
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
    cluster.exit_force_leader(region.get_id(), 1);

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

// Test the case that three of five nodes fail and force leader on a hibernated
// previous follower.
#[test]
fn test_force_leader_on_hibernated_follower() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    // wait a while to hibernate
    std::thread::sleep(Duration::from_millis(
        cluster.cfg.raft_store.raft_election_timeout_ticks as u64
            * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * 3,
    ));

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    cluster.must_enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
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
    cluster.exit_force_leader(region.get_id(), 1);

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

// Test the case that three of five nodes fail and force leader on the rest node
// with triggering snapshot.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_trigger_snapshot() {
    let mut cluster = new_cluster(0, 5);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 10;
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(90);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(8);
    cluster.cfg.raft_store.merge_max_log_gap = 3;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store1 = find_peer(&region, 1).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // Isolate node 2
    cluster.add_send_filter(IsolationFilterFactory::new(2));

    // Compact logs to force requesting snapshot after clearing send filters.
    let state = cluster.truncated_state(region.get_id(), 1);
    // Write some data to trigger snapshot.
    for i in 100..150 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }
    cluster.wait_log_truncated(region.get_id(), 1, state.get_index() + 40);

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    // Recover the isolation of 2, but still don't permit snapshot
    let recv_filter = Box::new(
        RegionPacketFilter::new(region.get_id(), 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgSnapshot),
    );
    cluster.sim.wl().add_recv_filter(2, recv_filter);
    cluster.clear_send_filters();

    // wait election timeout
    sleep_ms(
        cluster.cfg.raft_store.raft_election_timeout_ticks as u64
            * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * 5,
    );
    cluster.enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);

    sleep_ms(
        cluster.cfg.raft_store.raft_election_timeout_ticks as u64
            * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * 3,
    );
    let cmd = new_change_peer_request(
        ConfChangeType::RemoveNode,
        find_peer(&region, 3).unwrap().clone(),
    );
    let req = new_admin_request(region.get_id(), region.get_region_epoch(), cmd);
    // Though it has a force leader now, but the command can't committed because the
    // log is not replicated to all the alive peers.
    assert!(
        cluster
            .call_command_on_leader(req, Duration::from_millis(1000))
            .unwrap()
            .get_header()
            .has_error() /* error "there is a pending conf change" indicating no committed log
                          * after being the leader */
    );

    // Permit snapshot message, snapshot should be applied and advance commit index
    // now.
    cluster.sim.wl().clear_recv_filters(2);
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

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), None);
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
    cluster.must_transfer_leader(region.get_id(), find_peer(&region, 1).unwrap().clone());
}

// Test the case that three of five nodes fail and force leader on the rest node
// with uncommitted conf change.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_with_uncommitted_conf_change() {
    let mut cluster = new_cluster(0, 5);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 10;
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(90);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store1 = find_peer(&region, 1).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    confirm_quorum_is_lost!(cluster, region);

    // an uncommitted conf-change
    let cmd = new_change_peer_request(
        ConfChangeType::RemoveNode,
        find_peer(&region, 2).unwrap().clone(),
    );
    let req = new_admin_request(region.get_id(), region.get_region_epoch(), cmd);
    cluster
        .call_command_on_leader(req, Duration::from_millis(10))
        .unwrap_err();

    // wait election timeout
    std::thread::sleep(Duration::from_millis(
        cluster.cfg.raft_store.raft_election_timeout_ticks as u64
            * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * 8,
    ));
    cluster.must_enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
    // the uncommitted conf-change is committed successfully after being force
    // leader
    cluster
        .pd_client
        .must_none_peer(region.get_id(), find_peer(&region, 2).unwrap().clone());
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

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k2"), Some(b"v2".to_vec()));
    assert_eq!(cluster.must_get(b"k3"), None);
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

// Test the case that none of five nodes fails and force leader on one of the
// nodes. Note: It still can't defend extreme misuse cases. For example, a group
// of a, b and c. c is isolated from a, a is the leader. If c has increased its
// term by 2 somehow (for example false prevote success twice) and force leader
// is sent to b and break lease constrain, then b will reject a's heartbeat
// while can vote for c. So c becomes leader and there are two leaders in the
// group.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_on_healthy_region() {
    let mut cluster = new_cluster(0, 5);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(30);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 5;
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(40);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    // try to enter force leader, it can't succeed due to quorum isn't lost
    cluster.enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
    // make sure it leaves pre force leader state.
    std::thread::sleep(Duration::from_millis(
        cluster.cfg.raft_store.raft_election_timeout_ticks as u64
            * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * 3,
    ));
    // put and get can propose successfully.
    assert_eq!(cluster.must_get(b"k1"), Some(b"v1".to_vec()));
    cluster.must_put(b"k2", b"v2");

    // try to exit force leader, it will be ignored silently as it's not in the
    // force leader state
    cluster.exit_force_leader(region.get_id(), 1);

    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

// Test the case that three of five nodes fail and force leader on the one not
// having latest log
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_on_wrong_leader() {
    let mut cluster = new_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
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

    confirm_quorum_is_lost!(cluster, region);

    // try to force leader on peer of node2 which is stale
    cluster.must_enter_force_leader(region.get_id(), 2, vec![3, 4, 5]);
    // can't propose confchange as it's not in force leader state
    let cmd = new_change_peer_request(
        ConfChangeType::RemoveNode,
        find_peer(&region, 3).unwrap().clone(),
    );
    let req = new_admin_request(region.get_id(), region.get_region_epoch(), cmd);
    cluster
        .call_command_on_leader(req, Duration::from_millis(10))
        .unwrap_err();
    cluster.exit_force_leader(region.get_id(), 2);

    // peer on node2 still doesn't have the latest committed log.
    must_get_none(&cluster.get_engine(2), b"k2");
}

// Test the case that three of five nodes fail and force leader twice on
// peers on different nodes
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_twice_on_different_peers() {
    let mut cluster = new_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    // restart to clean lease
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    cluster.stop_node(2);
    cluster.run_node(2).unwrap();
    confirm_quorum_is_lost!(cluster, region);

    cluster.must_enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
    // enter force leader on a different peer
    cluster.enter_force_leader(region.get_id(), 2, vec![3, 4, 5]);
    assert_eq!(
        cluster.leader_of_region(region.get_id()).unwrap(),
        *find_peer(&region, 1).unwrap()
    );

    let conf_change = new_change_peer_request(ConfChangeType::RemoveNode, new_peer(3, 3));
    let mut req = new_admin_request(region.get_id(), region.get_region_epoch(), conf_change);
    req.mut_header()
        .set_peer(find_peer(&region, 2).unwrap().clone());
    let resp = cluster
        .call_command(req, Duration::from_millis(10))
        .unwrap();
    let mut not_leader = kvproto::errorpb::NotLeader {
        region_id: region.get_id(),
        ..Default::default()
    };
    not_leader.set_leader(find_peer(&region, 1).unwrap().clone());
    assert_eq!(resp.get_header().get_error().get_not_leader(), &not_leader,);

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
    cluster.exit_force_leader(region.get_id(), 1);

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

// Test the case that three of five nodes fail and force leader twice on
// peer on the same node
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_twice_on_same_peer() {
    let mut cluster = new_cluster(0, 5);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    // restart to clean lease
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    cluster.stop_node(2);
    cluster.run_node(2).unwrap();

    cluster.must_enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
    cluster.must_enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
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
    cluster.exit_force_leader(region.get_id(), 1);

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

// Test the case that three of five nodes fail and force leader doesn't finish
// in one election rounds due to network partition.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_force_leader_multiple_election_rounds() {
    let mut cluster = new_cluster(0, 5);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(30);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 5;
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(40);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k9");
    let region = cluster.get_region(b"k2");
    let peer_on_store5 = find_peer(&region, 5).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store5.clone());

    cluster.stop_node(3);
    cluster.stop_node(4);
    cluster.stop_node(5);

    cluster.add_send_filter(IsolationFilterFactory::new(1));
    cluster.add_send_filter(IsolationFilterFactory::new(2));

    // wait election timeout
    std::thread::sleep(Duration::from_millis(
        cluster.cfg.raft_store.raft_election_timeout_ticks as u64
            * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * 2,
    ));
    cluster.enter_force_leader(region.get_id(), 1, vec![3, 4, 5]);
    // wait multiple election rounds
    std::thread::sleep(Duration::from_millis(
        cluster.cfg.raft_store.raft_election_timeout_ticks as u64
            * cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * 6,
    ));

    cluster.clear_send_filters();
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
    cluster.exit_force_leader(region.get_id(), 1);

    // quorum is formed, can propose command successfully now
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.must_get(b"k4"), Some(b"v4".to_vec()));
}

// Tests whether unsafe recovery report sets has_commit_merge correctly.
// This field is used by PD to issue force leader command in order, so that the
// recovery process does not break the merge accidentally, when:
//   * The source region and the target region lost their quorum.
//   * The living peer(s) of the source region does not have prepare merge
//     message replicated.
//   * The living peer(s) of the target region has commit merge messages
//     replicated but uncommitted.
// If the living peer(s) of the source region in the above example enters force
// leader state before the peer(s) of the target region, thus proposes a no-op
// entry (while becoming the leader) which is conflict with part of the catch up
// logs, there will be data loss.
#[test_case(test_raftstore::new_node_cluster)]
// #[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_has_commit_merge() {
    let mut cluster = new_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    let left_on_store1 = find_peer(&left, 1).unwrap();
    cluster.must_transfer_leader(left.get_id(), left_on_store1.clone());
    let right_on_store1 = find_peer(&right, 1).unwrap();
    cluster.must_transfer_leader(right.get_id(), right_on_store1.clone());

    // Block the target region from receiving MsgAppendResponse, so that the commit
    // merge message will only be replicated but not committed.
    let recv_filter = Box::new(
        RegionPacketFilter::new(right.get_id(), 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppendResponse),
    );
    cluster.sim.wl().add_recv_filter(1, recv_filter);

    pd_client.merge_region(left.get_id(), right.get_id());
    // Wait until the commit merge is proposed.
    sleep_ms(300);
    // Send a empty recovery plan to trigger report.
    let plan = pdpb::RecoveryPlan::default();
    pd_client.must_set_unsafe_recovery_plan(1, plan);
    cluster.must_send_store_heartbeat(1);
    let mut store_report = None;
    for _ in 0..20 {
        store_report = pd_client.must_get_store_report(1);
        if store_report.is_some() {
            break;
        }
        sleep_ms(200);
    }
    assert_ne!(store_report, None);
    let mut has_commit_merge = false;
    for peer_report in store_report.unwrap().get_peer_reports().iter() {
        if peer_report.get_region_state().get_region().get_id() == right.get_id()
            && peer_report.get_has_commit_merge()
        {
            has_commit_merge = true;
        }
    }
    assert!(has_commit_merge);
}

#[test_case(test_raftstore::new_node_cluster)]
// #[test_case(test_raftstore_v2::new_node_cluster)]
fn test_unsafe_recovery_during_merge() {
    let mut cluster = new_cluster(0, 3);
    configure_for_merge(&mut cluster.cfg);

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    let left_on_store1 = find_peer(&left, 1).unwrap();
    cluster.must_transfer_leader(left.get_id(), left_on_store1.clone());
    let right_on_store1 = find_peer(&right, 1).unwrap();
    cluster.must_transfer_leader(right.get_id(), right_on_store1.clone());

    // Blocks the replication of prepare merge message, so that the commit merge
    // back fills it in CatchUpLogs.
    let append_filter = Box::new(
        RegionPacketFilter::new(left.get_id(), 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    );
    // Blocks the target region from receiving MsgAppendResponse, so that the commit
    // merge message will only be replicated but not committed.
    let commit_filter = Box::new(
        RegionPacketFilter::new(right.get_id(), 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppendResponse),
    );
    cluster.sim.wl().add_recv_filter(1, append_filter);
    cluster.sim.wl().add_recv_filter(1, commit_filter);

    pd_client.merge_region(left.get_id(), right.get_id());
    // Wait until the commit merge is proposed.
    sleep_ms(300);

    cluster.stop_node(1);
    cluster.stop_node(3);
    confirm_quorum_is_lost!(cluster, region);

    let report = cluster.must_enter_force_leader(right.get_id(), 2, vec![1, 3]);
    assert_eq!(report.get_peer_reports().len(), 1);
    let peer_report = &report.get_peer_reports()[0];
    assert_eq!(peer_report.get_has_commit_merge(), false);
    let region = peer_report.get_region_state().get_region();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(region.get_start_key().len(), 0);
    assert_eq!(region.get_end_key().len(), 0);

    let to_be_removed: Vec<metapb::Peer> = right
        .get_peers()
        .iter()
        .filter(|&peer| peer.get_store_id() != 2)
        .cloned()
        .collect();
    let mut plan = pdpb::RecoveryPlan::default();
    let mut demote = pdpb::DemoteFailedVoters::default();
    demote.set_region_id(right.get_id());
    demote.set_failed_voters(to_be_removed.into());
    plan.mut_demotes().push(demote);
    pd_client.must_set_unsafe_recovery_plan(2, plan);
    cluster.must_send_store_heartbeat(2);

    let mut demoted = true;
    for _ in 0..10 {
        let region = block_on(pd_client.get_region_by_id(right.get_id()))
            .unwrap()
            .unwrap();

        demoted = true;
        for peer in region.get_peers() {
            if peer.get_id() != 2 && peer.get_role() == metapb::PeerRole::Voter {
                demoted = false;
            }
        }
        if demoted {
            break;
        }
        sleep_ms(200);
    }
    assert!(demoted);
}

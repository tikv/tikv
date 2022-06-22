// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc, Arc},
    time::*,
};

use kvproto::{
    metapb::{self, PeerRole, Region},
    raft_cmdpb::{ChangePeerRequest, RaftCmdRequest, RaftCmdResponse},
};
use pd_client::PdClient;
use raft::eraftpb::ConfChangeType;
use raftstore::{store::util::find_peer, Result};
use test_raftstore::*;

/// Tests multiple confchange commands can be done by one request
#[test]
fn test_joint_consensus_conf_change() {
    let mut cluster = new_node_cluster(0, 4);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    assert_eq!(cluster.get(b"k1"), Some(b"v1".to_vec()));

    // add multiple nodes
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::AddNode, new_peer(2, 2)),
            (ConfChangeType::AddNode, new_peer(3, 3)),
            (ConfChangeType::AddLearnerNode, new_learner_peer(4, 4)),
        ],
    );
    pd_client.must_leave_joint(region_id);
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");

    // remove multiple nodes
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(3, 3)),
            (ConfChangeType::RemoveNode, new_learner_peer(4, 4)),
        ],
    );
    pd_client.must_leave_joint(region_id);
    assert_eq!(
        find_peer(&pd_client.get_region(b"").unwrap(), 3).unwrap(),
        &new_learner_peer(3, 3)
    );
    must_get_none(&cluster.get_engine(4), b"k1");

    // replace node
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::RemoveNode, new_learner_peer(3, 3)),
            (ConfChangeType::AddNode, new_peer(4, 5)),
        ],
    );
    pd_client.must_leave_joint(region_id);
    must_get_none(&cluster.get_engine(3), b"k1");
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");
}

/// Tests simple confchange will not enter joint state and when in joint
/// state any confchange request besides leave joint request should be rejected
#[test]
fn test_enter_joint_state() {
    let mut cluster = new_node_cluster(0, 4);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");

    // normal confchange request will not enter joint state
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    assert!(!pd_client.is_in_joint(region_id));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    assert!(!pd_client.is_in_joint(region_id));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // confchange_v2 request with one conchange request will not enter joint state
    pd_client.must_joint_confchange(
        region_id,
        vec![(ConfChangeType::RemoveNode, new_peer(3, 3))],
    );
    assert!(!pd_client.is_in_joint(region_id));
    must_get_none(&cluster.get_engine(3), b"k1");
    pd_client.must_joint_confchange(region_id, vec![(ConfChangeType::AddNode, new_peer(3, 3))]);
    assert!(!pd_client.is_in_joint(region_id));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Enter joint
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(3, 3)),
            (ConfChangeType::AddNode, new_peer(4, 4)),
        ],
    );
    assert!(pd_client.is_in_joint(region_id));

    // In joint state any confchange request besides leave joint request
    // will be rejected
    let resp = call_conf_change(
        &mut cluster,
        region_id,
        ConfChangeType::RemoveNode,
        new_learner_peer(3, 3),
    )
    .unwrap();
    must_contains_error(&resp, "in joint");

    let resp = call_conf_change_v2(
        &mut cluster,
        region_id,
        vec![change_peer(
            ConfChangeType::RemoveNode,
            new_learner_peer(3, 3),
        )],
    )
    .unwrap();
    must_contains_error(&resp, "in joint");

    // Leave joint
    pd_client.must_leave_joint(region_id);
}

/// Tests when in joint state, normal request can be handled as usual
#[test]
fn test_request_in_joint_state() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_learner_peer(3, 3));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Enter joint, now we have C_old(1, 2) and C_new(1, 3)
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(2, 2)),
            (ConfChangeType::AddNode, new_peer(3, 3)),
        ],
    );

    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    // Both new and old configuation have the newest log
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");

    let region = cluster.get_region(b"k1");

    // Isolated peer 2, so the old configuation can't reach quorum
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    let rx = cluster
        .async_request(put_request(&region, 1, b"k3", b"v3"))
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    cluster.clear_send_filters();

    // Isolated peer 3, so the new configuation can't reach quorum
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    let rx = cluster
        .async_request(put_request(&region, 1, b"k4", b"v4"))
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    cluster.clear_send_filters();

    // Leave joint
    pd_client.must_leave_joint(region_id);

    // Isolated peer 2, but it is not in quorum any more
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    cluster.must_put(b"k5", b"v5");
    must_get_equal(&cluster.get_engine(3), b"k5", b"v5");
}

/// Tests when replace peer, request can be handled as usual even two nodes
/// crashed, and the leader can be replaced when in joint state
#[test]
fn test_joint_replace_peers() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.cfg.raft_store.allow_remove_leader = false;

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Replace peers
    // Enter joint, now we have C_old(1, 2, 3) and C_new(1, 2, 4)
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(3, 3)),
            (ConfChangeType::AddNode, new_peer(4, 4)),
        ],
    );
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");

    // Isolated node 3 and node 4
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    cluster.add_send_filter(IsolationFilterFactory::new(4));

    // Request can be handle as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    must_get_none(&cluster.get_engine(3), b"k2");
    must_get_none(&cluster.get_engine(4), b"k2");

    // Leave joint
    pd_client.must_leave_joint(region_id);

    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(4), b"k2", b"v2");

    // Replace leader
    // Enter joint, now we have C_old(1, 2, 4) and C_new(2, 4, 5)
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(1, 1)),
            (ConfChangeType::AddNode, new_peer(5, 5)),
        ],
    );
    {
        // The leader now is DemotingVoter
        let mut peer = new_peer(1, 1);
        peer.set_role(PeerRole::DemotingVoter);
        pd_client.region_leader_must_be(region_id, peer);
    }
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(5), b"k3", b"v3");

    cluster.must_transfer_leader(region_id, new_peer(5, 5));

    cluster.must_put(b"k4", b"v4");
    for id in 1..=5 {
        must_get_equal(&cluster.get_engine(id), b"k4", b"v4");
    }

    // Leave joint
    pd_client.must_leave_joint(region_id);
}

/// Tests invalid confchange request should be rejected
#[test]
fn test_invalid_confchange_request() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.allow_remove_leader = false;

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    let region = cluster.get_region(b"");

    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_learner_peer(3, 3));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Can not remove voter directly in joint confchange request
    let resp = call_conf_change_v2(
        &mut cluster,
        region_id,
        vec![
            change_peer(ConfChangeType::RemoveNode, new_peer(2, 2)),
            change_peer(ConfChangeType::AddLearnerNode, new_learner_peer(4, 4)),
        ],
    )
    .unwrap();
    must_contains_error(&resp, "can not remove voter");

    // Can not have multiple commands for the same peer
    let resp = call_conf_change_v2(
        &mut cluster,
        region_id,
        vec![
            change_peer(ConfChangeType::AddLearnerNode, new_learner_peer(2, 2)),
            change_peer(ConfChangeType::RemoveNode, new_learner_peer(2, 2)),
        ],
    )
    .unwrap();
    must_contains_error(&resp, "multiple commands for the same peer");

    // Can not have multiple changes that only effect learner
    let resp = call_conf_change_v2(
        &mut cluster,
        region_id,
        vec![
            change_peer(ConfChangeType::RemoveNode, new_learner_peer(3, 3)),
            change_peer(ConfChangeType::AddLearnerNode, new_learner_peer(4, 4)),
        ],
    )
    .unwrap();
    must_contains_error(&resp, "multiple changes that only effect learner");

    // Can not demote leader with simple confchange
    let resp = call_conf_change_v2(
        &mut cluster,
        region_id,
        vec![change_peer(
            ConfChangeType::AddLearnerNode,
            new_learner_peer(1, 1),
        )],
    )
    .unwrap();
    must_contains_error(&resp, "ignore remove leader or demote leader");

    let resp = call_conf_change(
        &mut cluster,
        region_id,
        ConfChangeType::AddLearnerNode,
        new_learner_peer(1, 1),
    )
    .unwrap();
    must_contains_error(&resp, "ignore remove leader or demote leader");

    // Can not leave a non-joint config
    let resp = leave_joint(&mut cluster, region_id).unwrap();
    must_contains_error(&resp, "leave a non-joint config");

    // Split region
    cluster.must_split(&region, b"k3");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k5").unwrap();
    assert_eq!(region_id, right.get_id());
    // Enter joint
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(2, 2)),
            (ConfChangeType::AddNode, new_peer(3, 3)),
        ],
    );
    assert!(pd_client.is_in_joint(region_id));

    // Can not merge region while in jonit state
    let resp = cluster.try_merge(right.get_id(), left.get_id());
    must_contains_error(&resp, "in joint state, can not propose merge command");

    // Can not leave joint if which will demote leader
    cluster.must_transfer_leader(region_id, new_peer(2, 2));
    let resp = leave_joint(&mut cluster, region_id).unwrap();
    must_contains_error(&resp, "ignore leave joint command that demoting leader");
}

/// Tests when leader restart in joint state, joint state should be the same
/// as before
#[test]
fn test_restart_in_joint_state() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");

    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_learner_peer(3, 3));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Enter joint
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(2, 2)),
            (ConfChangeType::AddNode, new_peer(3, 3)),
        ],
    );
    assert!(pd_client.is_in_joint(region_id));

    cluster.stop_node(1);
    sleep_ms(50);

    cluster.run_node(1).unwrap();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Still in joint state
    assert!(pd_client.is_in_joint(region_id));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");

    // Leave joint
    pd_client.must_leave_joint(region_id);

    // Joint confchange finished
    let region = cluster.get_region(b"k2");
    must_has_peer(&region, 1, PeerRole::Voter);
    must_has_peer(&region, 2, PeerRole::Learner);
    must_has_peer(&region, 3, PeerRole::Voter);
}

/// Tests when leader down in joint state, both peers in new configuration and
/// old configuration can become the new leader
#[test]
fn test_leader_down_in_joint_state() {
    let mut cluster = new_node_cluster(0, 5);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    pd_client.must_add_peer(region_id, new_learner_peer(4, 4));
    pd_client.must_add_peer(region_id, new_learner_peer(5, 5));
    for i in 2..=5 {
        must_get_equal(&cluster.get_engine(i), b"k1", b"v1");
    }

    // Enter joint, now we have C_old(1, 2, 3) and C_new(1, 4, 5)
    pd_client.must_joint_confchange(
        region_id,
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(2, 2)),
            (ConfChangeType::AddLearnerNode, new_learner_peer(3, 3)),
            (ConfChangeType::AddNode, new_peer(4, 4)),
            (ConfChangeType::AddNode, new_peer(5, 5)),
        ],
    );
    cluster.must_put(b"k2", b"v2");
    for i in 2..=5 {
        must_get_equal(&cluster.get_engine(i), b"k2", b"v2");
    }

    // Isolated leader
    cluster.add_send_filter(IsolationFilterFactory::new(1));
    sleep_ms(500);

    // Peer from both configuration can become leader
    for leader_id in &[3, 4] {
        let (k, v) = (format!("k{}", leader_id), format!("v{}", leader_id));
        cluster.must_transfer_leader(region_id, new_peer(*leader_id, *leader_id));
        cluster.must_put(k.as_bytes(), v.as_bytes());

        for i in 2..=5 {
            must_get_equal(&cluster.get_engine(i), k.as_bytes(), v.as_bytes());
        }
    }

    // Leave joint
    pd_client.must_leave_joint(region_id);

    // Joint confchange finished
    let region = cluster.get_region(b"k1");
    must_has_peer(&region, 1, PeerRole::Voter);
    must_has_peer(&region, 2, PeerRole::Learner);
    must_has_peer(&region, 3, PeerRole::Learner);
    must_has_peer(&region, 4, PeerRole::Voter);
    must_has_peer(&region, 5, PeerRole::Voter);
}

fn call_conf_change_v2<T>(
    cluster: &mut Cluster<T>,
    region_id: u64,
    changes: Vec<ChangePeerRequest>,
) -> Result<RaftCmdResponse>
where
    T: Simulator,
{
    let conf_change = new_change_peer_v2_request(changes);
    let epoch = cluster.pd_client.get_region_epoch(region_id);
    let admin_req = new_admin_request(region_id, &epoch, conf_change);
    cluster.call_command_on_leader(admin_req, Duration::from_secs(3))
}

fn call_conf_change<T>(
    cluster: &mut Cluster<T>,
    region_id: u64,
    conf_change_type: ConfChangeType,
    peer: metapb::Peer,
) -> Result<RaftCmdResponse>
where
    T: Simulator,
{
    let conf_change = new_change_peer_request(conf_change_type, peer);
    let epoch = cluster.pd_client.get_region_epoch(region_id);
    let admin_req = new_admin_request(region_id, &epoch, conf_change);
    cluster.call_command_on_leader(admin_req, Duration::from_secs(3))
}

fn leave_joint<T>(cluster: &mut Cluster<T>, region_id: u64) -> Result<RaftCmdResponse>
where
    T: Simulator,
{
    call_conf_change_v2(cluster, region_id, vec![])
}

fn change_peer(conf_change_type: ConfChangeType, peer: metapb::Peer) -> ChangePeerRequest {
    let mut cp = ChangePeerRequest::default();
    cp.set_change_type(conf_change_type);
    cp.set_peer(peer);
    cp
}

fn put_request(region: &Region, id: u64, key: &[u8], val: &[u8]) -> RaftCmdRequest {
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_put_cf_cmd("default", key, val)],
        false,
    );
    request.mut_header().set_peer(new_peer(id, id));
    request
}

fn must_has_peer(region: &Region, peer_id: u64, role: PeerRole) {
    assert!(
        region
            .get_peers()
            .iter()
            .any(|p| p.get_id() == peer_id && p.get_role() == role)
    );
}

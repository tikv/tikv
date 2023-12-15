// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    iter::FromIterator,
    sync::{Arc, Mutex},
    time::Duration,
};

use collections::HashMap;
use futures::executor::block_on;
use kvproto::{
    metapb,
    raft_cmdpb::ChangePeerRequest,
    raft_serverpb::{PeerState, RaftApplyState},
};
use pd_client::PdClient;
use raft::eraftpb::{ConfChangeType, MessageType};
use test_raftstore::*;
use tikv_util::{
    config::ReadableDuration,
    store::{find_peer, new_witness_peer},
    HandyRwLock,
};

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
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );
    let before = cluster
        .apply_state(region.get_id(), nodes[2])
        .get_applied_index();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_split(&region, b"k2");
    must_get_none(&cluster.get_engine(3), b"k1");
    must_get_none(&cluster.get_engine(3), b"k2");
    // applied index of witness is updated
    let after = cluster
        .apply_state(region.get_id(), nodes[2])
        .get_applied_index();
    assert!(after - before >= 3);

    // the newly split peer should be witness as well
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k2");
    assert_ne!(left.get_id(), right.get_id());
    assert!(find_peer(&left, nodes[2]).unwrap().is_witness);
    assert!(find_peer(&right, nodes[2]).unwrap().is_witness);

    // merge
    pd_client.must_merge(left.get_id(), right.get_id());
    let after_merge = cluster.get_region(b"k1");
    assert!(find_peer(&after_merge, nodes[2]).unwrap().is_witness);
    must_get_none(&cluster.get_engine(3), b"k1");
    must_get_none(&cluster.get_engine(3), b"k2");
    // epoch of witness is updated
    assert_eq!(
        cluster
            .region_local_state(after_merge.get_id(), nodes[2])
            .get_region()
            .get_region_epoch(),
        after_merge.get_region_epoch()
    );

    // split again
    cluster.must_split(&after_merge, b"k2");
    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k2");
    assert!(find_peer(&left, nodes[2]).unwrap().is_witness);
    assert!(find_peer(&right, nodes[2]).unwrap().is_witness);

    // can't merge with different witness location
    let peer_on_store3 = find_peer(&left, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        left.get_id(),
        vec![peer_on_store3.get_id()],
        vec![false],
    );
    let left = cluster.get_region(b"k1");
    let req = new_admin_request(
        left.get_id(),
        left.get_region_epoch(),
        new_prepare_merge(right),
    );
    let resp = cluster
        .call_command_on_leader(req, Duration::from_millis(100))
        .unwrap();
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("peers doesn't match")
    );
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

    // can't switch witness by conf change
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    let mut peer = peer_on_store3.clone();
    peer.set_is_witness(true);
    let mut cp = ChangePeerRequest::default();
    cp.set_change_type(ConfChangeType::AddLearnerNode);
    cp.set_peer(peer);
    let req = new_admin_request(
        region.get_id(),
        region.get_region_epoch(),
        new_change_peer_v2_request(vec![cp]),
    );
    let resp = cluster
        .call_command_on_leader(req, Duration::from_millis(100))
        .unwrap();
    assert!(resp.get_header().has_error());

    // add a new witness peer
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store3.clone());
    peer_on_store3.set_is_witness(true);
    let applied_index = cluster.apply_state(1, 2).applied_index;
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    must_get_none(&cluster.get_engine(3), b"k1");
    let region = cluster.get_region(b"k1");
    cluster.wait_applied_index(region.get_id(), nodes[2], applied_index + 1);
    assert_eq!(
        cluster
            .region_local_state(region.get_id(), nodes[2])
            .get_region(),
        &region
    );

    // remove a witness peer
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store3);

    std::thread::sleep(Duration::from_millis(10));

    assert_eq!(
        cluster
            .region_local_state(region.get_id(), nodes[2])
            .get_state(),
        PeerState::Tombstone
    );
}

// Test flow of switch witness
#[test]
fn test_witness_switch_witness() {
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
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");

    // witness -> non-witness
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![false],
    );

    std::thread::sleep(Duration::from_millis(100));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
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
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // can't make leader to witness
    cluster
        .pd_client
        .switch_witnesses(region.get_id(), vec![peer_on_store1.get_id()], vec![true]);

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

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();

    // nonwitness -> witness
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );
    cluster.must_put(b"k0", b"v0");

    // make sure logs are replicated to the witness
    std::thread::sleep(Duration::from_millis(100));

    for i in 1..10 {
        let node = cluster.leader_of_region(region.get_id()).unwrap().store_id;
        cluster.stop_node(node);
        let (k, v) = (format!("k{}", i), format!("v{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
        // the witness can't be elected as the leader when there is no log gap
        assert_ne!(
            cluster.leader_of_region(region.get_id()).unwrap().store_id,
            nodes[2],
        );
        cluster.run_node(node).unwrap();
        // make sure logs are replicated to the restarted node
        std::thread::sleep(Duration::from_millis(100));
    }
}

// Test the case that truncated index won't advance when there is a witness even
// if the gap gap exceeds the gc count limit
#[test]
fn test_witness_raftlog_gc_lagged_follower() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100);
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
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

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
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );
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
        assert_ne!(
            900,
            state.get_truncated_state().get_index() - before_states[&id].get_index()
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
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

    // make sure the peer_on_store3 has completed applied to witness
    std::thread::sleep(Duration::from_millis(200));

    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(b"k0")],
        false,
    );
    request.mut_header().set_peer(peer_on_store3);
    request.mut_header().set_replica_read(true);

    let resp = cluster
        .read(None, None, request, Duration::from_millis(100))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_is_witness(),
        &kvproto::errorpb::IsWitness {
            region_id: region.get_id(),
            ..Default::default()
        }
    );
}

fn must_get_error_is_witness<T: Simulator>(
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
        resp.get_header().get_error().get_is_witness(),
        &kvproto::errorpb::IsWitness {
            region_id: region.get_id(),
            ..Default::default()
        },
        "{:?}",
        resp
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

    let peer_on_store2 = find_peer(&region, nodes[1]).unwrap().clone();
    // nonwitness -> witness
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store2.get_id()],
        vec![true],
    );

    // the other follower is isolated
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    for i in 1..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes());
    }
    // the leader is down
    cluster.stop_node(1);

    // witness would help to replicate the logs
    cluster.clear_send_filters();

    // forbid writes
    let put = new_put_cmd(b"k3", b"v3");
    must_get_error_is_witness(&mut cluster, &region, put);
    // forbid reads
    let get = new_get_cmd(b"k1");
    must_get_error_is_witness(&mut cluster, &region, get);
    // forbid read index
    let read_index = new_read_index_cmd();
    must_get_error_is_witness(&mut cluster, &region, read_index);

    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store3);
    cluster.must_put(b"k1", b"v1");
    assert_eq!(
        cluster.leader_of_region(region.get_id()).unwrap().store_id,
        nodes[2],
    );
    assert_eq!(cluster.must_get(b"k9"), Some(b"v9".to_vec()));
}

// Test the case that witness ignore consistency check as it has no data
#[test]
fn test_witness_ignore_consistency_check() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 50;
    // disable compact log to make test more stable.
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    cluster.cfg.raft_store.consistency_check_interval = ReadableDuration::secs(1);
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
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

    // make sure the peer_on_store3 has completed applied to witness
    std::thread::sleep(Duration::from_millis(200));

    for i in 0..300 {
        cluster.must_put(
            format!("k{:06}", i).as_bytes(),
            format!("k{:06}", i).as_bytes(),
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

// Test the case that witness apply snapshot with network isolation
#[test]
fn test_witness_apply_snapshot_with_network_isolation() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_snapshot(&mut cluster.cfg);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_witness_peer(3, 3));
    // Ensure all peers are initialized.
    std::thread::sleep(Duration::from_millis(100));

    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.add_send_filter(IsolationFilterFactory::new(3));

    for i in 0..20 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }
    sleep_ms(500);

    // Ignore witness's MsgAppendResponse, after applying snaphost
    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let recv_filter = Box::new(
        RegionPacketFilter::new(r1, 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppendResponse)
            .reserve_dropped(Arc::clone(&dropped_msgs)),
    );
    cluster.sim.wl().add_recv_filter(1, recv_filter);

    cluster.clear_send_filters();
    // Wait for leader send snapshot.
    sleep_ms(500);

    cluster.sim.wl().clear_recv_filters(1);

    // Witness's ProgressState must have been changed to Probe
    cluster.must_transfer_leader(1, new_peer(2, 2));

    for i in 20..25 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use collections::HashMap;
use engine_rocks::RocksEngine;
use futures::executor::block_on;
use kvproto::{metapb, raft_serverpb::RaftApplyState};
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::{config::ReadableDuration, store::find_peer};

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
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

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
        .read(None, None, request.clone(), Duration::from_millis(100))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_is_witness(),
        &kvproto::errorpb::IsWitness {
            region_id: region.get_id(),
            ..Default::default()
        }
    );

    fail::remove("change_peer_after_update_region_store_3");
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
        .read(None, None, request.clone(), Duration::from_millis(100))
        .unwrap();
    assert!(resp.get_header().has_error());
    assert!(!resp.get_header().get_error().has_is_witness());
    fail::remove("change_peer_after_update_region_store_3");
}

// Test the case witness pull voter_replicated_index when has pending compact
// cmd.
#[test]
fn test_witness_raftlog_gc_pull_voter_replicated_index() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    cluster
        .cfg
        .raft_store
        .request_voter_replicated_index_interval = ReadableDuration::millis(100);
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

    fail::cfg("on_raft_gc_log_tick", "return").unwrap();

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
    fail::remove("on_raft_gc_log_tick");
}

// Test the case witness gc raftlog after reboot.
#[test]
fn test_witness_raftlog_gc_after_reboot() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    cluster
        .cfg
        .raft_store
        .request_voter_replicated_index_interval = ReadableDuration::millis(100);
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

    fail::cfg("on_raft_gc_log_tick", "return").unwrap();

    // the follower is back online
    cluster.run_node(nodes[1]).unwrap();
    cluster.must_put(b"k00", b"v00");
    must_get_equal(&cluster.get_engine(nodes[1]), b"k00", b"v00");

    // the witness is down
    cluster.stop_node(nodes[2]);
    std::thread::sleep(Duration::from_millis(100));
    // the witness is back online
    cluster.run_node(nodes[2]).unwrap();

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
    fail::remove("on_raft_gc_log_tick");
}

// Test the case request snapshot and apply successfully after non-witness
// restart.
#[test]
fn test_request_snapshot_after_reboot() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.check_request_snapshot_interval = ReadableDuration::millis(20);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

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

    cluster.must_put(b"k1", b"v1");

    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");

    // witness -> nonwitness
    let fp = "ignore request snapshot";
    fail::cfg(fp, "return").unwrap();
    cluster
        .pd_client
        .switch_witnesses(region.get_id(), vec![peer_on_store3.get_id()], vec![false]);
    std::thread::sleep(Duration::from_millis(500));
    // as we ignore request snapshot, so snapshot should still not applied yet
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 1);
    must_get_none(&cluster.get_engine(3), b"k1");

    cluster.stop_node(nodes[2]);
    fail::remove(fp);
    std::thread::sleep(Duration::from_millis(100));
    // the PeerState is Unavailable, so it will request snapshot immediately after
    // start.
    cluster.run_node(nodes[2]).unwrap();
    must_get_none(&cluster.get_engine(3), b"k1");
    std::thread::sleep(Duration::from_millis(500));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 0);
}

// Test the case request snapshot and apply successfully after term change.
#[test]
fn test_request_snapshot_after_term_change() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.check_request_snapshot_interval = ReadableDuration::millis(20);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

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

    cluster.must_put(b"k1", b"v1");

    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");

    // witness -> nonwitness
    let fp1 = "ignore generate snapshot";
    fail::cfg(fp1, "return").unwrap();
    cluster
        .pd_client
        .switch_witnesses(region.get_id(), vec![peer_on_store3.get_id()], vec![false]);
    std::thread::sleep(Duration::from_millis(500));
    // as we ignore generate snapshot, so snapshot should still not applied yet
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 1);
    must_get_none(&cluster.get_engine(3), b"k1");

    let peer_on_store2 = find_peer(&region, nodes[1]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store2.clone());
    // After leader changes, the `term` and `last term` no longer match, so
    // continue to receive `MsgAppend` until the two get equal, then retry to
    // request snapshot and complete the application.
    std::thread::sleep(Duration::from_millis(500));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 0);
    fail::remove(fp1);
}

fn test_non_witness_availability(fp: &str) {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.check_peers_availability_interval = ReadableDuration::millis(20);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // non-witness -> witness
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

    cluster.must_put(b"k1", b"v1");

    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");

    fail::cfg(fp, "return").unwrap();

    // witness -> non-witness
    cluster
        .pd_client
        .switch_witnesses(region.get_id(), vec![peer_on_store3.get_id()], vec![false]);
    std::thread::sleep(Duration::from_millis(500));
    // snapshot applied
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 0);
    fail::remove(fp);
}

// Test the case leader pulls non-witness availability when non-witness failed
// to push the info.
#[test]
fn test_pull_non_witness_availability() {
    test_non_witness_availability("ignore notify leader the peer is available");
}

// Test the case non-witness pushes its availability without leader pulling.
#[test]
fn test_push_non_witness_availability() {
    test_non_witness_availability("ignore schedule check non-witness availability tick");
}

// Test the case non-witness hasn't finish applying snapshot when receives read
// request.
#[test]
fn test_non_witness_replica_read() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.check_request_snapshot_interval = ReadableDuration::millis(20);
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

    // witness -> nonwitness
    fail::cfg("ignore request snapshot", "return").unwrap();
    cluster
        .pd_client
        .switch_witnesses(region.get_id(), vec![peer_on_store3.get_id()], vec![false]);
    std::thread::sleep(Duration::from_millis(100));
    // as we ignore request snapshot, so snapshot should still not applied yet

    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(b"k0")],
        false,
    );
    request.mut_header().set_peer(peer_on_store3.clone());
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

    // start requesting snapshot and give enough time for applying snapshot to
    // complete
    fail::remove("ignore request snapshot");
    std::thread::sleep(Duration::from_millis(500));

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
    assert_eq!(resp.get_header().has_error(), false);
}

fn must_get_error_is_witness<T: Simulator<RocksEngine>>(
    cluster: &mut Cluster<RocksEngine, T>,
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

// Test the case that once a Raft election elects a voter as the leader, and
// then this voter applies the switch witness cmd, it becomes a witness and can
// correctly transfer the leader identity.
#[test]
fn test_witness_leader_transfer_out() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k0", b"v0");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);

    // prevent this peer from applying the switch witness command until it's elected
    // as the Raft leader
    fail::cfg("before_exec_batch_switch_witness", "pause").unwrap();
    let peer_on_store2 = find_peer(&region, nodes[1]).unwrap().clone();
    // nonwitness -> witness
    cluster
        .pd_client
        .switch_witnesses(region.get_id(), vec![peer_on_store2.get_id()], vec![true]);
    // make sure the left peers have applied switch witness cmd
    std::thread::sleep(Duration::from_millis(500));

    // the other follower is isolated
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    for i in 1..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes());
    }
    // the leader is down
    cluster.stop_node(1);

    // new leader would help to replicate the logs
    cluster.clear_send_filters();
    std::thread::sleep(Duration::from_millis(1000));
    // make sure the new leader has became to the witness
    fail::remove("before_exec_batch_switch_witness");
    std::thread::sleep(Duration::from_millis(500));

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

// Test the case that once a Raft election elects a voter as the leader,
// and is currently generating a snapshot for another peer, then applies the
// switch witness cmd to be a witness, the generated snapshot will be checked as
// invalidated and will not be regenerated
#[test]
fn test_witness_leader_ignore_gen_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100);
    configure_for_snapshot(&mut cluster.cfg);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k0", b"v0");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // the other follower is isolated
    cluster.add_send_filter(IsolationFilterFactory::new(3));

    // make sure raft log gc is triggered
    std::thread::sleep(Duration::from_millis(200));
    let mut before_states = HashMap::default();
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        before_states.insert(id, state.take_truncated_state());
    }

    // write some data to make log gap exceeds the gc limit
    for i in 1..1000 {
        let (k, v) = (format!("k{}", i), format!("v{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
    }

    std::thread::sleep(Duration::from_millis(200));

    // the truncated index is advanced
    for (&id, engines) in &cluster.engines {
        let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let diff = state.get_truncated_state().get_index() - before_states[&id].get_index();
        error!("EEEEE";
            "id" => &id,
            "diff" => diff,
            "state.get_truncated_state().get_index()" => state.get_truncated_state().get_index(),
            "before_states[&id].get_index()" => before_states[&id].get_index()
        );
        assert_ne!(
            900,
            state.get_truncated_state().get_index() - before_states[&id].get_index()
        );
    }

    // ingore raft log gc to avoid canceling snapshots
    fail::cfg("on_raft_gc_log_tick", "return").unwrap();
    // wait for leader applied switch to witness
    fail::cfg("before_region_gen_snap", "pause").unwrap();
    fail::cfg("ignore_snap_try_cnt", "return").unwrap();
    // After the snapshot is generated, it will be checked as invalidated and will
    // not be regenerated (handle_snapshot will not generate a snapshot for
    // witness)
    cluster.clear_send_filters();
    std::thread::sleep(Duration::from_millis(500));

    // non-witness -> witness
    fail::cfg("ignore_forbid_leader_to_be_witness", "return").unwrap();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store1.get_id()],
        vec![true],
    );
    fail::remove("before_region_gen_snap");

    std::thread::sleep(Duration::from_millis(500));

    // forbid writes
    let put = new_put_cmd(b"k3", b"v3");
    must_get_error_is_witness(&mut cluster, &region, put);
    // forbid reads
    let get = new_get_cmd(b"k1");
    must_get_error_is_witness(&mut cluster, &region, get);
    // forbid read index
    let read_index = new_read_index_cmd();
    must_get_error_is_witness(&mut cluster, &region, read_index);

    // reject to transfer, as can't send snapshot to peer_on_store3, there's a log
    // gap
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    let _ = cluster.try_transfer_leader(region.get_id(), peer_on_store3);
    std::thread::sleep(Duration::from_secs(5));
    assert_eq!(cluster.leader_of_region(1).unwrap(), peer_on_store1);

    // should be enable to transfer leader to peer_on_store2
    let peer_on_store2 = find_peer(&region, nodes[1]).unwrap().clone();
    cluster.must_transfer_leader(1, peer_on_store2);
    cluster.must_put(b"k1", b"v1");
    assert_eq!(
        cluster.leader_of_region(region.get_id()).unwrap().store_id,
        nodes[1],
    );
    assert_eq!(cluster.must_get(b"k9"), Some(b"v9".to_vec()));

    fail::remove("on_raft_gc_log_tick");
    fail::remove("ignore_snap_try_cnt");
    fail::remove("ignore_forbid_leader_to_be_witness");
}

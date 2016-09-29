// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use tikv::raftstore::store::*;
use tikv::raftstore::{Error, Result};
use kvproto::eraftpb::MessageType;
use kvproto::raft_cmdpb::RaftCmdResponse;

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::transport_simulate::*;

use rand;
use rand::Rng;
use std::time::Duration;

fn test_multi_base<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    test_multi_base_after_bootstrap(cluster);
}

fn test_multi_base_after_bootstrap<T: Simulator>(cluster: &mut Cluster<T>) {
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.must_get(key), Some(value.to_vec()));

    let check_res = cluster.check_quorum(|engine| {
        match engine.get_value(&keys::data_key(key)).unwrap() {
            None => false,
            Some(v) => &*v == value,
        }
    });
    assert!(check_res);

    cluster.must_delete(key);
    assert_eq!(cluster.must_get(key), None);

    let check_res =
        cluster.check_quorum(|engine| engine.get_value(&keys::data_key(key)).unwrap().is_none());
    assert!(check_res);

    // TODO add stale epoch test cases.
}

fn test_multi_leader_crash<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let (key1, value1) = (b"k1", b"v1");

    cluster.must_put(key1, value1);

    let last_leader = cluster.leader_of_region(1).unwrap();
    cluster.stop_node(last_leader.get_store_id());

    sleep_ms(800);
    cluster.reset_leader_of_region(1);
    let new_leader = cluster.leader_of_region(1).expect("leader should be elected.");
    assert!(new_leader != last_leader);

    assert_eq!(cluster.get(key1), Some(value1.to_vec()));

    let (key2, value2) = (b"k2", b"v2");

    cluster.must_put(key2, value2);
    cluster.must_delete(key1);
    must_get_none(&cluster.engines[&last_leader.get_store_id()], key2);
    must_get_equal(&cluster.engines[&last_leader.get_store_id()], key1, value1);

    // week up
    cluster.run_node(last_leader.get_store_id());

    must_get_equal(&cluster.engines[&last_leader.get_store_id()], key2, value2);
    must_get_none(&cluster.engines[&last_leader.get_store_id()], key1);
}


fn test_multi_cluster_restart<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let (key, value) = (b"k1", b"v1");

    assert_eq!(cluster.get(key), None);
    cluster.must_put(key, value);

    assert_eq!(cluster.get(key), Some(value.to_vec()));

    cluster.shutdown();

    // avoid TIMEWAIT
    sleep_ms(500);

    cluster.start();

    assert_eq!(cluster.get(key), Some(value.to_vec()));
}

fn test_multi_lost_majority<T: Simulator>(cluster: &mut Cluster<T>, count: usize) {
    cluster.run();

    let half = (count as u64 + 1) / 2;
    for i in 1..half + 1 {
        cluster.stop_node(i);
    }
    if let Some(leader) = cluster.leader_of_region(1) {
        if leader.get_store_id() >= half + 1 {
            cluster.stop_node(leader.get_store_id());
        }
    }
    cluster.reset_leader_of_region(1);
    sleep_ms(600);

    assert!(cluster.leader_of_region(1).is_none());

}

fn test_multi_random_restart<T: Simulator>(cluster: &mut Cluster<T>,
                                           node_count: usize,
                                           restart_count: u32) {
    cluster.run();

    let mut rng = rand::thread_rng();
    let mut value = [0u8; 5];

    for i in 1..restart_count {

        let id = 1 + rng.gen_range(0, node_count as u64);
        cluster.stop_node(id);

        let key = i.to_string().into_bytes();

        rng.fill_bytes(&mut value);
        cluster.must_put(&key, &value);
        assert_eq!(cluster.get(&key), Some(value.to_vec()));

        cluster.run_node(id);

        // verify whether data is actually being replicated and waiting for node online.
        must_get_equal(&cluster.get_engine(id), &key, &value);

        cluster.must_delete(&key);
        assert_eq!(cluster.get(&key), None);
    }
}

#[test]
fn test_multi_node_base() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_base(&mut cluster)
}

fn test_multi_drop_packet<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();
    cluster.add_send_filter(CloneFilterFactory(DropPacketFilter::new(30)));
    test_multi_base_after_bootstrap(cluster);
}

#[test]
fn test_multi_node_latency() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_latency(&mut cluster);
}

#[test]
fn test_multi_node_drop_packet() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_drop_packet(&mut cluster);
}

#[test]
fn test_multi_server_base() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_base(&mut cluster)
}


fn test_multi_latency<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();
    cluster.add_send_filter(CloneFilterFactory(DelayFilter::new(Duration::from_millis(30))));
    test_multi_base_after_bootstrap(cluster);
}

#[test]
fn test_multi_server_latency() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_latency(&mut cluster);
}

#[test]
fn test_multi_server_drop_packet() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_drop_packet(&mut cluster);
}

#[test]
fn test_multi_node_leader_crash() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_leader_crash(&mut cluster)
}

#[test]
fn test_multi_server_leader_crash() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_leader_crash(&mut cluster)
}

#[test]
fn test_multi_node_cluster_restart() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_cluster_restart(&mut cluster)
}

#[test]
fn test_multi_server_cluster_restart() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_cluster_restart(&mut cluster)
}

#[test]
fn test_multi_node_lost_majority() {
    let mut tests = vec![4, 5];
    for count in tests.drain(..) {
        let mut cluster = new_node_cluster(0, count);
        test_multi_lost_majority(&mut cluster, count)
    }
}

#[test]
fn test_multi_server_lost_majority() {
    let mut tests = vec![4, 5];
    for count in tests.drain(..) {
        let mut cluster = new_server_cluster(0, count);
        test_multi_lost_majority(&mut cluster, count)
    }
}

#[test]
fn test_multi_node_random_restart() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_random_restart(&mut cluster, count, 10);
}

#[test]
fn test_multi_server_random_restart() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_random_restart(&mut cluster, count, 10);
}

fn test_leader_change_with_uncommitted_log<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_election_timeout_ticks = 50;
    // disable compact log to make test more stable.
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    // We use three peers([1, 2, 3]) for this test.
    cluster.run();

    sleep_ms(500);

    // guarantee peer 1 is leader
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // So peer 3 won't replicate any message of the region but still can vote.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 3)
        .msg_type(MessageType::MsgAppend)));
    cluster.must_put(b"k1", b"v1");

    // peer 1 and peer 2 must have k2, but peer 3 must not.
    for i in 1..3 {
        let engine = cluster.get_engine(i);
        must_get_equal(&engine, b"k1", b"v1");
    }

    let engine3 = cluster.get_engine(3);
    must_get_none(&engine3, b"k1");

    // now only peer 1 and peer 2 can step to leader.

    // hack: first MsgAppend will append log, second MsgAppend will set commit index,
    // So only allowing first MsgAppend to make peer 2 have uncommitted entries.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 2)
        .msg_type(MessageType::MsgAppend)
        .direction(Direction::Recv)
        .allow(1)));
    // Make peer 2 have no way to know the uncommitted entries can be applied
    // when it becomes leader.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 1)
        .msg_type(MessageType::MsgHeartbeatResponse)
        .direction(Direction::Send)));
    // Make peer 2's msg won't be replicated when it becomes leader,
    // so the uncommitted entries won't be applied immediatly.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 1)
        .msg_type(MessageType::MsgAppend)
        .direction(Direction::Recv)));
    // Make peer 2 have no way to know the uncommitted entries can be applied
    // when it's still follower.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 2)
        .msg_type(MessageType::MsgHeartbeat)
        .direction(Direction::Recv)));
    debug!("putting k2");
    cluster.must_put(b"k2", b"v2");

    // peer 1 must have committed, but peer 2 has not.
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");

    cluster.must_transfer_leader(1, util::new_peer(2, 2));

    must_get_none(&cluster.get_engine(2), b"k2");

    let region = cluster.get_region(b"");
    let reqs = vec![new_put_cmd(b"k3", b"v3")];
    let mut put = new_request(region.get_id(),
                              region.get_region_epoch().clone(),
                              reqs,
                              false);
    debug!("requesting: {:?}", put);
    put.mut_header().set_peer(new_peer(2, 2));
    cluster.clear_send_filters();
    let resp = cluster.call_command(put, Duration::from_secs(5)).unwrap();
    assert!(!resp.get_header().has_error(), format!("{:?}", resp));

    for i in 1..4 {
        must_get_equal(&cluster.get_engine(i), b"k2", b"v2");
        must_get_equal(&cluster.get_engine(i), b"k3", b"v3");
    }
}

#[test]
fn test_node_read_leader_with_unapplied_log() {
    let mut cluster = new_node_cluster(0, 3);
    test_read_leader_with_unapplied_log(&mut cluster);
}

#[test]
fn test_server_read_leader_with_unapplied_log() {
    let mut cluster = new_server_cluster(0, 3);
    test_read_leader_with_unapplied_log(&mut cluster);
}

fn test_read_leader_with_unapplied_log<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_election_timeout_ticks = 50;
    // disable compact log to make test more stable.
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    // We use three peers([1, 2, 3]) for this test.
    cluster.run();

    sleep_ms(500);

    // guarantee peer 1 is leader
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // if peer 2 is unreachable, leader will not send MsgAppend to peer 2, and the leader will
    // send MsgAppend with committed information to peer 2 after network recovered, and peer 2
    // will apply the entry regardless of we add an filter, so we put k0/v0 to make sure the
    // network is reachable.
    let (k0, v0) = (b"k0", b"v0");
    cluster.must_put(k0, v0);

    for i in 1..4 {
        must_get_equal(&cluster.get_engine(i), k0, v0);
    }

    // hack: first MsgAppend will append log, second MsgAppend will set commit index,
    // So only allowing first MsgAppend to make peer 2 have uncommitted entries.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 2)
        .msg_type(MessageType::MsgAppend)
        .direction(Direction::Recv)
        .allow(1)));

    // Make peer 2's msg won't be replicated when it becomes leader,
    // so the uncommitted entries won't be applied immediatly.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 2)
        .msg_type(MessageType::MsgAppend)
        .direction(Direction::Send)));

    // Make peer 2 have no way to know the uncommitted entries can be applied
    // when it's still follower.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 2)
        .msg_type(MessageType::MsgHeartbeat)
        .direction(Direction::Recv)));

    let (k, v) = (b"k", b"v");
    cluster.must_put(k, v);

    // peer 1 must have committed, but peer 2 has not.
    must_get_equal(&cluster.get_engine(1), k, v);

    cluster.must_transfer_leader(1, util::new_peer(2, 2));

    // leader's term not equal applied index's term, if we read local, we may get old value
    // in this situation we need use raft read
    must_get_none(&cluster.get_engine(2), k);

    // internal read will use raft read no matter read_quorum is false or true, cause applied
    // index's term not equal leader's term, and will failed with timeout
    if let Err(Error::Timeout(_)) = get_with_timeout(cluster, k, false, Duration::from_secs(10)) {
        debug!("raft read failed with timeout");
    } else {
        panic!("read didn't use raft, beyond exceptions");
    }

    // recover network
    cluster.clear_send_filters();

    assert_eq!(cluster.get(k).unwrap(), v);
}

fn get_with_timeout<T: Simulator>(cluster: &mut Cluster<T>,
                                  key: &[u8],
                                  read_quorum: bool,
                                  timeout: Duration)
                                  -> Result<RaftCmdResponse> {
    let mut region = cluster.get_region(key);
    let region_id = region.get_id();
    let req = new_request(region_id,
                          region.take_region_epoch(),
                          vec![new_get_cmd(key)],
                          read_quorum);
    cluster.call_command_on_leader(req, timeout)
}

#[test]
fn test_node_leader_change_with_uncommitted_log() {
    let mut cluster = new_node_cluster(0, 3);
    test_leader_change_with_uncommitted_log(&mut cluster);
}

#[test]
fn test_server_leader_change_with_uncommitted_log() {
    let mut cluster = new_server_cluster(0, 3);
    test_leader_change_with_uncommitted_log(&mut cluster);
}

fn test_remove_leader_with_uncommitted_log<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_election_timeout_ticks = 50;
    // disable compact log to make test more stable.
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    // We use three peers([1, 2, 3]) for this test.
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    // guarantee peer 1 is leader
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // stop peer 2 replicate messages.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 2)
        .msg_type(MessageType::MsgAppend)
        .direction(Direction::Recv)));
    // peer 2 can't step to leader.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 2)
        .msg_type(MessageType::MsgRequestVote)
        .direction(Direction::Send)));

    let pd_client = cluster.pd_client.clone();
    pd_client.remove_peer(1, new_peer(1, 1));

    // wait for the leader receive the remove order.
    sleep_ms(1000);

    let region = cluster.get_region(b"");
    let reqs = vec![new_put_cmd(b"k3", b"v3")];
    let mut put = new_request(region.get_id(),
                              region.get_region_epoch().clone(),
                              reqs,
                              false);
    debug!("requesting: {:?}", put);
    put.mut_header().set_peer(new_peer(1, 1));
    cluster.clear_send_filters();
    let resp = cluster.call_command(put, Duration::from_secs(5)).unwrap();
    assert!(resp.get_header().has_error());
    assert!(resp.get_header().get_error().has_region_not_found(),
            format!("{:?} should have region not found", resp));
}

#[test]
fn test_node_remove_leader_with_uncommitted_log() {
    let mut cluster = new_node_cluster(0, 2);
    test_remove_leader_with_uncommitted_log(&mut cluster);
}

#[test]
fn test_server_remove_leader_with_uncommitted_log() {
    let mut cluster = new_server_cluster(0, 2);
    test_remove_leader_with_uncommitted_log(&mut cluster);
}

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

use std::time::Duration;
use std::sync::Arc;
use std::thread;

use tikv::raftstore::store::*;
use kvproto::eraftpb::ConfChangeType;
use kvproto::raft_cmdpb::RaftResponseHeader;
use kvproto::raft_serverpb::*;
use kvproto::metapb;
use tikv::pd::PdClient;

use futures::Future;

use super::cluster::{Cluster, Simulator};
use super::transport_simulate::*;
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util::*;
use super::pd::TestPdClient;

fn test_simple_conf_change<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();

    let r1 = cluster.run_conf_change();

    // Now region 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, b"k1");
    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // now peer 2 must have v1 and v2;
    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k2", b"v2");

    let epoch = cluster.pd_client.get_region_epoch(r1);

    // Conf version must change.
    assert!(epoch.get_conf_ver() > 1);

    // peer 5 must not exist
    let engine_5 = cluster.get_engine(5);
    must_get_none(&engine_5, b"k1");

    // add peer (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_peer(3, 3));
    // Remove peer (2, 2) from region 1.
    pd_client.must_remove_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 3 must have v1, v2 and v3
    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");
    must_get_equal(&engine_3, b"k2", b"v2");
    must_get_equal(&engine_3, b"k3", b"v3");

    // peer 2 has nothing
    must_get_none(&engine_2, b"k1");
    must_get_none(&engine_2, b"k2");

    // peer 3 must exist
    must_get_equal(&engine_3, b"k3", b"v3");

    // add peer 2 then remove it again.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    // Force update a2 to check whether peer 2 added ok and received the snapshot.
    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);

    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k2", b"v2");
    must_get_equal(&engine_2, b"k3", b"v3");

    // Make sure peer 2 is not in probe mode.
    cluster.must_put(b"k4", b"v4");
    assert_eq!(cluster.get(b"k4"), Some(b"v4".to_vec()));
    must_get_equal(&engine_2, b"k4", b"v4");

    let conf_change = new_change_peer_request(ConfChangeType::AddNode, new_peer(2, 2));
    let epoch = cluster.pd_client.get_region_epoch(r1);
    let admin_req = new_admin_request(r1, &epoch, conf_change);
    let resp = cluster.call_command_on_leader(admin_req, Duration::from_secs(3)).unwrap();
    let exec_res = resp.get_header().get_error().get_message().contains("duplicated");
    assert!(exec_res,
            "add duplicated peer should failed, but got {:?}",
            resp);

    // Remove peer (2, 2) from region 1.
    pd_client.must_remove_peer(r1, new_peer(2, 2));

    // add peer (2, 4) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 4));

    // Remove peer (3, 3) from region 1.
    pd_client.must_remove_peer(r1, new_peer(3, 3));

    let (key, value) = (b"k4", b"v4");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 4 in store 2 must have v1, v2, v3, v4, we check v1 and v4 here.
    let engine_2 = cluster.get_engine(2);

    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k4", b"v4");

    // peer 3 has nothing, we check v1 and v4 here.
    must_get_none(&engine_3, b"k1");
    must_get_none(&engine_3, b"k4");

    // TODO: add more tests.
}

fn new_conf_change_peer(store: &metapb::Store, pd_client: &Arc<TestPdClient>) -> metapb::Peer {
    let peer_id = pd_client.alloc_id().unwrap();
    new_peer(store.get_id(), peer_id)
}

fn test_pd_conf_change<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();

    cluster.start();

    let region = &pd_client.get_region(b"").unwrap();
    let region_id = region.get_id();

    let mut stores = pd_client.get_stores().unwrap();

    // Must have only one peer
    assert_eq!(region.get_peers().len(), 1);

    let peer = &region.get_peers()[0];

    let i = stores.iter().position(|store| store.get_id() == peer.get_store_id()).unwrap();
    stores.swap(0, i);

    // Now the first store has first region. others have none.

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let peer2 = new_conf_change_peer(&stores[1], &pd_client);
    let engine_2 = cluster.get_engine(peer2.get_store_id());
    assert!(engine_2.get_value(&keys::data_key(b"k1")).unwrap().is_none());
    // add new peer to first region.
    pd_client.must_add_peer(region_id, peer2.clone());

    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // now peer 2 must have v1 and v2;
    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k2", b"v2");

    // add new peer to first region.
    let peer3 = new_conf_change_peer(&stores[2], &pd_client);
    pd_client.must_add_peer(region_id, peer3.clone());

    // Remove peer2 from first region.
    pd_client.must_remove_peer(region_id, peer2.clone());

    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 3 must have v1, v2 and v3
    let engine_3 = cluster.get_engine(peer3.get_store_id());
    must_get_equal(&engine_3, b"k1", b"v1");
    must_get_equal(&engine_3, b"k2", b"v2");
    must_get_equal(&engine_3, b"k3", b"v3");

    // peer 2 has nothing
    must_get_none(&engine_2, b"k1");
    must_get_none(&engine_2, b"k2");
    // add peer4 to first region 1.
    let peer4 = new_conf_change_peer(&stores[1], &pd_client);
    pd_client.must_add_peer(region_id, peer4.clone());
    // Remove peer3 from first region.
    pd_client.must_remove_peer(region_id, peer3.clone());

    let (key, value) = (b"k4", b"v4");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer4 must have v1, v2, v3, v4, we check v1 and v4 here.
    let engine_2 = cluster.get_engine(peer4.get_store_id());

    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k4", b"v4");

    // peer 3 has nothing, we check v1 and v4 here.
    must_get_none(&engine_3, b"k1");
    must_get_none(&engine_3, b"k4");


    // TODO: add more tests.
}

#[test]
fn test_node_simple_conf_change() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_simple_conf_change(&mut cluster);
}

#[test]
fn test_server_simple_conf_change() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_simple_conf_change(&mut cluster);
}

#[test]
fn test_node_pd_conf_change() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_pd_conf_change(&mut cluster);
}

#[test]
fn test_server_pd_conf_change() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_pd_conf_change(&mut cluster);
}

fn wait_till_reach_count(pd_client: Arc<TestPdClient>, region_id: u64, c: usize) {
    let mut replica_count = 0;
    for _ in 0..1000 {
        let region = match pd_client.get_region_by_id(region_id).wait().unwrap() {
            Some(r) => r,
            None => continue,
        };
        replica_count = region.get_peers().len();
        if replica_count == c {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("replica count {} still not meet {} after 10 secs",
           replica_count,
           c);
}

fn test_auto_adjust_replica<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.start();

    let pd_client = cluster.pd_client.clone();
    let mut region = pd_client.get_region(b"").unwrap();
    let region_id = region.get_id();

    let stores = pd_client.get_stores().unwrap();

    // default replica is 5.
    wait_till_reach_count(pd_client.clone(), region_id, 5);

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    region = pd_client.get_region_by_id(region_id).wait().unwrap().unwrap();
    let i = stores.iter()
        .position(|s| region.get_peers().iter().all(|p| s.get_id() != p.get_store_id()))
        .unwrap();

    let peer = new_conf_change_peer(&stores[i], &pd_client);
    let engine = cluster.get_engine(peer.get_store_id());
    must_get_none(&engine, b"k1");

    pd_client.must_add_peer(region_id, peer.clone());

    wait_till_reach_count(pd_client.clone(), region_id, 6);

    // it should remove extra replica.
    pd_client.reset_rule();
    wait_till_reach_count(pd_client.clone(), region_id, 5);

    region = pd_client.get_region_by_id(region_id).wait().unwrap().unwrap();
    let peer = region.get_peers().get(1).unwrap().clone();
    pd_client.must_remove_peer(region_id, peer);
    wait_till_reach_count(pd_client.clone(), region_id, 4);

    // it should add missing replica.
    pd_client.reset_rule();
    wait_till_reach_count(pd_client.clone(), region_id, 5);
}

#[test]
fn test_node_auto_adjust_replica() {
    let count = 7;
    let mut cluster = new_node_cluster(0, count);
    test_auto_adjust_replica(&mut cluster);
}

#[test]
fn test_server_auto_adjust_replica() {
    let count = 7;
    let mut cluster = new_server_cluster(0, count);
    test_auto_adjust_replica(&mut cluster);
}

fn test_after_remove_itself<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();

    // disable auto compact log.
    cluster.cfg.raft_store.raft_log_gc_threshold = 10000;
    cluster.cfg.raft_store.allow_remove_leader = true;

    let r1 = cluster.run_conf_change();

    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    // 1, stop node 2
    // 2, add data to guarantee leader has more logs
    // 3, stop node 3
    // 4, remove leader itself and force compact log
    // 5, start node 2 again, so that we can commit log and apply.
    // For this scenario, peer 1 will do remove itself and then compact log
    // in the same ready result loop.
    cluster.stop_node(2);

    cluster.must_put(b"k1", b"v1");

    let engine1 = cluster.get_engine(1);
    let engine3 = cluster.get_engine(3);
    must_get_equal(&engine1, b"k1", b"v1");
    must_get_equal(&engine3, b"k1", b"v1");

    cluster.stop_node(3);

    pd_client.set_rule(box move |region, _| new_pd_remove_change_peer(region, new_peer(1, 1)));

    let epoch = cluster.pd_client
        .get_region_by_id(1)
        .wait()
        .unwrap()
        .unwrap()
        .take_region_epoch();

    let put = new_put_cmd(b"test_key", b"test_val");
    let mut req = new_request(1, epoch, vec![put], true);
    req.mut_header().set_peer(new_peer(1, 1));
    // ignore error, we just want to send this command to peer (1, 1),

    // and the command can't be executed because we have only one peer,
    // so here will return timeout error, we should ignore it.
    let _ = cluster.call_command(req, Duration::from_millis(1));

    cluster.run_node(2);
    cluster.run_node(3);

    for _ in 0..250 {
        let region: RegionLocalState =
            engine1.get_msg(&keys::region_state_key(r1)).unwrap().unwrap();
        if region.get_state() == PeerState::Tombstone {
            return;
        }
        sleep_ms(20);
    }
    let region: RegionLocalState = engine1.get_msg(&keys::region_state_key(r1)).unwrap().unwrap();
    assert_eq!(region.get_state(), PeerState::Tombstone);

    // TODO: add split after removing itself test later.
}

#[test]
fn test_node_after_remove_itself() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_after_remove_itself(&mut cluster);
}

#[test]
fn test_server_after_remove_itself() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_after_remove_itself(&mut cluster);
}

fn test_split_brain<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();
    cluster.cfg.raft_store.allow_remove_leader = true;

    let r1 = cluster.run_conf_change();

    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    // leader isolation
    cluster.must_transfer_leader(r1, new_peer(2, 2));
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    // refresh region info, maybe no need
    cluster.must_put(b"k1", b"v1");

    // add [4,5,6] and remove [2,3]
    pd_client.must_add_peer(r1, new_peer(4, 4));
    pd_client.must_add_peer(r1, new_peer(5, 5));
    pd_client.must_add_peer(r1, new_peer(6, 6));
    pd_client.must_remove_peer(r1, new_peer(2, 2));
    pd_client.must_remove_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(6), b"k2", b"v2");
    let region_detail = cluster.region_detail(r1, 1);
    let region_peers = region_detail.get_region().get_peers();
    assert_eq!(region_peers.len(), 3);
    for peer in region_peers {
        assert!(peer.get_id() < 4);
    }
    assert!(region_detail.get_leader().get_id() < 4);

    // when network recovers, 1 will send request vote to [2,3]
    cluster.clear_send_filters();
    cluster.partition(vec![1, 2, 3], vec![4, 5, 6]);

    // refresh region info, maybe no need
    cluster.must_put(b"k3", b"v3");

    // check whether a new cluster [1,2,3] is formed
    // if so, both [1,2,3] and [4,5,6] think they serve for region r1
    // result in split brain
    let header0 = find_leader_response_header(cluster, r1, new_peer(2, 2));
    assert!(header0.get_error().has_region_not_found());

    // at least wait for a round of election timeout and check again
    let term = find_leader_response_header(cluster, r1, new_peer(1, 1)).get_current_term();
    let mut current_term = term;
    while current_term < term + 2 {
        sleep_ms(10);
        let header2 = find_leader_response_header(cluster, r1, new_peer(1, 1));
        current_term = header2.get_current_term();
    }

    let header1 = find_leader_response_header(cluster, r1, new_peer(2, 2));
    assert!(header1.get_error().has_region_not_found());
}

fn find_leader_response_header<T: Simulator>(cluster: &mut Cluster<T>,
                                             region_id: u64,
                                             peer: metapb::Peer)
                                             -> RaftResponseHeader {
    let find_leader = new_status_request(region_id, peer, new_region_leader_cmd());
    let resp = cluster.call_command(find_leader, Duration::from_secs(5));
    resp.unwrap().take_header()
}

#[test]
fn test_server_split_brain() {
    let count = 6;
    let mut cluster = new_server_cluster(0, count);
    test_split_brain(&mut cluster);
}

#[test]
fn test_node_split_brain() {
    let count = 6;
    let mut cluster = new_node_cluster(0, count);
    test_split_brain(&mut cluster);
}

/// A helper function for testing the conf change is safe.
fn test_conf_change_safe<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();
    cluster.cfg.raft_store.allow_remove_leader = true;

    let region_id = cluster.run_conf_change();

    // Test adding nodes.

    // Ensure it works to add one node to a cluster that has only one node.
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));

    // Isolate the leader.
    cluster.must_transfer_leader(region_id, new_peer(1, 1));
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    // Ensure new leader is elected and it works.
    cluster.must_put(b"k1", b"v1");

    // Ensure the conf change is safe:
    // The "AddNode" request will be rejected
    // if there are only 2 healthy nodes in a cluster of 3 nodes.
    pd_client.add_peer(region_id, new_peer(4, 4));
    // Put a new kv to ensure the previous "AddNode" is handled.
    cluster.must_put(b"k2", b"v2");
    pd_client.must_none_peer(region_id, new_peer(4, 4));

    // Recover the isolated peer.
    cluster.clear_send_filters();

    // Then new node could be added.
    pd_client.must_add_peer(region_id, new_peer(4, 4));

    // Test removing nodes.

    // Ensure nodes could be removed.
    pd_client.must_remove_peer(region_id, new_peer(4, 4));

    // Isolate the leader.
    cluster.must_transfer_leader(region_id, new_peer(1, 1));
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    // Ensure new leader is elected and it works.
    cluster.must_put(b"k3", b"v3");

    // Ensure the conf change is safe:
    // The "RemoveNode" request which asks to remove one healthy node will be rejected
    // if there are only 2 healthy nodes in a cluster of 3 nodes.
    pd_client.remove_peer(region_id, new_peer(2, 2));
    cluster.must_put(b"k4", b"v4");
    pd_client.must_have_peer(region_id, new_peer(2, 2));

    // In this case, it's fine to remove one unhealthy node.
    pd_client.must_remove_peer(region_id, new_peer(1, 1));

    // Ensure it works to remove one node from the cluster that has only two healthy nodes.
    pd_client.must_remove_peer(region_id, new_peer(2, 2));
}

#[test]
fn test_node_conf_change_safe() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_conf_change_safe(&mut cluster);
}

#[test]
fn test_server_safe_conf_change() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_conf_change_safe(&mut cluster);
}

#[test]
fn test_conf_change_remove_leader() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_rule();
    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    // Transfer leader to the first peer.
    cluster.must_transfer_leader(r1, new_peer(1, 1));

    // Try to remove leader, which should be ignored.
    let epoch = cluster.get_region_epoch(r1);
    let req = new_admin_request(r1,
                                &epoch,
                                new_change_peer_request(ConfChangeType::RemoveNode,
                                                        new_peer(1, 1)));
    let res = cluster.call_command_on_leader(req, Duration::from_secs(5)).unwrap();
    assert_eq!(res.get_header().get_error().get_message(),
               "ignore remove leader");
}

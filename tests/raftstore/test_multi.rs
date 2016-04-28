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

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::transport_simulate::Strategy;

use rand;
use rand::Rng;
use std::time::Duration;

fn test_multi_base<T: Simulator>(cluster: &mut Cluster<T>) {
    test_multi_with_transport_strategy(cluster, vec![]);
}

fn test_multi_with_transport_strategy<T: Simulator>(cluster: &mut Cluster<T>,
                                                    strategy: Vec<Strategy>) {
    // init_log();

    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    cluster.bootstrap_region().expect("");
    cluster.start_with_strategy(strategy);

    let (key, value) = (b"a1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let check_res = cluster.check_quorum(|engine| {
        match engine.get_value(&keys::data_key(key)).unwrap() {
            None => false,
            Some(v) => &*v == value,
        }
    });
    assert!(check_res);

    cluster.must_delete(key);
    assert_eq!(cluster.get(key), None);

    let check_res = cluster.check_quorum(|engine| {
        engine.get_value(&keys::data_key(key)).unwrap().is_none()
    });
    assert!(check_res);

    // TODO add stale epoch test cases.
}

fn test_multi_leader_crash<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    let (key1, value1) = (b"a1", b"v1");

    cluster.must_put(key1, value1);

    let last_leader = cluster.leader_of_region(1).unwrap();
    cluster.stop_node(last_leader.get_store_id());

    sleep_ms(800);
    cluster.reset_leader_of_region(1);
    let new_leader = cluster.leader_of_region(1).expect("leader should be elected.");
    assert!(new_leader != last_leader);

    assert_eq!(cluster.get(key1), Some(value1.to_vec()));

    let (key2, value2) = (b"a2", b"v2");

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
    cluster.bootstrap_region().expect("");
    cluster.start();

    let (key, value) = (b"a1", b"v1");

    assert_eq!(cluster.get(key), None);
    cluster.must_put(key, value);

    assert_eq!(cluster.get(key), Some(value.to_vec()));

    cluster.shutdown();
    cluster.start();

    assert_eq!(cluster.get(key), Some(value.to_vec()));
}

fn test_multi_lost_majority<T: Simulator>(cluster: &mut Cluster<T>, count: usize) {
    cluster.bootstrap_region().expect("");
    cluster.start();

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
    cluster.bootstrap_region().expect("");
    cluster.start();

    let mut rng = rand::thread_rng();
    let key = b"a1";
    let mut value = [0u8; 5];

    assert_eq!(cluster.get(key), None);

    for _ in 1..restart_count {
        rng.fill_bytes(&mut value);
        cluster.must_put(key, &value);
        assert_eq!(cluster.get(key), Some(value.to_vec()));

        let id = 1 + rng.gen_range(0, node_count as u64);
        cluster.stop_node(id);
        cluster.run_node(id);
        wait_until_node_online(cluster, id);

        // verify whether data is actually being replicated
        must_get_equal(&cluster.get_engine(id), key, &value);

        cluster.must_delete(key);
        assert_eq!(cluster.get(key), None);
    }
}

pub fn wait_until_node_online<T: Simulator>(cluster: &mut Cluster<T>, node_id: u64) {
    for _ in 0..200 {
        // leverage the fact that store id is equal to node id actually
        let peer = new_peer(node_id, 0);
        let find_leader = new_status_request(1, peer, new_region_leader_cmd());
        let resp = cluster.call_command(find_leader, Duration::from_secs(3));
        if resp.is_err() {
            sleep_ms(10);
            continue;
        }
        if !resp.unwrap().get_status_response().get_region_leader().has_leader() {
            sleep_ms(10);
            continue;
        }
        return;
    }
    assert!(false);
}

#[test]
fn test_multi_node_base() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_base(&mut cluster)
}

#[test]
fn test_multi_node_latency() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_with_transport_strategy(&mut cluster, vec![Strategy::Delay(10)]);
}

#[test]
fn test_multi_node_drop_packet() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_with_transport_strategy(&mut cluster, vec![Strategy::DropPacket(30)]);
}

#[test]
fn test_multi_server_base() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_base(&mut cluster)
}

#[test]
fn test_multi_server_latency() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_with_transport_strategy(&mut cluster, vec![Strategy::Delay(10)]);
}

#[test]
fn test_multi_server_drop_packet() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_with_transport_strategy(&mut cluster, vec![Strategy::DropPacket(40)]);
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

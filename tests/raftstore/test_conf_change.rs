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
use std::sync::{Arc, RwLock};
use std::thread;

use tikv::raftstore::store::*;
use kvproto::raftpb::ConfChangeType;
use kvproto::metapb;
use tikv::pd::PdClient;
use tikv::util::HandyRwLock;

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util::*;
use super::pd::TestPdClient;


fn test_simple_conf_change<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_log();
    let r1 = cluster.bootstrap_conf_change();
    cluster.start();

    // Now region 1 only has peer (1, 1, 1);
    let (key, value) = (b"a1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, b"a1");
    // add peer (2,2,2) to region 1.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(2, 2));

    let (key, value) = (b"a2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // now peer 2 must have v1 and v2;
    must_get_equal(&engine_2, b"a1", b"v1");
    must_get_equal(&engine_2, b"a2", b"v2");

    let epoch = cluster.pd_client
                       .rl()
                       .get_region_by_id(cluster.id(), 1)
                       .unwrap()
                       .get_region_epoch()
                       .clone();

    // Conf version must change.
    assert!(epoch.get_conf_ver() > 1);

    let change_peer = new_admin_request(1,
                                        &epoch,
                                        new_change_peer_cmd(ConfChangeType::AddNode,
                                                            new_peer(2, 2)));
    let resp = cluster.call_command_on_leader(change_peer, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error(),
            "we can't add same peer twice");

    // Send an invalid stale epoch
    let mut stale_epoch = metapb::RegionEpoch::new();
    stale_epoch.set_version(1);
    stale_epoch.set_conf_ver(1);
    let change_peer = new_admin_request(1,
                                        &stale_epoch,
                                        new_change_peer_cmd(ConfChangeType::AddNode,
                                                            new_peer(5, 5)));
    let resp = cluster.call_command_on_leader(change_peer, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error(),
            "We can't change peer with stale epoch");

    // peer 5 must not exist
    let engine_5 = cluster.get_engine(5);
    must_get_none(&engine_5, b"a1");

    // add peer (3, 3, 3) to region 1.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(3, 3));
    // Remove peer (2, 2, 2) from region 1.
    cluster.change_peer(r1, ConfChangeType::RemoveNode, new_peer(2, 2));

    let (key, value) = (b"a3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 3 must have v1, v2 and v3
    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"a1", b"v1");
    must_get_equal(&engine_3, b"a2", b"v2");
    must_get_equal(&engine_3, b"a3", b"v3");

    // peer 2 has nothing
    must_get_none(&engine_2, b"a1");
    must_get_none(&engine_2, b"a2");

    let epoch = cluster.pd_client
                       .rl()
                       .get_region_by_id(cluster.id(), 1)
                       .unwrap()
                       .get_region_epoch()
                       .clone();
    let change_peer = new_admin_request(1,
                                        &epoch,
                                        new_change_peer_cmd(ConfChangeType::RemoveNode,
                                                            new_peer(2, 2)));
    let resp = cluster.call_command_on_leader(change_peer, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error(),
            "we can't remove same peer twice");

    let change_peer = new_admin_request(1,
                                        &stale_epoch,
                                        new_change_peer_cmd(ConfChangeType::RemoveNode,
                                                            new_peer(3, 3)));
    let resp = cluster.call_command_on_leader(change_peer, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error(),
            "We can't change peer with stale epoch");

    // peer 3 must exist
    must_get_equal(&engine_3, b"a3", b"v3");

    // add peer 2 then remove it again.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(2, 2));

    // Force update a2 to check whether peer 2 added ok and received the snapshot.
    let (key, value) = (b"a2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);

    must_get_equal(&engine_2, b"a1", b"v1");
    must_get_equal(&engine_2, b"a2", b"v2");
    must_get_equal(&engine_2, b"a3", b"v3");

    // Remove peer (2, 2, 2) from region 1.
    cluster.change_peer(r1, ConfChangeType::RemoveNode, new_peer(2, 2));

    // add peer (2, 2, 4) to region 1.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(2, 4));
    // Remove peer (3, 3, 3) from region 1.
    cluster.change_peer(r1, ConfChangeType::RemoveNode, new_peer(3, 3));

    let (key, value) = (b"a4", b"v4");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 4 in store 2 must have v1, v2, v3, v4, we check v1 and v4 here.
    let engine_2 = cluster.get_engine(2);

    must_get_equal(&engine_2, b"a1", b"v1");
    must_get_equal(&engine_2, b"a4", b"v4");

    // peer 3 has nothing, we check v1 and v4 here.
    must_get_none(&engine_3, b"a1");
    must_get_none(&engine_3, b"a4");

    // TODO: add more tests.
}

fn new_conf_change_peer(store: &metapb::Store,
                        pd_client: &Arc<RwLock<TestPdClient>>)
                        -> metapb::Peer {
    let peer_id = pd_client.wl().alloc_id(0).unwrap();
    new_peer(store.get_id(), peer_id)
}

fn test_pd_conf_change<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_log();
    cluster.start();

    let cluster_id = cluster.id();
    let pd_client = cluster.pd_client.clone();
    let region = &pd_client.rl().get_region(cluster_id, b"").unwrap();
    let region_id = region.get_id();

    let mut stores = pd_client.rl().get_stores(cluster_id).unwrap();

    // Must have only one peer
    assert_eq!(region.get_peers().len(), 1);

    let peer = &region.get_peers()[0];

    let i = stores.iter().position(|store| store.get_id() == peer.get_store_id()).unwrap();
    stores.swap(0, i);

    // Now the first store has first region. others have none.

    let (key, value) = (b"a1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let peer2 = new_conf_change_peer(&stores[1], &pd_client);
    let engine_2 = cluster.get_engine(peer2.get_store_id());
    assert!(engine_2.get_value(&keys::data_key(b"a1")).unwrap().is_none());
    // add new peer to first region.
    cluster.change_peer(region_id, ConfChangeType::AddNode, peer2.clone());

    let (key, value) = (b"a2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // now peer 2 must have v1 and v2;
    must_get_equal(&engine_2, b"a1", b"v1");
    must_get_equal(&engine_2, b"a2", b"v2");

    // add new peer to first region.
    let peer3 = new_conf_change_peer(&stores[2], &pd_client);
    cluster.change_peer(region_id, ConfChangeType::AddNode, peer3.clone());
    // Remove peer2 from first region.
    cluster.change_peer(region_id, ConfChangeType::RemoveNode, peer2.clone());

    let (key, value) = (b"a3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 3 must have v1, v2 and v3
    let engine_3 = cluster.get_engine(peer3.get_store_id());
    must_get_equal(&engine_3, b"a1", b"v1");
    must_get_equal(&engine_3, b"a2", b"v2");
    must_get_equal(&engine_3, b"a3", b"v3");

    // peer 2 has nothing
    must_get_none(&engine_2, b"a1");
    must_get_none(&engine_2, b"a2");
    // add peer4 to first region 1.
    let peer4 = new_conf_change_peer(&stores[1], &pd_client);
    cluster.change_peer(region_id, ConfChangeType::AddNode, peer4.clone());
    // Remove peer3 from first region.
    cluster.change_peer(region_id, ConfChangeType::RemoveNode, peer3.clone());

    let (key, value) = (b"a4", b"v4");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer4 must have v1, v2, v3, v4, we check v1 and v4 here.
    let engine_2 = cluster.get_engine(peer4.get_store_id());

    must_get_equal(&engine_2, b"a1", b"v1");
    must_get_equal(&engine_2, b"a4", b"v4");

    // peer 3 has nothing, we check v1 and v4 here.
    must_get_none(&engine_3, b"a1");
    must_get_none(&engine_3, b"a4");


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

fn wait_till_reach_count(pd_client: Arc<RwLock<TestPdClient>>,
                         cluster_id: u64,
                         region_id: u64,
                         c: usize) {
    let mut replica_count = 0;
    for _ in 0..1000 {
        let region = pd_client.rl().get_region_by_id(cluster_id, region_id).unwrap();
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
    cluster.cfg.store_cfg.replica_check_tick_interval = 200;
    cluster.start();

    let cluster_id = cluster.id();
    let pd_client = cluster.pd_client.clone();
    let mut region = pd_client.rl().get_region(cluster_id, b"").unwrap();
    let region_id = region.get_id();

    let stores = pd_client.rl().get_stores(cluster_id).unwrap();

    // default replica is 5.
    wait_till_reach_count(pd_client.clone(), cluster_id, region_id, 5);

    let (key, value) = (b"a1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    region = pd_client.rl().get_region_by_id(cluster_id, region_id).unwrap();
    let i = stores.iter()
                  .position(|s| region.get_peers().iter().all(|p| s.get_id() != p.get_store_id()))
                  .unwrap();

    let peer = new_conf_change_peer(&stores[i], &pd_client);
    let engine = cluster.get_engine(peer.get_store_id());
    must_get_none(&engine, b"a1");

    cluster.change_peer(region_id, ConfChangeType::AddNode, peer.clone());

    wait_till_reach_count(pd_client.clone(), cluster_id, region_id, 6);
    // it should remove extra replica.
    wait_till_reach_count(pd_client.clone(), cluster_id, region_id, 5);

    region = pd_client.rl().get_region_by_id(cluster_id, region_id).unwrap();
    let peer = region.get_peers().get(1).unwrap().clone();

    cluster.change_peer(region_id, ConfChangeType::RemoveNode, peer);
    wait_till_reach_count(pd_client.clone(), cluster_id, region_id, 4);
    // it should add missing replica.
    wait_till_reach_count(pd_client.clone(), cluster_id, region_id, 5);
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
    // disable auto compact log.
    cluster.cfg.store_cfg.raft_log_gc_threshold = 10000;

    let r1 = cluster.bootstrap_conf_change();
    cluster.start();

    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(2, 2));
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(3, 3));

    // 1, stop node 2
    // 2, add data to guarantee leader has more logs
    // 3, stop node 3
    // 4, remove leader itself and force compact log
    // 5, start node 2 again, so that we can commit log and apply.
    // For this scenario, peer 1 will do remove itself and then compact log
    // in the same ready result loop.
    cluster.stop_node(2);

    cluster.must_put(b"a1", b"v1");

    let engine1 = cluster.get_engine(1);
    let engine3 = cluster.get_engine(3);
    must_get_equal(&engine1, b"a1", b"v1");
    must_get_equal(&engine3, b"a1", b"v1");

    cluster.stop_node(3);

    let epoch = cluster.pd_client
                       .rl()
                       .get_region_by_id(cluster.id(), 1)
                       .unwrap()
                       .get_region_epoch()
                       .clone();
    let mut change_peer = new_admin_request(r1,
                                            &epoch,
                                            new_change_peer_cmd(ConfChangeType::RemoveNode,
                                                                new_peer(1, 1)));
    change_peer.mut_header().set_peer(new_peer(1, 1));
    // ignore error, we just want to send this command to peer (1, 1),
    // and know that it can't be executed because we have only one peer,
    // so here will return timeout error, we should ignore it.
    let _ = cluster.call_command(change_peer, Duration::from_millis(1));

    let engine1 = cluster.get_engine(1);
    let index = engine1.get_u64(&keys::raft_applied_index_key(r1)).unwrap().unwrap();
    let mut compact_log = new_admin_request(r1, &epoch, new_compact_log_cmd(index));
    compact_log.mut_header().set_peer(new_peer(1, 1));
    // ignore error, see above comment.
    let _ = cluster.call_command(compact_log, Duration::from_millis(1));

    cluster.run_node(2);
    cluster.run_node(3);

    sleep_ms(2000);

    let detail = cluster.region_detail(r1, 2);

    cluster.pd_client.wl().change_peer(cluster.id(), detail.get_region().clone()).unwrap();

    cluster.reset_leader_of_region(r1);
    cluster.must_put(b"a3", b"v3");

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

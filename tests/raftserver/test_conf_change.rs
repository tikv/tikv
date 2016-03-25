use std::time::Duration;
use std::sync::{Arc, RwLock};
use std::thread;

use tikv::raftserver::store::*;
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
    // init_env_log();
    let r1 = cluster.bootstrap_conf_change();

    cluster.start();

    // Now region 1 only has peer (1, 1, 1);
    let (key, value) = (b"a1", b"v1");

    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, b"a1");
    // add peer (2,2,2) to region 1.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(2, 2, 2));

    let (key, value) = (b"a2", b"v2");
    cluster.put(key, value);
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
    let change_peer = new_admin_request(1,
                                        new_change_peer_cmd(ConfChangeType::AddNode,
                                                            new_peer(2, 2, 2),
                                                            &epoch));
    let resp = cluster.call_command_on_leader(1, change_peer, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error(),
            "we can't add same peer twice");

    // Send an invalid stale epoch
    let mut stale_epoch = metapb::RegionEpoch::new();
    stale_epoch.set_version(1);
    stale_epoch.set_conf_ver(1);
    let change_peer = new_admin_request(1,
                                        new_change_peer_cmd(ConfChangeType::AddNode,
                                                            new_peer(5, 5, 5),
                                                            &stale_epoch));
    let resp = cluster.call_command_on_leader(1, change_peer, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error(),
            "We can't change peer with stale epoch");

    // peer 5 must not exist
    let engine_5 = cluster.get_engine(5);
    must_get_none(&engine_5, b"a1");

    // add peer (3, 3, 3) to region 1.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(3, 3, 3));
    // Remove peer (2, 2, 2) from region 1.
    cluster.change_peer(r1, ConfChangeType::RemoveNode, new_peer(2, 2, 2));

    let (key, value) = (b"a3", b"v3");
    cluster.put(key, value);
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
                                        new_change_peer_cmd(ConfChangeType::RemoveNode,
                                                            new_peer(2, 2, 2),
                                                            &epoch));
    let resp = cluster.call_command_on_leader(1, change_peer, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error(),
            "we can't remove same peer twice");

    let change_peer = new_admin_request(1,
                                        new_change_peer_cmd(ConfChangeType::RemoveNode,
                                                            new_peer(3, 3, 3),
                                                            &stale_epoch));
    let resp = cluster.call_command_on_leader(1, change_peer, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error(),
            "We can't change peer with stale epoch");

    // peer 3 must exist
    must_get_equal(&engine_3, b"a3", b"v3");

    // add peer 2 then remove it again.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(2, 2, 2));

    // Force update a2 to check whether peer 2 added ok and received the snapshot.
    let (key, value) = (b"a2", b"v2");
    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);

    must_get_equal(&engine_2, b"a1", b"v1");
    must_get_equal(&engine_2, b"a2", b"v2");
    must_get_equal(&engine_2, b"a3", b"v3");

    // Remove peer (2, 2, 2) from region 1.
    cluster.change_peer(r1, ConfChangeType::RemoveNode, new_peer(2, 2, 2));

    // add peer (2, 2, 4) to region 1.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(2, 2, 4));
    // Remove peer (3, 3, 3) from region 1.
    cluster.change_peer(r1, ConfChangeType::RemoveNode, new_peer(3, 3, 3));

    let (key, value) = (b"a4", b"v4");
    cluster.put(key, value);
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
    let peer_id = pd_client.wl().alloc_id().unwrap();
    new_peer(store.get_node_id(), store.get_store_id(), peer_id)
}

fn test_pd_conf_change<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_env_log();
    cluster.start();

    let cluster_id = cluster.id();
    let pd_client = cluster.pd_client.clone();
    let region = &pd_client.rl().get_region(cluster_id, b"").unwrap();
    let region_id = region.get_region_id();

    let mut stores = pd_client.rl().get_stores(cluster_id).unwrap();

    // Must have only one peer
    assert_eq!(region.get_peers().len(), 1);

    let peer = &region.get_peers()[0];

    let i = stores.iter().position(|store| store.get_store_id() == peer.get_store_id()).unwrap();
    stores.swap(0, i);

    // Now the first store has first region. others have none.

    let (key, value) = (b"a1", b"v1");
    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let peer2 = new_conf_change_peer(&stores[1], &pd_client);
    let engine_2 = cluster.get_engine(peer2.get_node_id());
    assert!(engine_2.get_value(&keys::data_key(b"a1")).unwrap().is_none());
    // add new peer to first region.
    cluster.change_peer(region_id, ConfChangeType::AddNode, peer2.clone());

    let (key, value) = (b"a2", b"v2");
    cluster.put(key, value);
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
    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 3 must have v1, v2 and v3
    let engine_3 = cluster.get_engine(peer3.get_node_id());
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
    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer4 must have v1, v2, v3, v4, we check v1 and v4 here.
    let engine_2 = cluster.get_engine(peer4.get_node_id());

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

fn test_auto_adjust_replica<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.store_cfg.replica_check_tick_interval = 200;
    cluster.start();

    let cluster_id = cluster.id();
    let pd_client = cluster.pd_client.clone();
    let region = pd_client.rl().get_region(cluster_id, b"").unwrap();
    let region_id = region.get_region_id();

    let stores = pd_client.rl().get_stores(cluster_id).unwrap();

    // Must have only one peer
    assert_eq!(region.get_peers().len(), 1);

    thread::sleep(Duration::from_secs(1));

    let region = pd_client.rl().get_region_by_id(cluster_id, region_id).unwrap();
    // default replica is 5.
    assert_eq!(region.get_peers().len(), 5);

    let (key, value) = (b"a1", b"v1");
    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let i = stores.iter()
                  .position(|s| {
                      region.get_peers().iter().all(|p| s.get_store_id() != p.get_store_id())
                  })
                  .unwrap();

    let peer = new_conf_change_peer(&stores[i], &pd_client);
    let engine = cluster.get_engine(peer.get_node_id());
    must_get_none(&engine, b"a1");

    cluster.change_peer(region_id, ConfChangeType::AddNode, peer.clone());

    let region = pd_client.rl().get_region_by_id(cluster_id, region_id).unwrap();
    assert_eq!(region.get_peers().len(), 6);

    thread::sleep(Duration::from_millis(300));

    let region = pd_client.rl().get_region_by_id(cluster_id, region_id).unwrap();
    assert_eq!(region.get_peers().len(), 5);

    let peer = region.get_peers().get(1).unwrap().clone();

    cluster.change_peer(region_id, ConfChangeType::RemoveNode, peer);
    let region = pd_client.rl().get_region_by_id(cluster_id, region_id).unwrap();
    assert_eq!(region.get_peers().len(), 4);

    thread::sleep(Duration::from_millis(300));

    let region = pd_client.rl().get_region_by_id(cluster_id, region_id).unwrap();
    assert_eq!(region.get_peers().len(), 5);
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

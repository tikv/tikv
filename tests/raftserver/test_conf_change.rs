use std::sync::{Arc, RwLock};

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
    assert!(engine_2.get_value(&keys::data_key(b"a1")).unwrap().is_none());
    // add peer (2,2,2) to region 1.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(2, 2, 2));

    let (key, value) = (b"a2", b"v2");
    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // now peer 2 must have v1 and v2;
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a1")).unwrap().unwrap(),
               b"v1");
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a2")).unwrap().unwrap(),
               b"v2");

    // add peer (3, 3, 3) to region 1.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(3, 3, 3));
    // Remove peer (2, 2, 2) from region 1.
    cluster.change_peer(r1, ConfChangeType::RemoveNode, new_peer(2, 2, 2));

    let (key, value) = (b"a3", b"v3");
    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 3 must have v1, v2 and v3
    let engine_3 = cluster.get_engine(3);
    assert_eq!(&*engine_3.get_value(&keys::data_key(b"a1")).unwrap().unwrap(),
               b"v1");
    assert_eq!(&*engine_3.get_value(&keys::data_key(b"a2")).unwrap().unwrap(),
               b"v2");
    assert_eq!(&*engine_3.get_value(&keys::data_key(b"a3")).unwrap().unwrap(),
               b"v3");

    // peer 2 has nothing
    assert!(engine_2.get_value(&keys::data_key(b"a1")).unwrap().is_none());
    assert!(engine_2.get_value(&keys::data_key(b"a2")).unwrap().is_none());

    // add peer 2 then remove it again.
    cluster.change_peer(r1, ConfChangeType::AddNode, new_peer(2, 2, 2));

    // Force update a2 to check whether peer 2 added ok and received the snapshot.
    let (key, value) = (b"a2", b"v2");
    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a1")).unwrap().unwrap(),
               b"v1");
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a2")).unwrap().unwrap(),
               b"v2");
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a3")).unwrap().unwrap(),
               b"v3");

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

    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a1")).unwrap().unwrap(),
               b"v1");
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a4")).unwrap().unwrap(),
               b"v4");

    // peer 3 has nothing, we check v1 and v4 here.
    assert!(engine_3.get_value(&keys::data_key(b"a1")).unwrap().is_none());
    assert!(engine_3.get_value(&keys::data_key(b"a4")).unwrap().is_none());

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

    let cluster_id = cluster.id;
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
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a1")).unwrap().unwrap(),
               b"v1");
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a2")).unwrap().unwrap(),
               b"v2");

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
    assert_eq!(&*engine_3.get_value(&keys::data_key(b"a1")).unwrap().unwrap(),
               b"v1");
    assert_eq!(&*engine_3.get_value(&keys::data_key(b"a2")).unwrap().unwrap(),
               b"v2");
    assert_eq!(&*engine_3.get_value(&keys::data_key(b"a3")).unwrap().unwrap(),
               b"v3");

    // peer 2 has nothing
    assert!(engine_2.get_value(&keys::data_key(b"a1")).unwrap().is_none());
    assert!(engine_2.get_value(&keys::data_key(b"a2")).unwrap().is_none());
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

    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a1")).unwrap().unwrap(),
               b"v1");
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a4")).unwrap().unwrap(),
               b"v4");

    // peer 3 has nothing, we check v1 and v4 here.
    assert!(engine_3.get_value(&keys::data_key(b"a1")).unwrap().is_none());
    assert!(engine_3.get_value(&keys::data_key(b"a4")).unwrap().is_none());


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

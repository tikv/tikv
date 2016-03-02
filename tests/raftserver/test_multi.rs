use tikv::raftserver::store::*;

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;

fn test_multi_base<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_env_log();

    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    cluster.bootstrap_region().expect("");
    cluster.run_all_nodes();

    let (key, value) = (b"a1", b"v1");

    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let check_res = cluster.check_quorum(|engine| {
        match engine.get_value(&keys::data_key(key)).unwrap() {
            None => false,
            Some(v) => &*v == value,
        }
    });
    assert!(check_res);

    cluster.delete(key);
    assert_eq!(cluster.get(key), None);

    let check_res = cluster.check_quorum(|engine| {
        engine.get_value(&keys::data_key(key)).unwrap().is_none()
    });
    assert!(check_res);
}


fn test_multi_leader_crash<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.run_all_nodes();

    let (key1, value1) = (b"a1", b"v1");

    cluster.put(key1, value1);

    let last_leader = cluster.leader_of_region(1).unwrap();
    cluster.stop_node(last_leader.get_node_id());

    sleep_ms(800);
    cluster.reset_leader_of_region(1);
    let new_leader = cluster.leader_of_region(1).expect("leader should be elected.");
    assert!(new_leader != last_leader);

    assert_eq!(cluster.get(key1), Some(value1.to_vec()));

    let (key2, value2) = (b"a2", b"v2");

    cluster.put(key2, value2);
    cluster.delete(key1);
    assert!(cluster.engines[&last_leader.get_node_id()]
                .get_value(&keys::data_key(key2))
                .unwrap()
                .is_none());
    assert!(cluster.engines[&last_leader.get_node_id()]
                .get_value(&keys::data_key(key1))
                .unwrap()
                .is_some());

    // week up
    cluster.run_node(last_leader.get_node_id());

    sleep_ms(400);
    let v = cluster.engines[&last_leader.get_node_id()]
                .get_value(&keys::data_key(key2))
                .unwrap()
                .unwrap();
    assert_eq!(&*v, value2);
    assert!(cluster.engines[&last_leader.get_node_id()]
                .get_value(&keys::data_key(key1))
                .unwrap()
                .is_none());
}


fn test_multi_cluster_restart<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.run_all_nodes();

    let (key, value) = (b"a1", b"v1");

    assert!(cluster.leader_of_region(1).is_some());
    assert_eq!(cluster.get(key), None);
    cluster.put(key, value);

    assert_eq!(cluster.get(key), Some(value.to_vec()));

    cluster.shutdown();
    cluster.run_all_nodes();

    assert!(cluster.leader_of_region(1).is_some());
    assert_eq!(cluster.get(key), Some(value.to_vec()));
}

fn test_multi_lost_majority<T: Simulator>(cluster: &mut Cluster<T>, count: usize) {
    cluster.bootstrap_region().expect("");
    cluster.run_all_nodes();

    let half = (count as u64 + 1) / 2;
    for i in 1..half + 1 {
        cluster.stop_node(i);
    }
    if let Some(leader) = cluster.leader_of_region(1) {
        if leader.get_node_id() >= half + 1 {
            cluster.stop_node(leader.get_node_id());
        }
    }
    cluster.reset_leader_of_region(1);
    sleep_ms(600);

    assert!(cluster.leader_of_region(1).is_none());

}

#[test]
fn test_multi_node_base() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_multi_base(&mut cluster)
}

#[test]
fn test_multi_server_base() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_multi_base(&mut cluster)
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

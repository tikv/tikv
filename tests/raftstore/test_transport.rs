use std::sync::Arc;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util::must_get_equal;
use std::collections::HashSet;

fn split_leader_with_majority(leader: u64, count: u64) -> (HashSet<u64>, HashSet<u64>) {
    if leader <= (count+1)/2 {
        ((1..count/2+2).collect(), (count/2+2..count+1).collect())
    } else {
        (((count+1)/2..count+1).collect(), (1..(count+1)/2).collect())
    }
}

fn split_leader_with_minority(leader: u64, count: u64) -> (HashSet<u64>, HashSet<u64>) {
    let mut s1 = HashSet::new();
    let mut s2 = HashSet::new();
    let mut vec: Vec<u64> = (1..count + 1).collect();
    vec.swap(0, (leader - 1) as usize);
    for i in 0..(count - 1) / 2 {
        s1.insert(vec[i as usize]);
    }
    for i in (count - 1) / 2..count {
        s2.insert(vec[i as usize]);
    }
    (s1, s2)
}

fn test_partition_stale_read<T: Simulator>(cluster: &mut Cluster<T>, count: u64) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    let (k, v) = (b"k1", b"v1");
    cluster.must_put(k, v);
    assert_eq!(cluster.get(k), Some(v.to_vec()));

    let region_id = cluster.get_region_id(k);
    let peer = cluster.leader_of_region(region_id).unwrap();
    let leader = peer.get_store_id();

    let (s1, s2) = split_leader_with_minority(leader, count);
    cluster.partition(Arc::new(s1), Arc::new(s2));

    // new leader should be elected
    let v2 = b"v2";
    cluster.must_put(k, v2);
    if let Some(v) = cluster.get(k) {
        assert_eq!(v, v2);
    }
    must_get_equal(&cluster.get_engine(leader), k, v2);
}

fn test_partition_write<T: Simulator>(cluster: &mut Cluster<T>, count: u64) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    let (key, value) = (b"k1", b"v1");
    cluster.must_delete(key);

    let region_id = cluster.get_region_id(key);
    let peer = cluster.leader_of_region(region_id).unwrap();
    let leader = peer.get_store_id();

    let (s1, s2) = split_leader_with_majority(leader, count);
    cluster.partition(Arc::new(s1), Arc::new(s2));
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    cluster.reset_transport_hooks();

    let (s1, s2) = split_leader_with_minority(leader, count);
    cluster.partition(Arc::new(s1), Arc::new(s2));
    cluster.must_put(key, b"v2");
    assert_eq!(cluster.get(key), Some(b"v2".to_vec()));
    cluster.must_delete(key);
    assert_eq!(cluster.get(key), None);

    cluster.reset_transport_hooks();
    if let Some(_) = cluster.get(key) {
        assert!(false);
    }
}

#[test]
fn test_node_partition_stale_read() {
    let mut cluster = new_node_cluster(0, 5);
    test_partition_stale_read(&mut cluster, 5);
}

#[test]
fn test_server_partition_stale_read() {
    let mut cluster = new_server_cluster(0, 5);
    test_partition_stale_read(&mut cluster, 5);
}

#[test]
fn test_node_partition_write() {
    let mut cluster = new_node_cluster(0, 5);
    test_partition_write(&mut cluster, 5);
}

#[test]
fn test_server_partition_write() {
    let mut cluster = new_server_cluster(0, 5);
    test_partition_write(&mut cluster, 5);
}

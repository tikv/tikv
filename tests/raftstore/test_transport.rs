use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use std::collections::HashSet;

fn split_leader_with_majority(leader: u64, count: u64) -> (HashSet<u64>, HashSet<u64>) {
    let mut s1 = HashSet::new();
    let mut s2 = HashSet::new();
    let mut vec: Vec<u64> = (1..count + 1).collect();
    vec.swap(0, (leader - 1) as usize);
    for i in 0..count / 2 + 1 {
        s1.insert(vec[i as usize]);
    }
    for i in count / 2 + 1..count {
        s2.insert(vec[i as usize]);
    }
    (s1, s2)
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

fn test_partition_stale_read<T: Simulator>(cluster: &mut Cluster<T>) {
    let mut cluster = new_node_cluster(0, 5);
    cluster.bootstrap_region().expect("");
    cluster.start();

    let (k, v) = (b"k1", b"v1");
    cluster.must_put(k, v);
    assert_eq!(cluster.get(k), Some(v.to_vec()));

    let region_id = cluster.get_region_id(k);
    let peer = cluster.leader_of_region(region_id).unwrap();
    let leader = peer.get_store_id();

    let (s1, s2) = split_leader_with_minority(leader, 5);
    cluster.partition(&s1, &s2);

    let v2 = b"v2";
    cluster.must_put(k, v2);
    if let Some(v) = cluster.get(k) {
        assert_eq!(v, v2);
    }
}

fn test_partition_write<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.bootstrap_region().expect("");
    cluster.start();

    let (key, value) = (b"k1", b"v1");
    cluster.must_delete(key);

    let region_id = cluster.get_region_id(key);
    let peer = cluster.leader_of_region(region_id).unwrap();
    let leader = peer.get_store_id();

    let (s1, s2) = split_leader_with_majority(leader, 5);
    cluster.partition(&s1, &s2);

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    cluster.reset_transport_hooks();
    cluster.must_delete(key);

    let (s1, s2) = split_leader_with_minority(leader, 5);
    cluster.partition(&s1, &s2);
    cluster.must_put(key, value);
    if let Some(_) = cluster.get(key) {
        assert!(false);
    }
}

#[test]
fn test_node_partition_stale_read() {
    let mut cluster = new_node_cluster(0, 5);
    test_partition_stale_read(&mut cluster);
}

#[test]
fn test_server_partition_stale_read() {
    let mut cluster = new_server_cluster(0, 5);
    test_partition_stale_read(&mut cluster);
}

#[test]
fn test_node_partition_write() {
    let mut cluster = new_node_cluster(0, 5);
    test_partition_write(&mut cluster);
}

#[test]
fn test_server_partition_write() {
    let mut cluster = new_server_cluster(0, 5);
    test_partition_write(&mut cluster);
}

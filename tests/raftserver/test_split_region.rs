use std::time::Duration;

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util;
use tikv::pd::Client;
use tikv::util::HandyRwLock;

fn test_base_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_env_log();

    cluster.bootstrap_region().expect("");
    cluster.start();

    let pd_client = cluster.pd_client.clone();
    let cluster_id = cluster.id;

    let region = pd_client.rl().get_region(cluster_id, b"").unwrap();

    cluster.put(b"a1", b"v1");

    cluster.put(b"a3", b"v3");

    // Split with a2, so a1 must in left, and a3 in right.
    cluster.split_region(region.get_region_id(), Some(b"a2".to_vec()));

    let left = pd_client.rl().get_region(cluster_id, b"a1").unwrap();
    let right = pd_client.rl().get_region(cluster_id, b"a3").unwrap();

    assert_eq!(region.get_region_id(), left.get_region_id());
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(left.get_end_key(), right.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());

    cluster.put(b"a1", b"vv1");
    assert_eq!(cluster.get(b"a1").unwrap(), b"vv1".to_vec());

    cluster.put(b"a3", b"vv3");
    assert_eq!(cluster.get(b"a3").unwrap(), b"vv3".to_vec());

    let get = util::new_request(left.get_region_id(), vec![util::new_get_cmd(b"a3")]);
    let resp = cluster.request(left.get_region_id(), get, Duration::from_secs(3));
    assert!(resp.get_header().has_error());
    assert!(resp.get_header().get_error().has_key_not_in_region());
}

#[test]
fn test_node_base_split_region() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_base_split_region(&mut cluster);
}

#[test]
fn test_server_base_split_region() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_base_split_region(&mut cluster);
}

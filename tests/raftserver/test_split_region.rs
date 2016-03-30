use std::time::Duration;
use std::thread;
use std::cmp;
use rand::{self, Rng};

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util;
use tikv::pd::PdClient;
use tikv::util::HandyRwLock;
use tikv::raftserver::store::keys::data_key;
use tikv::raftserver::store::engine::Iterable;

pub const REGION_MAX_SIZE: u64 = 50000;
pub const REGION_SPLIT_SIZE: u64 = 30000;

fn test_base_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_log();

    cluster.bootstrap_region().expect("");
    cluster.start();

    let pd_client = cluster.pd_client.clone();
    let cluster_id = cluster.id();

    let region = pd_client.rl().get_region(cluster_id, b"").unwrap();

    cluster.put(b"a1", b"v1");

    cluster.put(b"a3", b"v3");

    // Split with a2, so a1 must in left, and a3 in right.
    cluster.split_region(region.get_id(), Some(b"a2".to_vec()));

    let left = pd_client.rl().get_region(cluster_id, b"a1").unwrap();
    let right = pd_client.rl().get_region(cluster_id, b"a3").unwrap();

    assert_eq!(region.get_id(), left.get_id());
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(left.get_end_key(), right.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());

    cluster.put(b"a1", b"vv1");
    assert_eq!(cluster.get(b"a1").unwrap(), b"vv1".to_vec());

    cluster.put(b"a3", b"vv3");
    assert_eq!(cluster.get(b"a3").unwrap(), b"vv3".to_vec());

    let epoch = left.get_region_epoch().clone();
    let get = util::new_request(left.get_id(), epoch, vec![util::new_get_cmd(b"a3")]);
    let region_id = left.get_id();
    let resp = cluster.call_command_on_leader(region_id, get, Duration::from_secs(3)).unwrap();
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

/// Keep puting random kvs until specified size limit is reached.
fn put_till_size<T: Simulator>(cluster: &mut Cluster<T>,
                               limit: u64,
                               range: &mut Iterator<Item = u64>)
                               -> Vec<u8> {
    let mut len = 0;
    let mut rng = rand::thread_rng();
    let mut max_key = vec![];
    while len < limit {
        let key = range.next().unwrap().to_string().into_bytes();
        let mut value = vec![0; 64];
        rng.fill_bytes(&mut value);
        cluster.put(&key, &value);
        // plus 1 for the extra encoding prefix
        len += key.len() as u64 + 1;
        len += value.len() as u64;
        if max_key < key {
            max_key = key;
        }
    }
    max_key
}

fn test_auto_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.store_cfg.split_region_check_tick_interval = 100;
    cluster.cfg.store_cfg.region_max_size = REGION_MAX_SIZE;
    cluster.cfg.store_cfg.region_split_size = REGION_SPLIT_SIZE;

    let check_size_diff = cluster.cfg.store_cfg.region_check_size_diff;
    let mut range = 1..;

    cluster.bootstrap_region().expect("");
    cluster.start();

    let pd_client = cluster.pd_client.clone();
    let cluster_id = cluster.id();

    let region = pd_client.rl().get_region(cluster_id, b"").unwrap();

    let last_key = put_till_size(cluster, REGION_SPLIT_SIZE, &mut range);

    // it should be finished in millis if split.
    thread::sleep(Duration::from_secs(1));

    let target = pd_client.rl().get_region(cluster_id, &last_key).unwrap();

    assert_eq!(region, target);

    let final_key = put_till_size(cluster,
                                  REGION_MAX_SIZE - REGION_SPLIT_SIZE + check_size_diff,
                                  &mut range);
    let max_key = cmp::max(last_key, final_key);

    thread::sleep(Duration::from_secs(1));

    let left = pd_client.rl().get_region(cluster_id, b"").unwrap();
    let right = pd_client.rl().get_region(cluster_id, &max_key).unwrap();

    assert!(left != right);
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(right.get_start_key(), left.get_end_key());
    assert_eq!(region.get_end_key(), right.get_end_key());
    assert_eq!(pd_client.rl().get_region(cluster_id, &max_key).unwrap(),
               right);
    assert_eq!(pd_client.rl().get_region(cluster_id, left.get_end_key()).unwrap(),
               right);

    let middle_key = left.get_end_key();
    let leader = cluster.leader_of_region(left.get_id()).unwrap();
    let node_id = leader.get_node_id();
    let mut size = 0;
    cluster.engines[&node_id]
        .scan(&data_key(b""),
              &data_key(middle_key),
              &mut |k, v| {
                  size += k.len() as u64;
                  size += v.len() as u64;
                  Ok(true)
              })
        .expect("");
    assert!(size <= REGION_SPLIT_SIZE);
    // although size may be smaller than util::REGION_SPLIT_SIZE, but the diff should
    // be small.
    assert!(size > REGION_SPLIT_SIZE - 1000);

    let epoch = left.get_region_epoch().clone();
    let get = util::new_request(left.get_id(), epoch, vec![util::new_get_cmd(&max_key)]);
    let region_id = left.get_id();
    let resp = cluster.call_command_on_leader(region_id, get, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().has_error());
    assert!(resp.get_header().get_error().has_key_not_in_region());
}

#[test]
fn test_node_auto_split_region() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

#[test]
fn test_server_auto_split_region() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

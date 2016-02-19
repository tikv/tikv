use std::time::Duration;

use tikv::raftserver::store::*;
use tikv::proto::raft_cmdpb::CommandType;

use super::util::*;
use super::cluster::Cluster;

#[test]
fn test_multi_store() {
    // init_env_log();

    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    let count = 5;
    let mut cluster = Cluster::new(0, count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    // Let raft run.
    sleep_ms(300);

    let (key, value) = (&keys::data_key(b"a1"), b"v1");

    let put = new_request(1, vec![new_put_cmd(&key, value)]);
    let resp = cluster.call_command_on_leader(put, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Put);

    assert_eq!(cluster.get_value(key), Some(value.to_vec()));

    let sync_count = cluster.count_fit_peer(|engine| {
        match engine.get_value(&key).unwrap() {
            None => false,
            Some(v) => &*v == value,
        }
    });
    assert!(sync_count > count / 2);

    let delete = new_request(1, vec![new_delete_cmd(&key)]);
    let resp = cluster.call_command_on_leader(delete, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Delete);

    assert_eq!(cluster.get_value(key), None);

    let sync_count = cluster.count_fit_peer(|engine| engine.get_value(&key).unwrap().is_none());
    assert!(sync_count > count / 2);
}

#[test]
fn test_multi_store_leader_crash() {
    let count = 5;
    let mut cluster = Cluster::new(0, count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    let (key, value) = (&keys::data_key(b"a1"), b"v1");

    sleep_ms(300);
    let put = new_request(1, vec![new_put_cmd(&key, value)]);
    let resp = cluster.call_command_on_leader(put, Duration::from_secs(3)).unwrap();
    assert!(!resp.get_header().has_error());

    let last_leader = cluster.leader().unwrap();
    cluster.stop_store(last_leader.get_store_id());

    sleep_ms(500);
    cluster.reset_leader();
    let new_leader = cluster.leader().expect("leader should be elected.");
    assert!(new_leader != last_leader);

    assert_eq!(cluster.get_value(key), Some(value.to_vec()));

    let (key2, value2) = (keys::data_key(b"a2"), b"v2");
    let put = new_request(1, vec![new_put_cmd(&key2, value2)]);
    let resp = cluster.call_command_on_leader(put, Duration::from_secs(3)).unwrap();
    assert!(!resp.get_header().has_error());

    assert!(cluster.engines[&last_leader.get_store_id()].get_value(&key2).unwrap().is_none());

    cluster.run_store(last_leader.get_store_id());

    sleep_ms(300);

    let v = cluster.engines[&last_leader.get_store_id()].get_value(&key2).unwrap().unwrap();
    assert_eq!(&*v, value2);
}

#[test]
fn test_multi_store_cluster_restart() {
    let count = 5;
    let mut cluster = Cluster::new(0, count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    let (key, value) = (&keys::data_key(b"a1"), b"v1");

    sleep_ms(300);

    assert!(cluster.leader().is_some());
    assert_eq!(cluster.get_value(key), None);
    let put = new_request(1, vec![new_put_cmd(&key, value)]);
    let resp = cluster.call_command_on_leader(put, Duration::from_secs(3)).unwrap();
    assert!(!resp.get_header().has_error());

    assert_eq!(cluster.get_value(key), Some(value.to_vec()));

    cluster.shutdown();
    cluster.run_all_stores();

    sleep_ms(300);

    assert!(cluster.leader().is_some());
    assert_eq!(cluster.get_value(key), Some(value.to_vec()));
}

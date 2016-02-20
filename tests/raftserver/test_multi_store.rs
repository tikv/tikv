use tikv::raftserver::store::*;

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

    let (key, value) = (b"a1", b"v1");

    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let sync_count = cluster.count_fit_peer(|engine| {
        match engine.get_value(&keys::data_key(key)).unwrap() {
            None => false,
            Some(v) => &*v == value,
        }
    });
    assert!(sync_count > count / 2);

    cluster.delete(key);
    assert_eq!(cluster.get(key), None);

    let sync_count = cluster.count_fit_peer(|engine| {
        engine.get_value(&keys::data_key(key)).unwrap().is_none()
    });
    assert!(sync_count > count / 2);
}

#[test]
fn test_multi_store_leader_crash() {
    let count = 5;
    let mut cluster = Cluster::new(0, count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    let (key1, value1) = (b"a1", b"v1");

    sleep_ms(300);

    cluster.put(key1, value1);

    let last_leader = cluster.leader().unwrap();
    cluster.stop_store(last_leader.get_store_id());

    sleep_ms(500);
    cluster.reset_leader();
    let new_leader = cluster.leader().expect("leader should be elected.");
    assert!(new_leader != last_leader);

    assert_eq!(cluster.get(key1), Some(value1.to_vec()));

    let (key2, value2) = (b"a2", b"v2");

    cluster.put(key2, value2);
    cluster.delete(key1);
    assert!(cluster.engines[&last_leader.get_store_id()]
                .get_value(&keys::data_key(key2))
                .unwrap()
                .is_none());
    assert!(cluster.engines[&last_leader.get_store_id()]
                .get_value(&keys::data_key(key1))
                .unwrap()
                .is_some());

    cluster.run_store(last_leader.get_store_id());

    sleep_ms(300);
    let v = cluster.engines[&last_leader.get_store_id()]
                .get_value(&keys::data_key(key2))
                .unwrap()
                .unwrap();
    assert_eq!(&*v, value2);
    assert!(cluster.engines[&last_leader.get_store_id()]
                .get_value(&keys::data_key(key1))
                .unwrap()
                .is_none());
}

#[test]
fn test_multi_store_cluster_restart() {
    let count = 5;
    let mut cluster = Cluster::new(0, count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    let (key, value) = (b"a1", b"v1");

    sleep_ms(300);

    assert!(cluster.leader().is_some());
    assert_eq!(cluster.get(key), None);
    cluster.put(key, value);

    assert_eq!(cluster.get(key), Some(value.to_vec()));

    cluster.shutdown();
    cluster.run_all_stores();

    sleep_ms(300);

    assert!(cluster.leader().is_some());
    assert_eq!(cluster.get(key), Some(value.to_vec()));
}

#[test]
fn test_multi_store_lost_majority() {
    let count = 5;
    let mut cluster = Cluster::new(0, count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_stores();

    sleep_ms(300);

    let half = count as u64 / 2 + 1;
    for i in 1..half + 1 {
        cluster.stop_store(i);
    }
    if let Some(leader) = cluster.leader() {
        if leader.get_store_id() >= half + 1 {
            cluster.stop_store(leader.get_store_id());
        }
    }
    cluster.reset_leader();
    sleep_ms(600);

    assert!(cluster.leader().is_none());
}

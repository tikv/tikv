use tikv::raftserver::store::*;
use tikv::proto::raftpb::ConfChangeType;

use super::cluster::Cluster;
use super::util::*;

#[test]
fn test_simple_conf_change() {
    init_env_log();

    let count = 5;
    let mut cluster = Cluster::new(0, count);

    cluster.bootstrap_conf_change();

    cluster.run_all_stores();

    // Let raft run.
    sleep_ms(300);

    // Now region 1 only has peer (1, 1, 1);
    let (key, value) = (b"a1", b"v1");

    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // add peer (2,2,2) to region 1.
    cluster.change_peer(1, ConfChangeType::ConfChangeAddNode, new_peer(2, 2, 2));

    let (key, value) = (b"a2", b"v2");
    cluster.put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // now peer 2 must have v1 and v2;
    let engine_2 = cluster.get_engine(2);
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a1")).unwrap().unwrap(),
               b"v1");
    assert_eq!(&*engine_2.get_value(&keys::data_key(b"a2")).unwrap().unwrap(),
               b"v2");

    // add peer (3, 3, 3) to region 1.
    cluster.change_peer(1, ConfChangeType::ConfChangeAddNode, new_peer(3, 3, 3));
    // Remove peer (2, 2, 2) from region 1.
    cluster.change_peer(1, ConfChangeType::ConfChangeRemoveNode, new_peer(2, 2, 2));

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

    // TODO: add more tests.
}

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use test_raftstore::*;

fn test_snapshot_encryption<T: Simulator>(cluster: &mut Cluster<T>) {
    configure_for_encryption(cluster);
    cluster.pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    for i in 0..100 {
        cluster.must_put(format!("key-{:02}", i).as_bytes(), b"value");
        cluster.must_put_cf("write", format!("key-{:02}", i).as_bytes(), b"value");
        cluster.must_put_cf("lock", format!("key-{:02}", i).as_bytes(), b"value");
    }

    cluster.pd_client.must_add_peer(r1, new_learner_peer(2, 2));
    cluster.pd_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"key-00", b"value");
    must_get_cf_equal(&cluster.get_engine(2), "lock", b"key-50", b"value");
    must_get_cf_equal(&cluster.get_engine(2), "write", b"key-99", b"value");
}

#[test]
fn test_node_snapshot_encryption() {
    let mut cluster = new_node_cluster(0, 2);
    test_snapshot_encryption(&mut cluster);
    std::mem::forget(cluster.take_path());
}

#[test]
fn test_server_snapshot_encryption() {
    let mut cluster = new_server_cluster(0, 2);
    test_snapshot_encryption(&mut cluster);
    std::mem::forget(cluster.take_path());
}

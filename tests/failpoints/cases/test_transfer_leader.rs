// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use fail;
use test_raftstore::*;

/// When a follower applies log slowly, leader should not transfer leader
/// to it. Otherwise, new leader may wait a long time to serve read/write
/// requests.
#[test]
fn test_transfer_leader_slow_apply() {
    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);

    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 1002));
    pd_client.must_add_peer(r1, new_peer(3, 1003));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let fp = "on_handle_apply_1003";
    fail::cfg(fp, "pause").unwrap();
    for i in 0..=cluster.cfg.raft_store.leader_transfer_max_log_lag {
        let bytes = format!("k{:03}", i).into_bytes();
        cluster.must_put(&bytes, &bytes);
    }
    cluster.transfer_leader(r1, new_peer(3, 1003));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    assert_ne!(cluster.leader_of_region(r1).unwrap(), new_peer(3, 1003));
    fail::remove(fp);
    cluster.must_transfer_leader(r1, new_peer(3, 1003));
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
}

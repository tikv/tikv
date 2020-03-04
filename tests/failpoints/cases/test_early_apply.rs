// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::thread;
use std::time::*;
use test_raftstore::*;

/// Tests early apply is disabled for singleton.
#[test]
fn test_singleton_early_apply() {
    let guard = crate::setup();
    let mut cluster = new_node_cluster(0, 3);
    let _drop_before_cluster = guard;
    cluster.cfg.raft_store.store_pool_size = 1;
    cluster.pd_client.disable_default_operator();
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster);
    cluster.run_conf_change();
    // Put one key first to cache leader.
    cluster.must_put(b"k0", b"v0");

    let store_1_fp = "raft_before_save_on_store_1";

    fail::cfg(store_1_fp, "pause").unwrap();
    cluster.async_put(b"k1", b"v1").unwrap();
    thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(1), b"k1");
    fail::remove(store_1_fp);
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    let r1 = cluster.get_region(b"k1");
    cluster.must_split(&r1, b"k2");
    cluster.pd_client.must_add_peer(r1.get_id(), new_peer(2, 2));
    cluster.get_region(b"k3");
    // Put key value to cache leader.
    cluster.must_put(b"k0", b"v0");
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k0", b"v0");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
    fail::cfg(store_1_fp, "pause").unwrap();
    cluster.async_put(b"k1", b"v2").unwrap();
    // Sleep a while so that leader receives follower's response and commit log.
    thread::sleep(Duration::from_millis(100));
    cluster.async_put(b"k4", b"v4").unwrap();
    fail::cfg(store_1_fp, "off").unwrap();
    // Sleep a bit to wait for the failpoint being waken up.
    thread::sleep(Duration::from_millis(1));
    fail::cfg(store_1_fp, "pause").unwrap();
    must_get_equal(&cluster.get_engine(1), b"k1", b"v2");
    must_get_none(&cluster.get_engine(1), b"k4");
    fail::remove(store_1_fp);
    must_get_equal(&cluster.get_engine(1), b"k4", b"v4");
}

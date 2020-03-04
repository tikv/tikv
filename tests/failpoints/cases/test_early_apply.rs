// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::*;
use std::thread;
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

    fail::cfg("raft_before_save", "pause()").unwrap();
    cluster.async_put(b"k1", b"v1").unwrap();
    thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(1), b"k1");
    fail::remove("raft_before_save");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    let r1 = cluster.get_region(b"k1");
    cluster.must_split(&r1, b"k2");
    cluster.pd_client.must_add_peer(r1.get_id(), new_peer(2, 2));
    cluster.get_region(b"k3");
    fail::cfg("raft_before_save", "pause()").unwrap();
    cluster.async_put(b"k1", b"v2").unwrap();
    // Sleep awhile so that leader receives follower's response and commit log.
    thread::sleep(Duration::from_millis(100));
    cluster.async_put(b"k3", b"v3").unwrap();
    fail::cfg("raft_before_save", "pause()").unwrap();
    must_get_equal(&cluster.get_engine(1), b"k1", b"v2");
    must_get_none(&cluster.get_engine(1), b"k3");
    fail::remove("raft_before_save");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
}
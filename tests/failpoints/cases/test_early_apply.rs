// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use raft::eraftpb::MessageType;
use std::thread;
use std::time::*;
use test_raftstore::*;

/// Tests early apply is disabled for singleton.
#[test]
fn test_singleton_early_apply() {
    let guard = crate::setup();
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.early_apply = true;
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
    thread::sleep(Duration::from_millis(5));
    fail::cfg(store_1_fp, "pause").unwrap();
    must_get_equal(&cluster.get_engine(1), b"k1", b"v2");
    must_get_none(&cluster.get_engine(1), b"k4");
    fail::remove(store_1_fp);
    must_get_equal(&cluster.get_engine(1), b"k4", b"v4");
}

/// Tests whether disabling early apply really works.
#[test]
fn test_disable_early_apply() {
    let guard = crate::setup();
    let mut cluster = new_node_cluster(0, 3);
    let _drop_early = guard;
    cluster.cfg.raft_store.early_apply = false;
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    let filter = RegionPacketFilter::new(1, 1)
        .msg_type(MessageType::MsgAppendResponse)
        .direction(Direction::Recv);
    cluster.add_send_filter(CloneFilterFactory(filter));
    let last_index = cluster.raft_local_state(1, 1).get_last_index();
    cluster.async_put(b"k2", b"v2").unwrap();
    cluster.wait_last_index(1, 1, last_index + 1, Duration::from_secs(3));
    fail::cfg("raft_before_save_on_store_1", "pause").unwrap();
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    must_get_none(&cluster.get_engine(1), b"k2");
}

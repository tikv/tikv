// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use raft::eraftpb::MessageType;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use test_raftstore::*;

// Test if a singleton can apply a log before persisting it.
#[test]
fn test_singleton_cannot_early_apply() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster);
    cluster.run();
    // Put one key first to cache leader.
    cluster.must_put(b"k0", b"v0");

    let store_1_fp = "raft_before_save_on_store_1";

    // Check singleton region can be scheduled correctly.
    fail::cfg(store_1_fp, "pause").unwrap();
    cluster.async_put(b"k1", b"v1").unwrap();
    sleep_ms(100);

    must_get_none(&cluster.get_engine(1), b"k1");

    fail::remove(store_1_fp);
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
}

#[test]
fn test_multi_early_apply() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.store_batch_system.pool_size = 1;
    cluster.run_conf_change();
    // Check mixed regions can be scheduled correctly.
    let r1 = cluster.get_region(b"k1");
    cluster.must_split(&r1, b"k2");
    cluster.pd_client.must_add_peer(r1.get_id(), new_peer(2, 2));
    // Put key value to cache leader.
    cluster.must_put(b"k0", b"v0");
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k0", b"v0");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");

    let store_1_fp = "raft_before_save_on_store_1";

    let executed = AtomicBool::new(false);
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .msg_type(MessageType::MsgAppend)
            // Just for callback, so never filter.
            .when(Arc::new(AtomicBool::new(false)))
            .set_msg_callback(Arc::new(move |_| {
                if !executed.swap(true, Ordering::SeqCst) {
                    fail::cfg(store_1_fp, "pause").unwrap();
                }
            })),
    ));
    cluster.async_put(b"k4", b"v4").unwrap();
    // Sleep a while so that follower will send append response.
    sleep_ms(100);
    cluster.async_put(b"k11", b"v22").unwrap();
    // Now the store thread of store 1 pauses on `store_1_fp`.
    // Set `store_1_fp` again to make this store thread does not pause on it.
    // Then leader 1 will receive the append response and commit the log.
    fail::cfg(store_1_fp, "pause").unwrap();
    must_get_equal(&cluster.get_engine(1), b"k4", b"v4");
    must_get_none(&cluster.get_engine(1), b"k11");
    fail::remove(store_1_fp);
    must_get_equal(&cluster.get_engine(1), b"k11", b"v22");
}

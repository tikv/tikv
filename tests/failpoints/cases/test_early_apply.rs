// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use raft::eraftpb::MessageType;
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
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster);

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

/// Test if the commit state check of apply msg is ok.
/// In the previous implementation, the commit state check uses the state of last
/// committed entry and it relies on the guarantee that the commit index and term
/// of the last committed entry must be monotonically increasing even between restarting.
/// However, this guarantee can be broken by
///     1. memory limitation of fetching committed entries
///     2. batching apply msg
/// Now the commit state uses the minimum of persist index and commit index from the peer
/// to fix this issue.
/// For simplicity, this test uses region merge to ensure that the apply state will be written
/// to kv db before crash.
#[test]
fn test_early_apply_yield_followed_with_many_entries() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();

    configure_for_merge(&mut cluster);
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");

    let left = cluster.get_region(b"k1");
    let right = cluster.get_region(b"k2");

    cluster.must_put(b"k2", b"v2");

    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");

    let left_peer_1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_peer_1);

    let right_peer_2 = find_peer(&right, 2).unwrap().to_owned();
    cluster.must_transfer_leader(right.get_id(), right_peer_2);

    let before_handle_normal_3_fp = "before_handle_normal_3";
    fail::cfg(before_handle_normal_3_fp, "pause").unwrap();

    // Put another key before CommitMerge to make write-to-kv-db really happen
    cluster.must_put(b"k3", b"v3");

    cluster.pd_client.must_merge(left.get_id(), right.get_id());

    let large_val = vec![b'a'; 1024 * 1024];
    // The size of these entries should be larger than MAX_COMMITTED_SIZE_PER_READY
    for i in 0..50 {
        cluster.must_put(format!("k1{}", i).as_bytes(), large_val.as_slice());
    }
    cluster.must_put(b"k150", b"v150");

    let after_handle_catch_up_logs_for_merge_1003_fp = "after_handle_catch_up_logs_for_merge_1003";
    fail::cfg(after_handle_catch_up_logs_for_merge_1003_fp, "return").unwrap();

    fail::remove(before_handle_normal_3_fp);

    // Wait for apply state writting to kv db
    sleep_ms(200);

    cluster.shutdown();

    fail::remove(after_handle_catch_up_logs_for_merge_1003_fp);

    cluster.start().unwrap();

    must_get_equal(&cluster.get_engine(3), b"k150", b"v150");
}

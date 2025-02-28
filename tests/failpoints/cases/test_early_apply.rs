// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use kvproto::raft_serverpb::RaftMessage;
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use raftstore::store::{InspectedRaftMessage, PeerMsg};
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv_util::future::block_on_timeout;

// Test if a singleton can apply a log before persisting it.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_singleton_cannot_early_apply() {
    let mut cluster = new_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster.cfg);

    cluster.run();
    // Put one key first to cache leader.
    cluster.must_put(b"k0", b"v0");

    let store_1_fp = "raft_before_save_on_store_1";

    // Check singleton region can be scheduled correctly.
    fail::cfg(store_1_fp, "pause").unwrap();
    let _ = cluster.async_put(b"k1", b"v1").unwrap();
    sleep_ms(100);

    must_get_none(&cluster.get_engine(1), b"k1");

    fail::remove(store_1_fp);
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_multi_early_apply() {
    let mut cluster = new_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.store_batch_system.pool_size = 1;
    cluster.cfg.raft_store.max_apply_unpersisted_log_limit = 0;
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster.cfg);

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
    let _ = cluster.async_put(b"k4", b"v4").unwrap();
    // Sleep a while so that follower will send append response
    sleep_ms(100);
    let _ = cluster.async_put(b"k11", b"v22").unwrap();
    // Sleep a while so that follower will send append response.
    sleep_ms(100);
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
/// In the previous implementation, the commit state check uses the state of
/// last committed entry and it relies on the guarantee that the commit index
/// and term of the last committed entry must be monotonically increasing even
/// between restarting. However, this guarantee can be broken by
///     1. memory limitation of fetching committed entries
///     2. batching apply msg
/// Now the commit state uses the minimum of persist index and commit index from
/// the peer to fix this issue.
/// For simplicity, this test uses region merge to ensure that the apply state
/// will be written to kv db before crash.
///
/// Note: partitioned-raft-kv does not need this due to change in disk
/// persistence logic
#[test]
fn test_early_apply_yield_followed_with_many_entries() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();

    configure_for_merge(&mut cluster.cfg);
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

    // Wait for apply state writing to kv db
    sleep_ms(200);

    cluster.shutdown();

    fail::remove(after_handle_catch_up_logs_for_merge_1003_fp);

    cluster.start().unwrap();

    must_get_equal(&cluster.get_engine(3), b"k150", b"v150");
}

// Test the consistency of EntryCache when partitioned leader contains
// uncommitted propose, and after the partition is recovered, it can replicate
// raft entries for new leader correctly. This case tests the corner scenario
// that partitioned leader receives a new Append msg from new elected leader and
// the new entries are already committed and overlap with existing
// uncommitted entries in the entry cache, it may cause panic if handled
// incorrectly. See issue https://github.com/tikv/tikv/issues/17868 for more details.
#[test]
fn test_early_apply_leader_demote_by_append() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    let region = cluster.pd_client.get_region(b"k1").unwrap();
    cluster.must_transfer_leader(region.id, new_peer(1, 1));

    // isolate 1 for 2,3
    let dropped_append: Arc<Mutex<RaftMessage>> = Arc::new(Mutex::new(RaftMessage::default()));
    let msg_ref = dropped_append.clone();
    cluster.add_recv_filter_on_node(
        1,
        Box::new(DropMessageFilter::new(Arc::new(move |m| {
            // save dropped append.
            if m.get_message().msg_type == MessageType::MsgAppend {
                *msg_ref.lock().unwrap() = m.clone();
            }

            false
        }))),
    );
    for id in [2, 3] {
        cluster.add_recv_filter_on_node(
            id,
            Box::new(DropMessageFilter::new(Arc::new(|m| {
                m.get_message().from != 1
            }))),
        );
    }

    // propose a new write, the write should timeout.
    let ch = cluster.async_put(b"k1", b"v2").unwrap();
    block_on_timeout(ch, Duration::from_millis(100)).unwrap_err();

    let fp = "pause_on_peer_collect_message";
    // pause peer 1 to wait for leader timeout.
    fail::cfg(fp, "pause").unwrap();

    // wait for leader timeout.
    sleep_ms(200);

    cluster.reset_leader_of_region(region.id);
    let leader = cluster.leader_of_region(region.id).unwrap();
    assert_ne!(leader.store_id, 1);

    cluster.must_put(b"k1", b"v3");

    // Send the dropped msg to store 1.
    let mut msg = dropped_append.lock().unwrap().clone();
    assert_eq!(msg.get_message().to, 1);
    // Advance the committed index of the Append msg to trigger the corner case.
    // I don't find an easy way to trigger this kind of msg, so direct modify the
    // first append to mock that scenario.
    let entry_idx = msg
        .get_message()
        .get_entries()
        .last()
        .map(|e| e.index)
        .unwrap();
    msg.mut_message().commit = entry_idx;
    let peer_msg = PeerMsg::RaftMessage(Box::new(InspectedRaftMessage { heap_size: 0, msg }), None);
    cluster.get_router(1).unwrap().send(1, peer_msg).unwrap();

    for i in 1..=3 {
        cluster.clear_recv_filter_on_node(i);
    }

    // remove fp.
    fail::remove(fp);
    cluster.must_put(b"k1", b"v4");
    eventually_get_equal(&cluster.get_engine(1), b"k1", b"v4");
}

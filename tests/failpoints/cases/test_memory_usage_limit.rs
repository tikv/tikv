// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use raft::eraftpb::MessageType;
use raftstore::store::MEMTRACE_ENTRY_CACHE;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

// Test even if memory usage reaches high water, committed entries can still get applied slowly.
#[test]
fn test_memory_usage_reaches_high_water() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    cluster.run_conf_change();

    fail::cfg("memory_usage_reaches_high_water", "return").unwrap();
    for i in 0..10 {
        let k = format!("k{:02}", i).into_bytes();
        cluster.must_put(&k, b"value");
        must_get_equal(&cluster.get_engine(1), &k, b"value");
    }
    fail::cfg("memory_usage_reaches_high_water", "off").unwrap();
}

#[test]
fn test_evict_entry_cache() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    // Don't consider life time when clearing entry cache.
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::secs(1000);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Don't compact raft log even if failpoint `on_raft_gc_log_tick` is disabled.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Forbid store 1 to clean entry cache.
    fail::cfg("on_raft_gc_log_tick_1", "pause").unwrap();
    fail::cfg("on_entry_cache_evict_tick", "return").unwrap();

    let value = vec![b'x'; 1024];
    for i in 0..100 {
        let k = format!("k{:02}", i).into_bytes();
        cluster.must_put(&k, &value);
        must_get_equal(&cluster.get_engine(1), &k, &value);
    }

    sleep_ms(500); // Wait to trigger a raft log compaction.
    let entry_cache_size = MEMTRACE_ENTRY_CACHE.sum();
    // Entries on store 2 will be cleaned, but on store 1 won't.
    assert!(entry_cache_size > 100 * 1024);

    fail::cfg("memory_usage_reaches_high_water", "return").unwrap();
    fail::cfg("needs_evict_entry_cache", "return").unwrap();
    fail::cfg("on_raft_gc_log_tick_1", "off").unwrap();

    sleep_ms(500); // Wait to trigger a raft log compaction.
    let entry_cache_size = MEMTRACE_ENTRY_CACHE.sum();
    // Entries on store 1 will be evict even if they are still in life time.
    assert!(entry_cache_size < 50 * 1024);

    fail::cfg("memory_usage_reaches_high_water", "off").unwrap();
    fail::cfg("needs_evict_entry_cache", "off").unwrap();
    fail::cfg("on_entry_cache_evict_tick", "off").unwrap();
}

// Test Raft leader will pause a follower if it meets memory full.
#[test]
fn test_memory_full_cause_of_raft_message() {
    let mut cluster = new_server_cluster(0, 2);
    let pd_client = cluster.pd_client.clone();
    cluster.pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(r1, new_learner_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let append_to_2 = Arc::new(AtomicUsize::new(0));
    let append_to_2_ = append_to_2.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend)
            .allow(usize::MAX)
            .set_msg_callback(Arc::new(move |_m| {
                append_to_2_.fetch_add(1, Ordering::SeqCst);
            })),
    ));

    let response_to_1 = Arc::new(AtomicUsize::new(0));
    let response_to_1_ = response_to_1.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1, 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgUnreachable)
            .allow(usize::MAX)
            .set_msg_callback(Arc::new(move |_m| {
                response_to_1_.fetch_add(1, Ordering::SeqCst);
            })),
    ));

    // A MsgHeartbeatResponse will trigger one MsgAppend.
    fail::cfg("needs_reject_raft_append", "return").unwrap();
    (0..10).for_each(|_| cluster.must_put(b"k1", b"v1"));
    let now = Instant::now();
    while now.elapsed() < Duration::from_secs(2) {
        sleep_ms(100);
        let appends = append_to_2.load(Ordering::SeqCst);
        if appends < 10 {
            continue;
        }
        let responses = response_to_1.load(Ordering::SeqCst);
        if responses < 10 {
            continue;
        }
        fail::cfg("needs_delay_raft_append", "off").unwrap();
        return;
    }
    panic!("must have 10 MsgAppend and 10 MsgUnreachable");
}

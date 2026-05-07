// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use more_asserts::assert_ge;
use raft::eraftpb::MessageType;
use raftstore::store::MEMTRACE_ENTRY_CACHE;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

// Test even if memory usage reaches high water, committed entries can still get
// applied slowly.
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

fn setup_server_cluster() -> Cluster<ServerCluster> {
    let mut cluster = new_server_cluster(0, 2);
    let pd_client = cluster.pd_client.clone();
    cluster.pd_client.disable_default_operator();
    cluster.run_conf_change();
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster
}

fn put_n_entries(cluster: &mut Cluster<ServerCluster>, num_entries: usize, value_size: usize) {
    let value = vec![b'x'; value_size];
    (0..num_entries).for_each(|_| cluster.must_put(b"k1", &value));
}

fn add_message_filter(
    cluster: &mut Cluster<ServerCluster>,
    region_id: u64,
    peer_id: u64,
    msg_type: MessageType,
    counter: Arc<AtomicUsize>,
) {
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(region_id, peer_id)
            .direction(Direction::Recv)
            .msg_type(msg_type)
            .allow(usize::MAX)
            .set_msg_callback({
                let counter = Arc::clone(&counter);
                Arc::new(move |_m| {
                    counter.fetch_add(1, Ordering::SeqCst);
                })
            }),
    ));
}

fn add_filter_append_and_unreachable(
    cluster: &mut Cluster<ServerCluster>,
    append_to_2: Arc<AtomicUsize>,
    response_to_1: Arc<AtomicUsize>,
) {
    add_message_filter(
        cluster,
        1,
        2,
        MessageType::MsgAppend,
        Arc::clone(&append_to_2),
    );
    add_message_filter(
        cluster,
        1,
        1,
        MessageType::MsgUnreachable,
        Arc::clone(&response_to_1),
    );
}

fn wait_msg_counter(
    append_counter: Arc<AtomicUsize>,
    unreachable_counter: Arc<AtomicUsize>,
    num_appends: usize,
    num_unreachable: usize,
) {
    let timeout = Instant::now() + Duration::from_secs(2);
    loop {
        if Instant::now() > timeout {
            break;
        }

        let appends = append_counter.load(Ordering::SeqCst);
        let responses = unreachable_counter.load(Ordering::SeqCst);

        if appends >= num_appends && responses >= num_unreachable {
            break;
        }
        sleep_ms(50);
    }
}

// Test Raft leader will pause a follower if it meets memory full.
#[test]
fn test_memory_full_cause_of_raft_message() {
    let mut cluster = setup_server_cluster();

    let append_to_2 = Arc::new(AtomicUsize::new(0));
    let unreachable_to_2 = Arc::new(AtomicUsize::new(0));
    add_filter_append_and_unreachable(&mut cluster, append_to_2.clone(), unreachable_to_2.clone());

    fail::cfg("needs_reject_raft_append", "return").unwrap();
    put_n_entries(&mut cluster, 10, 1024);
    wait_msg_counter(append_to_2.clone(), unreachable_to_2.clone(), 10, 10);
    assert_ge!(append_to_2.load(Ordering::SeqCst), 10);
    assert_ge!(unreachable_to_2.load(Ordering::SeqCst), 10);
}

#[test]
fn test_evict_early_avoids_reject() {
    let mut cluster = setup_server_cluster();
    // Set a long lifetime to avoid clearing the entry cache based on time.
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::secs(1000);

    fail::cfg("on_raft_gc_log_tick", "return").unwrap();
    fail::cfg("on_entry_cache_evict_tick", "return").unwrap();

    // Initially disable `evict_entry_cache` to prevent eviction during the first
    // round of insertions.
    fail::cfg("mock_evict_entry_cache", "return").unwrap();
    // Insert a batch of entries and verify that the entry cache size increases as
    // expected.
    put_n_entries(&mut cluster, 10, 1024);
    let entry_cache_size_before = MEMTRACE_ENTRY_CACHE.sum();
    assert!(
        entry_cache_size_before > 10 * 1024,
        "Entry cache should not have been evicted during the first roundd"
    );
    // Remove the failpoint to allow `evict_entry_cache` for subsequent operations.
    fail::remove("mock_evict_entry_cache");

    // Track MsgAppend and MsgUnreachable.
    let append_to_2 = Arc::new(AtomicUsize::new(0));
    let unreachable_to_2 = Arc::new(AtomicUsize::new(0));
    add_filter_append_and_unreachable(&mut cluster, append_to_2.clone(), unreachable_to_2.clone());

    // Simulate high memory usage conditions.
    // The memory usage is set to 9216, which is close to the high water mark of
    // 10240. We use 9216 since 9216 < MEMORY_HIGH_WATER_MARGIN_SPLIT_POINT, and
    // 10240 * MEMORY_HIGH_WATER_MARGIN_RATIO = 9216.
    fail::cfg("mock_memory_usage", "return(9216)").unwrap();
    fail::cfg("mock_memory_usage_high_water", "return(10240)").unwrap();
    fail::cfg("mock_memory_usage_entry_cache", "return(10240)").unwrap();

    // Insert another batch of entries and ensure that no `MsgUnreachable` messages
    // are sent, indicating no rejection occurred during the append
    // process.
    put_n_entries(&mut cluster, 10, 1024);
    wait_msg_counter(append_to_2.clone(), unreachable_to_2.clone(), 10, 0);
    assert!(
        append_to_2.load(Ordering::SeqCst) == 10 ||
         // This value could be 11 if a no-op entry.
        append_to_2.load(Ordering::SeqCst) == 11
    );
    assert_eq!(unreachable_to_2.load(Ordering::SeqCst), 0);

    // Verify that `evict_entry_cache` was triggered by checking that the entry
    // cache size has decreased.
    let entry_cache_size_after = MEMTRACE_ENTRY_CACHE.sum();
    assert!(
        entry_cache_size_after < entry_cache_size_before,
        "Entry cache should be evicted after the second round of insertions"
    );
}

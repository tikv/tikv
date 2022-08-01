// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use kvproto::raft_serverpb::ExtraMessageType;
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

// test leader behavior when follower memory is insufficient.
#[test]
fn test_leader_behavior_when_follower_memory_insufficient() {
    let mut cluster = new_server_cluster(0, 2);
    let pd_client = cluster.pd_client.clone();
    cluster.pd_client.disable_default_operator();

    // raft_base_tick_interval 10ms
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.tikv.raft_store.raft_heartbeat_ticks = 10;
    cluster.cfg.tikv.raft_store.raft_election_timeout_ticks = 200;
    cluster.cfg.tikv.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(2000);
    cluster.cfg.tikv.raft_store.peer_stale_state_check_interval = ReadableDuration::millis(20000);
    cluster.cfg.tikv.raft_store.abnormal_leader_missing_duration = ReadableDuration::millis(40000);
    cluster.cfg.tikv.raft_store.max_leader_missing_duration = ReadableDuration::millis(50000);

    let r1 = cluster.run_conf_change();

    pd_client.must_add_peer(r1, new_learner_peer(2, 2));

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

    let heartbeat = Arc::new(AtomicUsize::new(0));
    let heartbeat_ = heartbeat.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1, 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgHeartbeatResponse)
            .allow(usize::MAX)
            .set_msg_callback(Arc::new(move |m| {
                heartbeat_.fetch_add(1, Ordering::SeqCst);
                debug!("get a MsgHeartbeatResponse msg, and msg is: {:?}", m);
            })),
    ));

    let response_to_1 = Arc::new(AtomicUsize::new(0));
    let response_to_1_ = response_to_1.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1, 1)
            .direction(Direction::Recv)
            .extra_msg_type(ExtraMessageType::MsgRejectRaftLogCausedByMemoryUsage)
            .allow(usize::MAX)
            .set_msg_callback(Arc::new(move |m| {
                response_to_1_.fetch_add(1, Ordering::SeqCst);
                debug!(
                    "get a MsgRejectRaftLogCausedByMemoryUsage msg, and msg is: {:?}",
                    m
                );
            })),
    ));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    let mut value_cnt = 0;
    let t = cluster.cfg.tikv.raft_store.raft_heartbeat_ticks * 10 / 5;
    loop {
        heartbeat.store(0, Ordering::SeqCst);
        let loop_start = Instant::now();
        debug!("loop_start time");
        let first_hbt = loop {
            let h = heartbeat.load(Ordering::SeqCst);
            if h == 0 {
                sleep_ms(t as u64);
                continue;
            } else {
                debug!("enable failpoint");
                fail::cfg("needs_reject_raft_append", "return").unwrap();
                break h;
            }
        };

        debug!("first_heartbeat={}", first_hbt);
        append_to_2.store(0, Ordering::Release);
        response_to_1.store(0, Ordering::Release);

        debug!("put value");
        // Trigger flow control so that the message flow of the replica becomes the Proble state.
        cluster.must_put(
            &(String::from("k") + (value_cnt + 2).to_string().as_str()).into_bytes(),
            &(String::from("v") + (value_cnt + 2).to_string().as_str()).into_bytes(),
        );
        value_cnt += 1;

        debug!("put value");
        // Trigger Probe state to pause.
        cluster.must_put(
            &(String::from("k") + (value_cnt + 2).to_string().as_str()).into_bytes(),
            &(String::from("v") + (value_cnt + 2).to_string().as_str()).into_bytes(),
        );
        value_cnt += 1;

        let mut a22 = append_to_2.load(Ordering::SeqCst);
        let mut r21 = response_to_1.load(Ordering::SeqCst);

        debug!(
            "heartbeat={}, append_to_2={}, response_to_1={}",
            heartbeat.load(Ordering::SeqCst),
            a22,
            r21
        );
        loop {
            if a22 == 0 || r21 == 0 {
                sleep_ms(6);
                a22 = append_to_2.load(Ordering::SeqCst);
                r21 = response_to_1.load(Ordering::SeqCst);
            } else {
                break;
            }
        }
        debug!("put value");
        // check the append and response count.
        cluster.must_put(
            &(String::from("k") + (value_cnt + 2).to_string().as_str()).into_bytes(),
            &(String::from("v") + (value_cnt + 2).to_string().as_str()).into_bytes(),
        );
        value_cnt += 1;
        let cur_hbt = heartbeat.load(Ordering::SeqCst);
        let a22_cur = append_to_2.load(Ordering::SeqCst);
        let r21_cur = response_to_1.load(Ordering::SeqCst);

        debug!(
            "heartbeat={}, append_to_2={}, cur response_to_1={}",
            heartbeat.load(Ordering::SeqCst),
            a22_cur,
            r21_cur
        );
        debug!("heartbeat 2 is {}", cur_hbt);
        if cur_hbt == first_hbt {
            debug!(
                "check: cur_heartbeat={}, a22_cur={}, a22={}, r21_cur={}, r21={}",
                cur_hbt, a22_cur, a22, r21_cur, r21
            );
            assert!(a22_cur == a22 && r21_cur == r21);
        } else {
            continue;
        }

        let mut cur_hbt = heartbeat.load(Ordering::SeqCst);
        loop {
            if cur_hbt == first_hbt {
                sleep_ms(t as u64);
                cur_hbt = heartbeat.load(Ordering::SeqCst);
                continue;
            } else {
                break;
            }
        }

        debug!("put value after new heartbeat");
        cluster.must_put(
            &(String::from("k") + (value_cnt + 2).to_string().as_str()).into_bytes(),
            &(String::from("v") + (value_cnt + 2).to_string().as_str()).into_bytes(),
        );
        let a22_after_heartbeat = append_to_2.load(Ordering::SeqCst);
        let r21_after_heartbeat = response_to_1.load(Ordering::SeqCst);
        debug!(
            "check after new heartbeat, heartbeat count = {}, a22 = {}, r21 = {}; before new heartbeat, a22={}, r21={}",
            cur_hbt, a22_after_heartbeat, r21_after_heartbeat, a22_cur, r21_cur
        );
        assert!(a22_after_heartbeat > a22_cur && r21_after_heartbeat > r21_cur);

        debug!("loop duration is: {:?}", loop_start.elapsed());
        fail::remove("needs_reject_raft_append");
        break;
    }
}

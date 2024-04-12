// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc, Mutex},
    time::Duration,
};

use collections::HashMap;
use engine_traits::{Peekable, CF_RAFT};
use kvproto::raft_serverpb::RaftApplyState;
use pd_client::PdClient;
use raftstore::store::*;
use test_raftstore::*;
use tikv_util::config::*;

// Test the case that cache is compacted when one node down to check
// if it goes well after the node is back online which triggers async fetch.
#[test]
fn test_node_async_fetch() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);

    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 50;
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.raft_log_reserve_max_ticks = 2;
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        must_get_equal(&engines.kv, b"k1", b"v1");
        let mut state: RaftApplyState = engines
            .kv
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap_or_default();
        let state = state.take_truncated_state();
        // compact should not start
        assert_eq!(RAFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(RAFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    cluster.stop_node(1);

    for i in 1..60u32 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    // wait log gc.
    sleep_ms(500);

    let (sender, receiver) = mpsc::channel();
    let sync_sender = Mutex::new(sender);
    fail::cfg_callback("on_async_fetch_return", move || {
        let sender = sync_sender.lock().unwrap();
        sender.send(true).unwrap();
    })
    .unwrap();
    cluster.run_node(1).unwrap();

    // limit has not reached, should not gc.
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = engines
            .kv
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap_or_default();
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        assert_eq!(idx, before_state.get_index());
    }

    assert_eq!(
        receiver.recv_timeout(Duration::from_millis(500)).unwrap(),
        true
    );

    // logs should be replicated to node 1 successfully.
    for i in 1..60u32 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        must_get_equal(&cluster.engines[&1].kv, &k, &v);
    }

    for i in 60..500u32 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        let v2 = cluster.get(&k);
        assert_eq!(v2, Some(v));

        if i > 100
            && check_compacted(
                &cluster.engines,
                &before_states,
                1,
                false, // must_compacted
            )
        {
            return;
        }
    }
    check_compacted(
        &cluster.engines,
        &before_states,
        1,
        true, // must_compacted
    );
}

#[test]
fn test_persist_delay_block_log_compaction() {
    let mut cluster = new_node_cluster(0, 3);

    cluster.cfg.raft_store.cmd_batch_concurrent_ready_max_count = 0;
    cluster.cfg.raft_store.store_io_pool_size = 1;
    cluster.cfg.raft_store.max_apply_unpersisted_log_limit = 10000;

    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 50;
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.raft_log_reserve_max_ticks = 2;
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::millis(100);
    cluster.run();

    let region = cluster.pd_client.get_region(b"k1").unwrap();
    let peer_1 = find_peer(&region, 1).cloned().unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_1);

    let raft_before_save_on_store_1_fp = "raft_before_persist_on_store_1";

    for i in 0..100 {
        let k = format!("k{}", i).into_bytes();
        let v = "v1".as_bytes().to_owned();
        cluster.must_put(&k, &v);
    }
    // Wait log gc.
    sleep_ms(100);

    let mut before_states = HashMap::default();
    for (&id, engines) in &cluster.engines {
        must_get_equal(&engines.kv, b"k1", b"v1");
        let mut state: RaftApplyState = engines
            .kv
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap_or_default();
        let state = state.take_truncated_state();
        println!("  store id: {}, truncate state: {:?}", id, &state);
        // Should trigger compact.
        assert!(state.get_index() > RAFT_INIT_LOG_INDEX);
        assert!(state.get_term() > RAFT_INIT_LOG_TERM);
        before_states.insert(id, state);
    }

    // Skip persisting to simulate raft log persist lag but not block node restart.
    fail::cfg(raft_before_save_on_store_1_fp, "pause").unwrap();

    for i in 0..100 {
        let k = format!("k{}", i).into_bytes();
        let v = "v2".as_bytes().to_owned();
        cluster.must_put(&k, &v);
    }
    for i in 0..100 {
        let k = format!("k{}", i).into_bytes();
        must_get_equal(&cluster.engines[&1].kv, &k, "v2".as_bytes());
    }

    // Wait log gc.
    sleep_ms(100);
    // Log perisist is block, should not trigger log gc.
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = engines
            .kv
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap_or_default();
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        assert!(idx <= before_state.get_index() + 10);
    }

    fail::remove(raft_before_save_on_store_1_fp);

    // Wait log persist and trigger gc.
    sleep_ms(200);

    // Log perisist is block, should not trigger log gc.
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = engines
            .kv
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(1))
            .unwrap()
            .unwrap_or_default();
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        assert!(idx > before_state.get_index() + 100);
    }
}

// Test the case that async fetch is performed well while the peer is removed.
#[test]
fn test_node_async_fetch_remove_peer() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    cluster.pd_client.disable_default_operator();

    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 50;
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.raft_log_reserve_max_ticks = 2;
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // cause log lag and trigger async fetch
    cluster.stop_node(5);
    for i in 1..60 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }
    fail::cfg("worker_async_fetch_raft_log", "pause").unwrap();
    cluster.run_node(5).unwrap();

    // make sure destroy_peer and on_entries_fetched are called in one batch.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    fail::cfg("worker_gc_raft_log", "pause").unwrap();
    cluster.pd_client.must_remove_peer(1, new_peer(1, 1));
    fail::cfg("pause_on_peer_collect_message", "pause").unwrap();
    fail::remove("worker_gc_raft_log");
    sleep_ms(10);
    fail::remove("worker_async_fetch_raft_log");
    sleep_ms(10);
    fail::remove("pause_on_peer_collect_message");

    cluster.pd_client.must_add_peer(1, new_peer(1, 1));

    // logs should be replicated to node 1 successfully.
    for i in 1..60 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        must_get_equal(&cluster.get_engine(1), &k, &v);
    }
}

// Test the case that async fetch is performed well after the leader has stepped
// down to follower and is applying snapshot.
#[test]
fn test_node_async_fetch_leader_change() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    cluster.pd_client.disable_default_operator();

    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(80);
    cluster.cfg.raft_store.raft_log_gc_threshold = 50;
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.raft_log_reserve_max_ticks = 2;
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // cause log lag and trigger async fetch
    cluster.stop_node(5);
    for i in 1..60 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }
    fail::cfg("worker_async_fetch_raft_log", "pause").unwrap();
    cluster.run_node(5).unwrap();

    cluster.must_transfer_leader(1, new_peer(2, 2));
    // make recent active
    cluster.pd_client.must_remove_peer(1, new_peer(5, 5));
    cluster.pd_client.must_add_peer(1, new_peer(5, 5));
    // trigger log gc
    for i in 60..100 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }
    sleep_ms(100);

    // isolate node 1
    cluster.add_send_filter(IsolationFilterFactory::new(1));
    fail::cfg("apply_pending_snapshot", "return").unwrap();
    // cause log lag and trigger log gc and make node 1 requesting snapshot
    for i in 100..200 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }
    // wait log gc.
    sleep_ms(100);

    // trigger applying snapshot
    cluster.clear_send_filters();
    sleep_ms(100);
    fail::remove("worker_async_fetch_raft_log");

    sleep_ms(100);
    fail::remove("apply_pending_snapshot");

    // logs should be replicated to node 1 successfully.
    for i in 1..200 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        must_get_equal(&cluster.get_engine(1), &k, &v);
    }
}

// Test the case whether entry cache is reserved for the newly added peer.
#[test]
fn test_node_compact_entry_cache() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    cluster.pd_client.disable_default_operator();

    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.raft_log_reserve_max_ticks = 2;
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k0", b"v0");
    cluster.pd_client.must_remove_peer(1, new_peer(5, 5));

    // pause snapshot applied
    fail::cfg("before_region_gen_snap", "pause").unwrap();
    fail::cfg("worker_async_fetch_raft_log", "pause").unwrap();
    // change one peer to learner
    cluster.pd_client.add_peer(1, new_learner_peer(5, 5));

    // cause log lag and pause async fetch to check if entry cache is reserved for
    // the learner
    for i in 1..6 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }
    std::thread::sleep(Duration::from_millis(100));

    fail::remove("before_region_gen_snap");
    cluster.pd_client.must_have_peer(1, new_learner_peer(5, 5));

    // if entry cache is not reserved, the learner will not be able to catch up.
    must_get_equal(&cluster.get_engine(5), b"5", b"5");
}

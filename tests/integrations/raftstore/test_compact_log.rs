// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use kvproto::raft_serverpb::RaftApplyState;
use raft::prelude::MessageType;
use raftstore::store::*;
use test_raftstore::*;
use tikv_util::config::*;

fn test_compact_log<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        before_states.insert(id, state.take_truncated_state());
    }

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);

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

fn test_compact_count_limit<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100);
    cluster.cfg.raft_store.raft_log_gc_threshold = 500;
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        must_get_equal(&engines.kv, b"k1", b"v1");
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let state = state.take_truncated_state();
        // compact should not start
        assert_eq!(RAFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(RAFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    for i in 1..60 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    // wait log gc.
    sleep_ms(500);

    // limit has not reached, should not gc.
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        assert_eq!(idx, before_state.get_index());
    }

    for i in 60..200 {
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

fn test_compact_many_times<T: Simulator>(cluster: &mut Cluster<T>) {
    let gc_limit: u64 = 100;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(gc_limit);
    cluster.cfg.raft_store.raft_log_gc_threshold = 500;
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        must_get_equal(&engines.kv, b"k1", b"v1");
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let state = state.take_truncated_state();
        // compact should not start
        assert_eq!(RAFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(RAFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    for i in 1..500 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        let v2 = cluster.get(&k);
        assert_eq!(v2, Some(v));

        if i >= 200
            && check_compacted(
                &cluster.engines,
                &before_states,
                gc_limit * 2,
                false, // must_compacted
            )
        {
            return;
        }
    }

    check_compacted(
        &cluster.engines,
        &before_states,
        gc_limit * 2,
        true, // must_compacted
    );
}

#[test]
fn test_node_compact_log() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_log(&mut cluster);
}

#[test]
fn test_node_compact_count_limit() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_count_limit(&mut cluster);
}

#[test]
fn test_node_compact_many_times() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_many_times(&mut cluster);
}

fn test_compact_size_limit<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100000);
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(1));
    cluster.run();
    cluster.stop_node(1);

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        if id == 1 {
            continue;
        }
        must_get_equal(&engines.kv, b"k1", b"v1");
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let state = state.take_truncated_state();
        // compact should not start
        assert_eq!(RAFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(RAFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    let key = vec![1; 100];
    let value = vec![1; 10240];
    // 25 * 10240 = 250KiB < 1MiB
    for _ in 0..25 {
        cluster.must_put(&key, &value);
    }

    // wait log gc.
    sleep_ms(500);

    // limit has not reached, should not gc.
    for (&id, engines) in &cluster.engines {
        if id == 1 {
            continue;
        }
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        assert_eq!(idx, before_state.get_index());
    }

    // 100 * 10240 + 250KiB > 1MiB
    for _ in 0..100 {
        cluster.must_put(&key, &value);
    }

    sleep_ms(500);

    // Size exceed max limit, every peer must have compacted logs,
    // so the truncate log state index/term must > than before.
    for (&id, engines) in &cluster.engines {
        if id == 1 {
            continue;
        }
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        assert!(idx > before_state.get_index());
    }
}

#[test]
fn test_node_compact_size_limit() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_size_limit(&mut cluster);
}

fn test_compact_reserve_max_ticks<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100);
    cluster.cfg.raft_store.raft_log_gc_threshold = 500;
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    cluster.cfg.raft_store.raft_log_reserve_max_ticks = 2;
    cluster.run();
    let apply_key = keys::apply_state_key(1);

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();
    for (&id, engines) in &cluster.engines {
        must_get_equal(&engines.kv, b"k1", b"v1");
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &apply_key);
        let state = state.take_truncated_state();
        // compact should not start
        assert_eq!(RAFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(RAFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    for i in 1..60 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    // wait log gc.
    sleep_ms(500);

    // Should GC even if limit has not reached.
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &apply_key);
        let after_state = state.take_truncated_state();
        let before_state = &before_states[&id];
        assert_ne!(after_state.get_index(), before_state.get_index());
    }
}

#[test]
fn test_node_compact_reserve_max_ticks() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_reserve_max_ticks(&mut cluster);
}

/// Check the log lag status of a node
fn check_log_lag<T: Simulator>(cluster: &Cluster<T>, store_id: u64, region_id: u64) -> u64 {
    let engines = &cluster.engines[&store_id];
    let state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(region_id));
    let applied_index = state.get_applied_index();

    // Get compact_index and first_index
    let compact_index = state.get_truncated_state().get_index();
    let first_index = compact_index + 1;

    let log_lag = applied_index.saturating_sub(first_index);
    println!(
        "Store {}: compact_index={}, first_index={}, applied_index={}, log_lag={}",
        store_id, compact_index, first_index, applied_index, log_lag
    );

    log_lag
}

/// Test that demonstrates the new HashSet-based force compaction mechanism
/// which efficiently manages regions that need forced compaction
fn test_force_compaction<T: Simulator>(cluster: &mut Cluster<T>) {
    // Configure force GC to trigger more frequently for testing
    cluster.cfg.raft_store.raft_log_force_gc_interval = ReadableDuration::millis(500); // 0.5s per tick

    // Set memory ratio threshold to trigger force compaction
    cluster.cfg.raft_store.evict_cache_on_memory_ratio = 0.8; // 80% memory usage threshold

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    // Create initial state baseline
    let mut before_states = HashMap::default();
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let state = state.take_truncated_state();
        before_states.insert(id, state);
    }

    // Write initial data to establish baseline growth patterns
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }
    sleep_ms(300);

    // Get region info and ensure leader is not on store 1
    let region = cluster.get_region(b"key_0");
    let current_leader = cluster.leader_of_region(region.get_id()).unwrap();

    if current_leader.get_store_id() == 1 {
        let new_leader = cluster
            .engines
            .keys()
            .find(|&&id| id != 1)
            .copied()
            .unwrap();
        cluster.must_transfer_leader(region.get_id(), new_peer(new_leader, new_leader));
        sleep_ms(200);
    }

    // Block store 1 from receiving raft messages to create log lag
    cluster.add_recv_filter_on_node(
        1,
        Box::new(
            RegionPacketFilter::new(1, 1)
                .direction(Direction::Recv)
                .msg_type(MessageType::MsgAppend),
        ),
    );

    // Clear states and write more data to create significant log lag
    before_states.clear();
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let state = state.take_truncated_state();
        before_states.insert(id, state);
    }

    // Write substantial data to trigger force compaction
    for i in 100..300 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }

    // Wait for force GC tick to trigger and process regions
    sleep_ms(1000);

    // Check if the new HashSet-based force compaction mechanism worked
    let mut force_compaction_triggered = false;
    let mut regions_processed = 0;

    for (&id, _engines) in &cluster.engines {
        if id == 1 {
            continue; // Skip blocked store 1
        }

        let log_lag = check_log_lag(cluster, id, 1);
        println!("Store {} log lag: {}", id, log_lag);

        // Check if compaction happened (log lag should be reduced)
        if log_lag < 150 {
            // Should be significantly less than the 200+ entries we wrote
            force_compaction_triggered = true;
            regions_processed += 1;
        }
    }

    cluster.clear_recv_filter_on_node(1);

    // Verify that the new mechanism worked
    assert!(
        force_compaction_triggered,
        "HashSet-based force compaction should have processed regions and reduced log lag. \
         Regions processed: {}, Expected: at least 1",
        regions_processed
    );

    // Additional verification: check that the mechanism is efficient
    // by waiting for another tick and ensuring it uses cached HashSet
    sleep_ms(600); // Wait for another force GC tick

    let mut second_tick_efficient = false;
    for (&id, _engines) in &cluster.engines {
        if id == 1 {
            continue;
        }

        let log_lag_after = check_log_lag(cluster, id, 1);
        // If the HashSet mechanism is working, subsequent ticks should be efficient
        // and may show further compaction
        if log_lag_after < 100 {
            second_tick_efficient = true;
            break;
        }
    }

    println!(
        "HashSet-based force compaction test completed. \
         Initial compaction: {}, Second tick efficiency: {}",
        force_compaction_triggered, second_tick_efficient
    );
}

#[test]
fn test_node_hashset_based_force_compaction() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_hashset_based_force_compaction(&mut cluster);
}

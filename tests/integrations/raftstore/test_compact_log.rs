// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use collections::HashMap;
use kvproto::raft_serverpb::RaftApplyState;
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
    log_lag
}

fn test_raft_log_force_gc<T: Simulator>(cluster: &mut Cluster<T>) {
    // Configure force GC settings
    cluster.cfg.raft_store.evict_cache_on_memory_ratio = 1.0;
    cluster.cfg.raft_store.pin_compact_region_ratio = 0.2; // Pin 20% of regions
    cluster.cfg.raft_store.region_sampling_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(150);
    cluster.cfg.raft_store.raft_log_force_gc_tick_interval = ReadableDuration::millis(200);
    cluster.cfg.raft_store.snap_mgr_gc_tick_interval = ReadableDuration::millis(50);

    cluster.run();

    // Create initial data
    cluster.must_put(b"k1", b"v1");

    // Create multiple regions by splitting
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    // Split region to create multiple regions for testing force GC classification
    // Split in correct order: first split at "k1", then at "k2", then at "k3"
    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k1");

    // Get the new region after first split and split it at "k2"
    let region = cluster.get_region(b"k2");
    cluster.must_split(&region, b"k2");

    // Get the new region after second split and split it at "k3"
    let region = cluster.get_region(b"k3");
    cluster.must_split(&region, b"k3");

    sleep_ms(200);

    // Get all regions from store 2's store_meta (a healthy store)
    let store_meta = cluster.store_metas.get(&2).unwrap();
    let regions: Vec<_> = store_meta
        .lock()
        .unwrap()
        .regions
        .values()
        .cloned()
        .collect();

    // Generate initial writes to populate region_write_rate map
    // Put data after splitting so it naturally distributes across regions
    for i in 0..50 {
        let k = format!("k{}", i).into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    sleep_ms(100);

    // Isolate store 1 from the rest - this will cause logs to accumulate
    cluster.add_send_filter(IsolationFilterFactory::new(1));
    sleep_ms(100);

    // Continue writing data while network is partitioned
    // This will cause logs to accumulate on the isolated store
    for i in 50..70 {
        let k = format!("a{}", i).into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    for i in 70..90 {
        let k = format!("k1a{}", i).into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    for i in 90..110 {
        let k = format!("k2a{}", i).into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    for i in 110..130 {
        let k = format!("k3a{}", i).into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
    }

    sleep_ms(400);

    // Check log lag before force GC on a healthy store (store 2)
    let mut total_log_lag = 0;
    for region in &regions {
        let log_lag = check_log_lag(cluster, 2, region.get_id());
        total_log_lag += log_lag;
    }
    assert!(
        total_log_lag >= 80,
        "Total log lag before force GC should be > 60, got {}",
        total_log_lag
    );

    // Enable force GC failpoint to ensure it triggers
    fail::cfg("needs_force_compact", "return(1.0)").unwrap();
    sleep_ms(400);

    // Check log lag after force GC on the same healthy store
    let mut total_log_lag_after = 0;
    for region in &regions {
        let log_lag = check_log_lag(cluster, 2, region.get_id());
        total_log_lag_after += log_lag;
    }
    assert!(
        total_log_lag_after <= 50,
        "Total log lag after force GC should be < 50, got {}",
        total_log_lag_after
    );

    // Clear network partition
    cluster.clear_send_filters();

    sleep_ms(500);

    // Check if store 1 can catch up after network recovery
    let mut store1_caught_up = true; // Start with true, will be set to false if any region hasn't caught up

    // Get log lag after network recovery
    for region in &regions {
        let log_lag_after_recovery = check_log_lag(cluster, 1, region.get_id());
        if log_lag_after_recovery >= 5 {
            store1_caught_up = false;
        }
    }

    assert!(
        store1_caught_up,
        "Store 1 should have caught up with accumulated logs after network recovery"
    );
}

#[test]
fn test_node_raft_log_force_gc() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_raft_log_force_gc(&mut cluster);
}

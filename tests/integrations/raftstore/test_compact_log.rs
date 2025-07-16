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

/// Test that demonstrates forced compaction bypassing slow nodes after
/// consecutive high log lag ticks
fn test_high_log_lag_forced_compaction<T: Simulator>(cluster: &mut Cluster<T>) {
    // Configuration: let log count be between threshold and count_limit, only
    // trigger via high_log_lag_ticks
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(50); // Force compaction threshold 50
    cluster.cfg.raft_store.raft_log_gc_threshold = 30; // Normal GC threshold 30
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(200); // 0.2s per tick
    cluster.cfg.raft_store.raft_log_reserve_max_ticks = 6; // Reduced to trigger faster
    cluster.cfg.raft_store.raft_log_force_gc_ticks = 20;

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    // Get initial log state
    let mut before_states = HashMap::default();
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let state = state.take_truncated_state();
        before_states.insert(id, state);
    }

    // Phase 1: Normal compaction
    println!("Phase 1: Writing data and verifying normal compaction");
    for i in 0..30 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }
    sleep_ms(500);

    // Phase 2: Block store 1 from receiving raft messages
    println!("Phase 2: Blocking store 1 from receiving raft messages");
    let region = cluster.get_region(b"key_0");
    let current_leader = cluster.leader_of_region(region.get_id()).unwrap();

    // Ensure store 1 is not the leader
    if current_leader.get_store_id() == 1 {
        let new_leader = cluster
            .engines
            .keys()
            .find(|&&id| id != 1)
            .copied()
            .unwrap();
        cluster.must_transfer_leader(region.get_id(), new_peer(new_leader, new_leader));
        println!(
            "Transferred leadership from store 1 to store {}",
            new_leader
        );
        sleep_ms(200);
    }

    // Block store 1 from receiving raft messages - this prevents p.matched from
    // updating
    cluster.add_recv_filter_on_node(
        1,
        Box::new(
            RegionPacketFilter::new(1, 1)
                .direction(Direction::Recv)
                .msg_type(MessageType::MsgAppend),
        ),
    );
    println!("Phase 2: Store 1 blocked from receiving raft messages");

    // Record state before writing more data
    before_states.clear();
    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let state = state.take_truncated_state();
        before_states.insert(id, state);
    }

    // Wait for system to stabilize
    sleep_ms(300);

    // Write more data to create log lag
    println!("Writing more entries to create log lag...");
    for i in 30..65 {
        // Write 35 more entries to ensure log_lag > 30
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }

    // Wait for healthy nodes to process but store 1 to fall behind
    sleep_ms(500);

    // Check log lag after writing
    println!("Checking log lag after creating backlog:");
    for &store_id in cluster.engines.keys() {
        let log_lag = check_log_lag(cluster, store_id, 1);
        println!(
            "Store {}: log_lag={}, threshold={}, count_limit={}",
            store_id, log_lag, 30, 50
        );
    }

    // Only check healthy nodes (non-store 1)
    let healthy_engines: HashMap<u64, _> = cluster
        .engines
        .iter()
        .filter(|(&id, _)| id != 1)
        .map(|(&id, e)| (id, e.clone()))
        .collect();
    let healthy_before: HashMap<u64, _> = before_states
        .iter()
        .filter(|(&id, _)| id != 1)
        .map(|(&id, s)| (id, s.clone()))
        .collect();

    // Check that normal compaction is blocked initially
    let compaction_blocked = !check_compacted(&healthy_engines, &healthy_before, 10, false);
    println!(
        "Phase 2a: Normal compaction blocked: {}",
        compaction_blocked
    );

    // Phase 3: Wait for high log lag forced compaction
    println!("Phase 3: Waiting for high log lag forced compaction...");

    // Update baseline for forced compaction test
    let mut before_states_phase3 = HashMap::default();
    for (&id, engines) in &cluster.engines {
        if id == 1 {
            continue;
        } // Skip store 1
        let mut state: RaftApplyState = get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let state = state.take_truncated_state();
        before_states_phase3.insert(id, state);
    }

    // Wait for high_log_lag_ticks mechanism (3 ticks * 0.2s = 0.6s minimum)
    sleep_ms(3500);

    // Check log lag after waiting
    println!("Checking log lag after waiting for forced compaction:");
    for &store_id in cluster.engines.keys() {
        if store_id != 1 {
            let log_lag = check_log_lag(cluster, store_id, 1);
            println!(
                "Store {}: log_lag={}, threshold={}, count_limit={}",
                store_id, log_lag, 30, 50
            );
        }
    }

    // Check if forced compaction happened by comparing compact_index
    let mut forced_compaction_worked = false;
    for (&id, engines) in &cluster.engines {
        if id == 1 {
            continue;
        } // Skip store 1
        let current_state: RaftApplyState =
            get_raft_msg_or_default(engines, &keys::apply_state_key(1));
        let current_compact_index = current_state.get_truncated_state().get_index();
        let before_compact_index = before_states_phase3[&id].get_index();

        println!(
            "Store {}: before_compact_index={}, current_compact_index={}",
            id, before_compact_index, current_compact_index
        );

        if current_compact_index > before_compact_index {
            forced_compaction_worked = true;
            println!(
                "Store {}: Forced compaction detected! compact_index advanced from {} to {}",
                id, before_compact_index, current_compact_index
            );
        }
    }

    // Phase 4: Cleanup
    println!("Phase 4: Removing blockages and verifying recovery");
    cluster.clear_recv_filter_on_node(1);

    sleep_ms(300);

    // Final assertion
    assert!(
        forced_compaction_worked,
        "High log lag forced compaction should have bypassed slow node after sufficient ticks"
    );

    if forced_compaction_worked {
        println!("SUCCESS: High log lag forced compaction worked as expected!");
    } else {
        println!("FAILED: High log lag forced compaction did not work");
    }
}

#[test]
fn test_node_high_log_lag_forced_compaction() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_high_log_lag_forced_compaction(&mut cluster);
}

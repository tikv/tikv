// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Mutex};
use std::time::Duration;

use collections::HashMap;
use engine_traits::{Peekable, CF_RAFT};
use kvproto::raft_serverpb::RaftApplyState;
use raftstore::store::*;
use test_raftstore::*;
use tikv_util::config::*;

// Test the case where cache is compacted when one node down to check
// if it goes well after the node is back online.
// It's mainly aimmed to test the async fetch raft log mechanism.
#[test]
fn test_node_cache_compact_with_one_node_down() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);

    cluster.cfg.raft_store.raft_log_gc_count_limit = 100000;
    cluster.cfg.raft_store.raft_log_gc_threshold = 50;
    cluster.cfg.raft_store.raft_log_gc_size_limit = ReadableSize::mb(20);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.raft_log_reserve_max_ticks = 2;
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::millis(100);
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::default();

    for (&id, engines) in &cluster.engines {
        must_get_equal(engines.kv.as_inner(), b"k1", b"v1");
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

    for i in 1..60 {
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
    for i in 1..60 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        must_get_equal(cluster.engines[&1].kv.as_inner(), &k, &v);
    }

    for i in 60..200 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        let v2 = cluster.get(&k);
        assert_eq!(v2, Some(v));

        if i > 100 && check_compacted(&cluster.engines, &before_states, 1) {
            return;
        }
    }
    panic!("cluster is not compacted after inserting 200 entries.");
}

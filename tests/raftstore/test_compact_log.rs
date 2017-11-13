// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use tikv::raftstore::store::*;
use tikv::storage::CF_RAFT;
use rocksdb::DB;
use protobuf;
use kvproto::raft_serverpb::{RaftApplyState, RaftTruncatedState};

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;

fn get_msg_cf_or_default<M>(engine: &DB, cf: &str, key: &[u8]) -> M
where
    M: protobuf::Message + protobuf::MessageStatic,
{
    engine.get_msg_cf(cf, key).unwrap().unwrap_or_default()
}

fn test_compact_log<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let mut before_states = HashMap::new();

    for (&id, engines) in &cluster.engines {
        let mut state: RaftApplyState =
            get_msg_cf_or_default(&engines.kv_engine, CF_RAFT, &keys::apply_state_key(1));
        before_states.insert(id, state.take_truncated_state());
    }

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);

        if i > 100 && check_compacted(&cluster.engines, &before_states, 1) {
            return;
        }
    }

    panic!("after inserting 1000 entries, compaction is still not finished.");
}

fn check_compacted(
    all_engines: &HashMap<u64, Engines>,
    before_states: &HashMap<u64, RaftTruncatedState>,
    compact_count: u64,
) -> bool {
    // Every peer must have compacted logs, so the truncate log state index/term must > than before.
    let mut compacted_idx = HashMap::new();

    for (&id, engines) in all_engines {
        let mut state: RaftApplyState =
            get_msg_cf_or_default(&engines.kv_engine, CF_RAFT, &keys::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = &before_states[&id];
        let idx = after_state.get_index();
        let term = after_state.get_term();
        if idx == before_state.get_index() || term == before_state.get_term() {
            return false;
        }
        if idx - before_state.get_index() < compact_count {
            return false;
        }
        assert!(term > before_state.get_term());
        compacted_idx.insert(id, idx);
    }

    // wait for actual deletion.
    sleep_ms(100);

    for (id, engines) in all_engines {
        for idx in 0..compacted_idx[id] {
            assert!(engines.raft_engine.get_entry(1, idx).unwrap().is_none());
        }
    }
    true
}

fn test_compact_many_times<T: Simulator>(cluster: &mut Cluster<T>) {
    let gc_limit: u64 = 100;
    cluster.cfg.raft_store.raft_log_gc_threshold = 10;
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::new();

    for (&id, engines) in &cluster.engines {
        must_get_equal(&engines.kv_engine, b"k1", b"v1");
        let mut state: RaftApplyState =
            get_msg_cf_or_default(&engines.kv_engine, CF_RAFT, &keys::apply_state_key(1));
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

        if i >= 200 && check_compacted(&cluster.engines, &before_states, gc_limit * 2) {
            return;
        }
    }

    panic!("compact is expected to be executed multiple times");
}

#[test]
fn test_node_compact_log() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_log(&mut cluster);
}

#[test]
fn test_node_compact_many_times() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_many_times(&mut cluster);
}

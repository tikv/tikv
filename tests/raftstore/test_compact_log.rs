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
use kvproto::raft_serverpb::RaftTruncatedState;

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;

fn test_compact_log<T: Simulator>(cluster: &mut Cluster<T>) {
    // init_log();

    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    cluster.bootstrap_region().expect("");
    cluster.start();

    let mut before_states = HashMap::new();

    for (&id, engine) in &cluster.engines {
        let state: RaftTruncatedState = engine.get_msg(&keys::raft_truncated_state_key(1))
                                              .unwrap()
                                              .unwrap_or_default();
        before_states.insert(id, state);
    }

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let put_key = k.as_bytes();
        let put_value = v.as_bytes();
        cluster.put(put_key, put_value);
        let v = cluster.get(put_key);
        assert_eq!(v, Some(put_value.to_vec()));
    }

    // wait log gc.
    sleep_ms(500);

    // Every peer must have compacted logs, so the truncate log state index/term must > than before.
    for (&id, engine) in &cluster.engines {
        let after_state: RaftTruncatedState = engine.get_msg(&keys::raft_truncated_state_key(1))
                                                    .unwrap()
                                                    .unwrap_or_default();

        let before_state = before_states.get(&id).unwrap();
        assert!(after_state.get_index() > before_state.get_index());
        assert!(after_state.get_term() > before_state.get_term());
    }
}

#[test]
fn test_node_compact_log() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_log(&mut cluster);
}

#[test]
fn test_server_compact_log() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_compact_log(&mut cluster);
}

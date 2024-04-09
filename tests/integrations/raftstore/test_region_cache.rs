// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use test_raftstore::new_node_cluster_with_hybrid_engine;

#[test]
fn test_basic_read() {
    let _cluster = new_node_cluster_with_hybrid_engine(1, 3);
    // todo(SpadeA): add test logic
}

#[test]
fn test_read_index() {
    let _cluster = new_node_cluster_with_hybrid_engine(1, 3);
    // todo(SpadeA): add test logic
}

// todo(SpadeA): more tests when other relevant modules are ready.

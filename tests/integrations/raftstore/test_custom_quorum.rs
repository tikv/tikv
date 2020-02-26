// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use raftstore::store::QuorumAlgorithm;
use test_raftstore::*;

// Test QuorumAlgorithm::IntegrationOnHalfFail works as expected.
#[test]
fn test_integration_on_half_fail_quorum_fn() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.cfg.raft_store.quorum_algorithm = QuorumAlgorithm::IntegrationOnHalfFail;
    cluster.run();
    cluster.must_put(b"k1", b"v0");

    // After peer 4 and 5 fail, no new leader could be elected.
    cluster.stop_node(4);
    cluster.stop_node(5);
    for _ in 0..500 {
        sleep_ms(10);
        if cluster.leader_of_region(1).is_none() {
            return;
        }
    }
    panic!("region 1 must lost leader because quorum fail");
}

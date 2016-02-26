use std::collections::HashMap;

use tikv::raftserver::store::*;
use tikv::proto::raft_serverpb::RaftTruncatedState;

use super::util::*;
use super::store::new_store_cluster;

#[test]
fn test_compact_log() {
    // init_env_log();

    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    let count = 5;
    let mut cluster = new_store_cluster(0, count);
    cluster.bootstrap_single_region().expect("");
    cluster.run_all_nodes();

    sleep_ms(400);

    let mut before_states = HashMap::new();

    for (&id, engine) in &cluster.engines {
        let state: RaftTruncatedState = engine.get_msg(&keys::raft_truncated_state_key(1))
                                              .unwrap()
                                              .unwrap_or_default();
        before_states.insert(id, state);
    }

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let putk = k.as_bytes();
        let putv = v.as_bytes();
        cluster.put(putk, putv);
        let v = cluster.get(putk);
        assert_eq!(v, Some(putv.to_vec()));
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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[path = "../mod.rs"]
mod testsuite;
pub use testsuite::*;

use futures::executor::block_on;
use kvproto::kvrpcpb::*;
use pd_client::PdClient;

#[test]
fn test_resolved_ts_basic() {
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);
    let (k, v) = (b"k1", b"v");
    // Prewrite
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.to_vec();
    mutation.value = v.to_vec();
    suite.must_kv_prewrite(region.id, vec![mutation], k.to_vec(), start_ts);
    suite.must_get_rts(region.id, start_ts);

    // Split region
    suite.cluster.must_split(&region, k);
    let r1 = suite.cluster.get_region(&[]);
    let r2 = suite.cluster.get_region(k);
    let current_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    // Resolved ts of region1 should be advanced
    suite.must_get_rts_ge(r1.id, current_ts);
    // Resolved ts of region2 should be equal to start ts
    suite.must_get_rts(r2.id, start_ts);

    // Merge region2 to region1
    suite.cluster.must_try_merge(r2.id, r1.id);
    // Resolved ts of region1 should be equal to start ts
    suite.must_get_rts(r1.id, start_ts);

    // Commit
    let commit_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(r1.id, vec![k.to_vec()], start_ts, commit_ts);
    // Resolved ts of region1 should be advanced
    let current_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_get_rts_ge(r1.id, current_ts);

    suite.stop();
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[path = "../mod.rs"]
mod testsuite;
pub use testsuite::*;

use futures::executor::block_on;
use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use std::time::Duration;
use test_raftstore::sleep_ms;
use txn_types::TimeStamp;
#[test]
fn test_resolved_ts_basic() {
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);

    // Prewrite
    let (k, v) = (b"k1", b"v");
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.to_vec();
    mutation.value = v.to_vec();
    suite.must_kv_prewrite(region.id, vec![mutation], k.to_vec(), start_ts);

    // The `resolved-ts` won't be updated due to there is lock on the region,
    // the `resolved-ts` may not be the `start_ts` of the lock if the `resolved-ts`
    // is updated with a newer ts before the prewrite request come, but still the
    // `resolved-ts` won't be updated
    let rts = suite.region_resolved_ts(region.id).unwrap();

    // Split region
    suite.cluster.must_split(&region, k);
    let r1 = suite.cluster.get_region(&[]);
    let r2 = suite.cluster.get_region(k);
    let current_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    // Resolved ts of region1 should be advanced
    suite.must_get_rts_ge(r1.id, current_ts);
    // Resolved ts of region2 should be equal to rts
    suite.must_get_rts(r2.id, rts);

    // Merge region2 to region1
    suite.cluster.must_try_merge(r2.id, r1.id);
    // Resolved ts of region1 should be equal to rts
    suite.must_get_rts(r1.id, rts);

    // Commit
    let commit_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(r1.id, vec![k.to_vec()], start_ts, commit_ts);
    // Resolved ts of region1 should be advanced
    let current_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_get_rts_ge(r1.id, current_ts);

    suite.stop();
}

#[test]
fn test_dynamic_change_advance_ts_interval() {
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);

    // `reolved-ts` should update with the interval of 10ms
    suite.must_get_rts_ge(
        region.id,
        block_on(suite.cluster.pd_client.get_tso()).unwrap(),
    );

    // change the interval to 10min
    suite.must_change_advance_ts_interval(1, Duration::from_secs(600));
    // sleep to wait for previous update task finish
    sleep_ms(200);

    // `resolved-ts` should not be updated
    for _ in 0..10 {
        if let Some(ts) = suite.region_resolved_ts(region.id) {
            if block_on(suite.cluster.pd_client.get_tso()).unwrap() <= ts {
                panic!("unexpect update");
            }
        }
        sleep_ms(10)
    }

    // change the interval to 10ms
    suite.must_change_advance_ts_interval(1, Duration::from_millis(10));
    // `resolved-ts` should be updated immediately
    suite.must_get_rts_ge(
        region.id,
        block_on(suite.cluster.pd_client.get_tso()).unwrap(),
    );

    suite.stop();
}

#[test]
fn test_report_min_resolved_ts() {
    fail::cfg("mock_tick_interval", "return(0)").unwrap();
    fail::cfg("mock_collect_tick_interval", "return(0)").unwrap();
    fail::cfg("mock_min_resolved_ts_interval", "return(0)").unwrap();
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);
    let ts1 = suite.cluster.pd_client.get_min_resolved_ts();

    // Prewrite
    let (k, v) = (b"k1", b"v");
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.to_vec();
    mutation.value = v.to_vec();
    suite.must_kv_prewrite(region.id, vec![mutation], k.to_vec(), start_ts);

    // Commit
    let commit_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(region.id, vec![k.to_vec()], start_ts, commit_ts);

    sleep_ms(100);
    let ts3 = suite.cluster.pd_client.get_min_resolved_ts();
    let unapplied_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    assert!(ts3 > ts1);
    assert!(TimeStamp::new(ts3) > commit_ts);
    assert!(TimeStamp::new(ts3) < unapplied_ts);
    fail::remove("mock_tick_interval");
    fail::remove("mock_collect_tick_interval");
    fail::remove("mock_min_resolved_ts_interval");
    suite.stop();
}

#[test]
fn test_report_min_resolved_ts_disable() {
    fail::cfg("mock_tick_interval", "return(0)").unwrap();
    fail::cfg("mock_collect_tick_interval", "return(0)").unwrap();
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);
    let ts1 = suite.cluster.pd_client.get_min_resolved_ts();

    // Prewrite
    let (k, v) = (b"k1", b"v");
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.to_vec();
    mutation.value = v.to_vec();
    suite.must_kv_prewrite(region.id, vec![mutation], k.to_vec(), start_ts);

    // Commit
    let commit_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(region.id, vec![k.to_vec()], start_ts, commit_ts);

    sleep_ms(100);

    // no report
    let ts3 = suite.cluster.pd_client.get_min_resolved_ts();
    assert!(ts3 == ts1);
    fail::remove("mock_tick_interval");
    fail::remove("mock_collect_tick_interval");
    suite.stop();
}

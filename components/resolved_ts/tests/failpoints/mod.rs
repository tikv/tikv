// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[path = "../mod.rs"]
mod testsuite;
use futures::executor::block_on;
use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use test_raftstore::{new_peer, sleep_ms};
pub use testsuite::*;
use txn_types::TimeStamp;

#[test]
fn test_check_leader_timeout() {
    let mut suite = TestSuite::new(3);
    let region = suite.cluster.get_region(&[]);

    // Prewrite
    let (k, v) = (b"k1", b"v");
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.to_vec();
    mutation.value = v.to_vec();
    suite.must_kv_prewrite(region.id, vec![mutation], k.to_vec(), start_ts);
    suite
        .cluster
        .must_transfer_leader(region.id, new_peer(1, 1));

    // The `resolved-ts` won't be updated due to there is lock on the region,
    // the `resolved-ts` may not be the `start_ts` of the lock if the `resolved-ts`
    // is updated with a newer ts before the prewrite request come, but still the
    // `resolved-ts` won't be updated
    let rts = suite.region_resolved_ts(region.id).unwrap();

    let store2_fp = "before_check_leader_store_2";
    fail::cfg(store2_fp, "pause").unwrap();
    let store3_fp = "before_check_leader_store_3";
    fail::cfg(store3_fp, "pause").unwrap();

    // Commit
    let commit_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(region.id, vec![k.to_vec()], start_ts, commit_ts);
    sleep_ms(6000);
    // Check rts was not advanced after 5s
    suite.must_get_rts(region.id, rts);
    fail::remove(store2_fp);
    // And can be advanced after store2 recovered.
    suite.must_get_rts_ge(region.id, commit_ts);

    fail::remove(store3_fp);
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

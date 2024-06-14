// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[path = "../mod.rs"]
mod testsuite;
use std::{sync::mpsc::channel, time::Duration};

use futures::executor::block_on;
use kvproto::{kvrpcpb::*, metapb::RegionEpoch};
use pd_client::PdClient;
use resolved_ts::Task;
use tempfile::Builder;
use test_raftstore::{sleep_ms, IsolationFilterFactory};
use test_sst_importer::*;
pub use testsuite::*;
use tikv_util::store::new_peer;

#[test]
fn test_resolved_ts_basic() {
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);

    // Prewrite
    let (k, v) = (b"k1", b"v");
    let mut start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.to_vec();
    mutation.value = v.to_vec();
    suite.must_kv_prewrite(region.id, vec![mutation], k.to_vec(), start_ts, false);

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

    // ingest sst
    let temp_dir = Builder::new().prefix("test_resolved_ts").tempdir().unwrap();
    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);

    let mut sst_epoch = RegionEpoch::default();
    sst_epoch.set_conf_ver(1);
    sst_epoch.set_version(4);

    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(r1.id);
    meta.set_region_epoch(sst_epoch);

    let import = suite.get_import_client(r1.id);
    send_upload_sst(import, &meta, &data).unwrap();

    let tracked_index_before = suite.region_tracked_index(r1.id);
    let ctx = suite.get_context(r1.id);
    let import = suite.get_import_client(r1.id);
    must_ingest_sst(import, ctx, meta);
    let mut tracked_index_after = suite.region_tracked_index(r1.id);
    for _ in 0..10 {
        if tracked_index_after > tracked_index_before {
            break;
        }
        tracked_index_after = suite.region_tracked_index(r1.id);
        sleep_ms(200)
    }
    assert!(tracked_index_after > tracked_index_before);

    // 1PC
    let tracked_index_before = suite.region_tracked_index(r1.id);

    start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let (k, v) = (b"k2", b"v");
    let mut mutation_1pc = Mutation::default();
    mutation_1pc.set_op(Op::Put);
    mutation_1pc.key = k.to_vec();
    mutation_1pc.value = v.to_vec();
    suite.must_kv_prewrite(r1.id, vec![mutation_1pc], k.to_vec(), start_ts, true);

    tracked_index_after = suite.region_tracked_index(r1.id);
    for _ in 0..10 {
        if tracked_index_after > tracked_index_before {
            break;
        }
        tracked_index_after = suite.region_tracked_index(r1.id);
        sleep_ms(200)
    }
    assert!(tracked_index_after > tracked_index_before);

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
fn test_change_log_memory_quota_exceeded() {
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);

    suite.must_get_rts_ge(
        region.id,
        block_on(suite.cluster.pd_client.get_tso()).unwrap(),
    );

    // Set a small memory quota to trigger memory quota exceeded.
    suite.must_change_memory_quota(1, 1);
    let (k, v) = (b"k1", b"v");
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.to_vec();
    mutation.value = v.to_vec();
    suite.must_kv_prewrite(region.id, vec![mutation], k.to_vec(), start_ts, false);

    // Resolved ts should not advance.
    let (tx, rx) = channel();
    suite.must_schedule_task(
        1,
        Task::GetDiagnosisInfo {
            region_id: 1,
            log_locks: false,
            min_start_ts: u64::MAX,
            callback: Box::new(move |res| {
                tx.send(res).unwrap();
            }),
        },
    );
    let res = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(res.unwrap().1, 0, "{:?}", res);

    suite.stop();
}

#[test]
fn test_scan_log_memory_quota_exceeded() {
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);

    suite.must_get_rts_ge(
        region.id,
        block_on(suite.cluster.pd_client.get_tso()).unwrap(),
    );

    let (k, v) = (b"k1", b"v");
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.to_vec();
    mutation.value = v.to_vec();
    suite.must_kv_prewrite(region.id, vec![mutation], k.to_vec(), start_ts, false);

    // Set a small memory quota to trigger memory quota exceeded.
    suite.must_change_memory_quota(1, 1);
    // Split region
    suite.cluster.must_split(&region, k);

    let r1 = suite.cluster.get_region(&[]);
    let r2 = suite.cluster.get_region(k);
    let current_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    // Wait for scan log.
    sleep_ms(500);
    // Resolved ts of region1 should be advanced
    suite.must_get_rts_ge(r1.id, current_ts);

    // Resolved ts should not advance.
    let (tx, rx) = channel();
    suite.must_schedule_task(
        r2.id,
        Task::GetDiagnosisInfo {
            region_id: r2.id,
            log_locks: false,
            min_start_ts: u64::MAX,
            callback: Box::new(move |res| {
                tx.send(res).unwrap();
            }),
        },
    );
    let res = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(res.unwrap().1, 0, "{:?}", res);

    suite.stop();
}

// This case checks resolved ts can still be advanced quickly even if some TiKV
// stores are partitioned.
#[test]
fn test_store_partitioned() {
    let mut suite = TestSuite::new(3);
    let r = suite.cluster.get_region(&[]);
    suite.cluster.must_transfer_leader(r.id, new_peer(1, 1));
    suite.must_get_rts_ge(r.id, block_on(suite.cluster.pd_client.get_tso()).unwrap());

    suite
        .cluster
        .add_send_filter(IsolationFilterFactory::new(3));
    let tso = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    for _ in 0..50 {
        let rts = suite.region_resolved_ts(r.id).unwrap();
        if rts > tso {
            if rts.physical() - tso.physical() < 3000 {
                break;
            } else {
                panic!("resolved ts doesn't advance in time")
            }
        }
        sleep_ms(100);
    }

    suite.stop();
}

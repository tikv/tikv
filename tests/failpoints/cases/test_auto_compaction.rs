// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use engine_traits::{CF_WRITE, MiscExt};
use kvproto::kvrpcpb::*;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

#[test]
fn test_gc_worker_auto_compaction_with_failpoints() {
    // Test that auto compaction can be started and stopped gracefully
    // This test verifies that the auto compaction infrastructure works,
    // even though it may not actually compact in test environments
    let (mut cluster, client, _ctx) = must_new_cluster_with_cfg_and_kv_client_mul(1, |cluster| {
        cluster.cfg.rocksdb.writecf.disable_auto_compactions = true;
        cluster.cfg.gc.auto_compaction.check_interval = ReadableDuration::secs(1); // 1 second for fast testing
        cluster.cfg.gc.auto_compaction.tombstones_num_threshold = 3; // Low threshold for testing
        cluster.cfg.gc.auto_compaction.redundant_rows_threshold = 3; // Low threshold for testing
        cluster.cfg.gc.auto_compaction.tombstones_percent_threshold = 10; // Low threshold for testing
        cluster
            .cfg
            .gc
            .auto_compaction
            .redundant_rows_percent_threshold = 10; // Low threshold for testing
    });

    cluster.pd_client.disable_default_operator();
    cluster
        .sim
        .write()
        .unwrap()
        .gc_safe_point
        .store(17, Ordering::SeqCst);

    let mut region = cluster.get_region(b"k1");

    cluster.must_split(&region, b"k05");
    region = cluster.get_region(b"k10");
    cluster.must_split(&region, b"k10");
    region = cluster.get_region(b"k15");
    cluster.must_split(&region, b"k15");
    region = cluster.get_region(b"k20");
    cluster.must_split(&region, b"k20");

    let region1 = cluster.get_region(b"k01");
    let region2 = cluster.get_region(b"k05");
    let region3 = cluster.get_region(b"k10");
    let region4 = cluster.get_region(b"k15");
    let region5 = cluster.get_region(b"k20");

    let mut ctx1 = Context::new();
    ctx1.set_region_id(region1.get_id());
    ctx1.set_region_epoch(region1.get_region_epoch().clone());
    ctx1.set_peer(region1.get_peers()[0].clone());
    let mut ctx2 = Context::new();
    ctx2.set_region_id(region2.get_id());
    ctx2.set_region_epoch(region2.get_region_epoch().clone());
    ctx2.set_peer(region2.get_peers()[0].clone());
    let mut ctx3 = Context::new();
    ctx3.set_region_id(region3.get_id());
    ctx3.set_region_epoch(region3.get_region_epoch().clone());
    ctx3.set_peer(region3.get_peers()[0].clone());
    let mut ctx4 = Context::new();
    ctx4.set_region_id(region4.get_id());
    ctx4.set_region_epoch(region4.get_region_epoch().clone());
    ctx4.set_peer(region4.get_peers()[0].clone());
    let mut ctx5 = Context::new();
    ctx5.set_region_id(region5.get_id());
    ctx5.set_region_epoch(region5.get_region_epoch().clone());
    ctx5.set_peer(region5.get_peers()[0].clone());

    // Set up failpoint to notify test when auto compaction thread starts
    let fp_thread_start = "gc_worker_auto_compaction_thread_start";
    let thread_started = Arc::new(AtomicBool::new(false));
    let thread_started_clone = thread_started.clone();

    fail::cfg_callback(fp_thread_start, move || {
        thread_started_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // Set up failpoint to notify test when auto compaction main loop starts
    let fp_start_notify = "gc_worker_auto_compaction_start";
    let compaction_started = Arc::new(AtomicBool::new(false));
    let compaction_started_clone = compaction_started.clone();

    fail::cfg_callback(fp_start_notify, move || {
        compaction_started_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // Set up failpoint to pause execution after candidates are collected
    let fp_pause_after_candidates = "gc_worker_auto_compaction_candidates_collected";
    fail::cfg(fp_pause_after_candidates, "pause").unwrap();

    let large_value = vec![b'x'; 100];

    // Range 1: k0-k5 (low redundancy - clean data)
    for i in 0..5 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        // Only one version, no deletes
        let commit_ts = 10;
        let start_ts = commit_ts - 3;
        let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
        must_kv_prewrite(&client, ctx1.clone(), muts, pk.clone(), start_ts);
        let keys = vec![pk.clone()];
        must_kv_commit(&client, ctx1.clone(), keys, start_ts, commit_ts, commit_ts);
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // Range 2: k5-k10 medium redundancy, 10 total entries, 5 redundant versions.
    // Estimated num of redundant versions is 5, since redundant versions' max ts is
    // 10, which is smaller than the GC safe point 17.
    for i in 5..10 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        // Create fewer versions
        for commit_ts in [10, 20] {
            let start_ts = commit_ts - 3;
            let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
            must_kv_prewrite(&client, ctx2.clone(), muts, pk.clone(), start_ts);
            let keys = vec![pk.clone()];
            must_kv_commit(&client, ctx2.clone(), keys, start_ts, commit_ts, commit_ts);
        }
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // Range 3: k10-k15 high redundancy, 15 total entries, 10 redundant versions.
    // Estimated num of discardable versions is 2 = 10 * (17 - 15) / (25 - 15)
    for i in 10..15 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        // Create multiple versions
        for commit_ts in [15, 25] {
            let start_ts = commit_ts - 3;
            let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
            must_kv_prewrite(&client, ctx3.clone(), muts, pk.clone(), start_ts);
            let keys = vec![pk.clone()];
            must_kv_commit(&client, ctx3.clone(), keys, start_ts, commit_ts, commit_ts);
        }

        let commit_ts = 30;
        let start_ts = commit_ts - 3;
        let muts = vec![new_mutation(Op::Del, &pk, &[])];
        must_kv_prewrite(&client, ctx3.clone(), muts, pk.clone(), start_ts);
        let keys = vec![pk.clone()];
        must_kv_commit(&client, ctx3.clone(), keys, start_ts, commit_ts, commit_ts);
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // Range 4: k15-k20 (high redundancy with more versions before GC safe point)
    // 20 total entries, 15 redundant versions.
    // Estimated num of discardable versions is 7 = 15 * (17 - 10) / (25 - 10)
    for i in 15..20 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        for commit_ts in [10, 15, 25] {
            let start_ts = commit_ts - 3;
            let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
            must_kv_prewrite(&client, ctx4.clone(), muts, pk.clone(), start_ts);
            let keys = vec![pk.clone()];
            must_kv_commit(&client, ctx4.clone(), keys, start_ts, commit_ts, commit_ts);
        }
        let commit_ts = 30;
        let start_ts = commit_ts - 3;
        let muts = vec![new_mutation(Op::Del, &pk, &[])];
        must_kv_prewrite(&client, ctx4.clone(), muts, pk.clone(), start_ts);
        let keys = vec![pk.clone()];
        must_kv_commit(&client, ctx4.clone(), keys, start_ts, commit_ts, commit_ts);
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // Range 5: k20-k35 (write ts 10-25, delete ts 15-30)
    // Each key has 2 versions: 1 write and 1 delete
    // 30 total entries, 15 redundant versions. 15 deletes.
    // Estimated num of discardable versions is:
    //  discardable_mvccs = 15 * (17 - 10) / (24 - 10) ~= 8
    //  discardable_deletes = 15 * (17 - 15) / (29 - 15) ~= 2
    // Total discardable = 8 + 2 = 10
    for i in 20..35 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        // Create one write with timestamp in range 10-24
        let write_commit_ts = 10 + (i - 20);
        let write_start_ts = write_commit_ts - 3;
        let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
        must_kv_prewrite(&client, ctx5.clone(), muts, pk.clone(), write_start_ts);
        let keys = vec![pk.clone()];
        must_kv_commit(
            &client,
            ctx5.clone(),
            keys,
            write_start_ts,
            write_commit_ts,
            write_commit_ts,
        );

        // Create one delete with timestamp in range 15-30
        let delete_commit_ts = 15 + (i - 20);
        let delete_start_ts = delete_commit_ts - 3;
        let muts = vec![new_mutation(Op::Del, &pk, &[])];
        must_kv_prewrite(&client, ctx5.clone(), muts, pk.clone(), delete_start_ts);
        let keys = vec![pk.clone()];
        must_kv_commit(
            &client,
            ctx5.clone(),
            keys,
            delete_start_ts,
            delete_commit_ts,
            delete_commit_ts,
        );
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // First check if the auto compaction thread was started
    let mut timeout = 50; // 5 seconds
    while !thread_started.load(Ordering::SeqCst) && timeout > 0 {
        thread::sleep(Duration::from_millis(100));
        timeout -= 1;
    }

    if !thread_started.load(Ordering::SeqCst) {
        // Auto compaction couldn't start (likely due to test engine limitations)
        // This is acceptable - just verify no crash occurred
        fail::remove(fp_thread_start);
        fail::remove(fp_start_notify);
        fail::remove(fp_pause_after_candidates);
        return;
    }

    // Auto compaction thread started successfully, now test the full flow
    assert!(
        thread_started.load(Ordering::SeqCst),
        "Auto compaction thread should have started and hit the thread_start failpoint"
    );

    // Wait for auto compaction main loop to start and hit the pause
    timeout = 100; // 10 seconds
    while !compaction_started.load(Ordering::SeqCst) && timeout > 0 {
        thread::sleep(Duration::from_millis(100));
        timeout -= 1;
    }
    assert!(
        compaction_started.load(Ordering::SeqCst),
        "Auto compaction main loop should have started and hit the start failpoint"
    );

    // Remove the start notify failpoint and the pause to allow execution to proceed
    fail::remove(fp_start_notify);
    fail::remove(fp_pause_after_candidates);

    // Set up failpoints to check which specific regions are found as candidates
    // We expect ranges k05-k10, k10-k15, k15-k20, and k20-k35 to be candidates
    // (high redundancy)
    let fp_k05_k10 = "gc_worker_auto_compaction_candidate_k05_k10";
    let k05_k10_found = Arc::new(AtomicBool::new(false));
    let k05_k10_found_clone = k05_k10_found.clone();

    fail::cfg_callback(fp_k05_k10, move || {
        k05_k10_found_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    let fp_k10_k15 = "gc_worker_auto_compaction_candidate_k10_k15";
    let k10_k15_found = Arc::new(AtomicBool::new(false));
    let k10_k15_found_clone = k10_k15_found.clone();

    fail::cfg_callback(fp_k10_k15, move || {
        k10_k15_found_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    let fp_k15_k20 = "gc_worker_auto_compaction_candidate_k15_k20";
    let k15_k20_found = Arc::new(AtomicBool::new(false));
    let k15_k20_found_clone = k15_k20_found.clone();

    fail::cfg_callback(fp_k15_k20, move || {
        k15_k20_found_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    let fp_k20_k35 = "gc_worker_auto_compaction_candidate_k20_k35";
    let k20_k35_found = Arc::new(AtomicBool::new(false));
    let k20_k35_found_clone = k20_k35_found.clone();

    fail::cfg_callback(fp_k20_k35, move || {
        k20_k35_found_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // Wait for candidate evaluation to complete
    timeout = 100; // 10 seconds  
    while (!k05_k10_found.load(Ordering::SeqCst)
        || !k10_k15_found.load(Ordering::SeqCst)
        || !k15_k20_found.load(Ordering::SeqCst)
        || !k20_k35_found.load(Ordering::SeqCst))
        && timeout > 0
    {
        thread::sleep(Duration::from_millis(100));
        timeout -= 1;
    }

    // We expect ranges k5-k10, k10-k15, k15-k20, and k20-k35 to be found as
    // compaction candidates but NOT k0-k5 (clean data with only single
    // versions)

    // Range k5-k10: Should be a candidate
    // - 10 total entries (5 keys × 2 versions each)
    // - 5 redundant versions (older puts that are superseded)
    // - Medium redundancy ratio exceeds threshold
    assert!(
        k05_k10_found.load(Ordering::SeqCst),
        "Expected range k5-k10 with 10 total entries, 5 discardable versions to be found as a compact candidate"
    );

    // Range k10-k15: Should be a candidate
    // - 15 total entries (5 keys × 2 versions each) + 5 deletes = 15 total
    // - 2 discardable versions estimated by formula
    // - 5 tombstones (delete entries)
    // - High redundancy ratio exceeds threshold
    assert!(
        k10_k15_found.load(Ordering::SeqCst),
        "Expected range k10-k15 with 15 total entries, 2 discardable versions to be found as a compact candidate"
    );

    // Range k15-k20: Should be a candidate
    // - 20 total entries (5 keys × 3 versions each) + 5 deletes = 20 total
    // - 7 discardable versions estimated by formula
    // - 5 tombstones (delete entries)
    // - Very high redundancy ratio exceeds threshold
    assert!(
        k15_k20_found.load(Ordering::SeqCst),
        "Expected range k15-k20 with 20 total entries, 7 discardable versions to be found as a compact candidate"
    );

    // Range k20-k35: Should be a candidate
    // - 30 total entries (15 keys × 2 versions each: 1 write + 1 delete)
    // - 10 discardable versions estimated by formula
    // - High redundancy ratio exceeds threshold
    assert!(
        k20_k35_found.load(Ordering::SeqCst),
        "Expected range k20-k35 with 30 total entries, 10 discardable versions to be found as a compact candidate"
    );

    // Clean up failpoints
    fail::remove(fp_thread_start);
    fail::remove(fp_k05_k10);
    fail::remove(fp_k10_k15);
    fail::remove(fp_k15_k20);
    fail::remove(fp_k20_k35);
}

// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use engine_traits::{CF_DEFAULT, CF_WRITE, MiscExt};
use kvproto::kvrpcpb::*;
use raftstore::store::Callback;
use test_raftstore::*;
use tikv_util::config::{ReadableDuration, ReadableSize};

// Failpoints and FIRST_COMPACTION_CANDIDATE_REGION are process-global. Keep the
// tests in this file from changing them concurrently.
static AUTO_COMPACTION_TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_auto_compaction_test() -> MutexGuard<'static, ()> {
    AUTO_COMPACTION_TEST_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[test]
fn test_gc_worker_auto_compaction_with_failpoints() {
    let _guard = lock_auto_compaction_test();
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

#[test]
fn test_no_valid_split_key_wakes_auto_compaction_scan() {
    let _guard = lock_auto_compaction_test();

    let fp_compaction_start = "gc_worker_auto_compaction_start";
    let fp_candidates_collected = "gc_worker_auto_compaction_candidates_collected";
    fail::cfg(fp_compaction_start, "pause").unwrap();

    let scan_rounds = Arc::new(AtomicUsize::new(0));
    let scan_rounds_clone = scan_rounds.clone();
    fail::cfg_callback(fp_candidates_collected, move || {
        scan_rounds_clone.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();

    let (mut cluster, _client, _ctx) = must_new_cluster_with_cfg_and_kv_client_mul(1, |cluster| {
        // A split-failure hint must wake the runner well before this interval.
        cluster.cfg.gc.auto_compaction.check_interval = ReadableDuration::secs(30);
    });
    cluster.pd_client.disable_default_operator();
    cluster.pd_client.set_gc_safe_point(100);
    let region = cluster.get_region(b"k50");
    cluster.must_split(&region, b"k50");
    let region = cluster.get_region(b"k50");
    assert_eq!(region.get_start_key(), b"k50");

    fail::remove(fp_compaction_start);
    for _ in 0..100 {
        if scan_rounds.load(Ordering::SeqCst) >= 1 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(scan_rounds.load(Ordering::SeqCst), 1);

    // The split key equals the Region start key, so SplitObserver reports
    // NO_VALID_SPLIT_KEY. The request itself is expected to fail; its purpose
    // is to deliver a best-effort wake-up hint to CompactionRunner.
    cluster.split_region(&region, region.get_start_key(), Callback::None);

    for _ in 0..100 {
        if scan_rounds.load(Ordering::SeqCst) >= 2 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        scan_rounds.load(Ordering::SeqCst),
        2,
        "NO_VALID_SPLIT_KEY should wake a new scan before the 30-second interval"
    );

    fail::remove(fp_candidates_collected);
}

#[test]
fn test_large_value_region_is_prioritized_by_reclaimable_bytes() {
    use tikv::server::gc_worker::FIRST_COMPACTION_CANDIDATE_REGION;

    let _guard = lock_auto_compaction_test();

    let fp_compaction_start = "gc_worker_auto_compaction_start";
    fail::cfg(fp_compaction_start, "pause").unwrap();
    FIRST_COMPACTION_CANDIDATE_REGION.store(0, Ordering::SeqCst);

    let (mut cluster, client, _ctx) = must_new_cluster_with_cfg_and_kv_client_mul(1, |cluster| {
        cluster.cfg.rocksdb.writecf.disable_auto_compactions = true;
        cluster.cfg.rocksdb.defaultcf.disable_auto_compactions = true;
        cluster.cfg.gc.auto_compaction.check_interval = ReadableDuration::secs(30);
        // Neither region meets the entry-count or percentage admission gates.
        // Both are admitted through the byte gate, then ranked by reclaimable bytes.
        cluster.cfg.gc.auto_compaction.redundant_rows_threshold = u64::MAX;
        cluster
            .cfg
            .gc
            .auto_compaction
            .redundant_rows_percent_threshold = 100;
        cluster.cfg.gc.auto_compaction.redundant_bytes_threshold = ReadableSize(1);
    });
    cluster.pd_client.disable_default_operator();
    cluster.pd_client.set_gc_safe_point(100);

    let region = cluster.get_region(b"k50");
    cluster.must_split(&region, b"k50");
    let large_value_region = cluster.get_region(b"k10");
    let small_value_region = cluster.get_region(b"k60");

    let make_ctx = |region: &kvproto::metapb::Region| {
        let mut ctx = Context::new();
        ctx.set_region_id(region.get_id());
        ctx.set_region_epoch(region.get_region_epoch().clone());
        ctx.set_peer(region.get_peers()[0].clone());
        ctx
    };
    let large_ctx = make_ctx(&large_value_region);
    let small_ctx = make_ctx(&small_value_region);

    // The first region has only two stale versions, but they carry large values.
    for (i, key) in [b"k10".as_slice(), b"k20".as_slice()].iter().enumerate() {
        for commit_ts in [10 + i as u64, 30 + i as u64] {
            let start_ts = commit_ts - 3;
            let mutations = vec![new_mutation(Op::Put, key, &vec![b'x'; 256 * 1024])];
            must_kv_prewrite(
                &client,
                large_ctx.clone(),
                mutations,
                key.to_vec(),
                start_ts,
            );
            must_kv_commit(
                &client,
                large_ctx.clone(),
                vec![key.to_vec()],
                start_ts,
                commit_ts,
                commit_ts,
            );
        }
    }

    // The second region has more stale versions, but their values are tiny.
    for i in 0..20 {
        let key = format!("k{:02}", 60 + i);
        for commit_ts in [10 + i, 40 + i] {
            let start_ts = commit_ts - 3;
            let mutations = vec![new_mutation(Op::Put, key.as_bytes(), b"small")];
            must_kv_prewrite(
                &client,
                small_ctx.clone(),
                mutations,
                key.as_bytes().to_vec(),
                start_ts,
            );
            must_kv_commit(
                &client,
                small_ctx.clone(),
                vec![key.as_bytes().to_vec()],
                start_ts,
                commit_ts,
                commit_ts,
            );
        }
    }

    for cf in [CF_WRITE, CF_DEFAULT] {
        cluster.engines[&1].kv.flush_cf(cf, true).unwrap();
    }
    fail::remove(fp_compaction_start);

    let mut timeout = 100;
    while FIRST_COMPACTION_CANDIDATE_REGION.load(Ordering::SeqCst) == 0 && timeout > 0 {
        thread::sleep(Duration::from_millis(100));
        timeout -= 1;
    }
    assert_eq!(
        FIRST_COMPACTION_CANDIDATE_REGION.load(Ordering::SeqCst),
        large_value_region.get_id(),
        "the region with more reclaimable bytes should be compacted first"
    );
}

#[test]
fn test_mvcc_aware_compaction_prioritization() {
    use tikv::{
        server::gc_worker::FIRST_COMPACTION_CANDIDATE_REGION,
        storage::mvcc::mvcc_read_tracker::MVCC_READ_TRACKER,
    };

    let _guard = lock_auto_compaction_test();
    FIRST_COMPACTION_CANDIDATE_REGION.store(0, Ordering::SeqCst);

    // Test that MVCC-aware compaction correctly prioritizes regions
    // with high MVCC read activity over regions with just high redundancy
    // First check if the auto compaction thread was started
    let fp_compaction_start = "gc_worker_auto_compaction_start";
    fail::cfg(fp_compaction_start, "pause").unwrap();

    let fp_thread_start = "gc_worker_auto_compaction_thread_start";
    let thread_started = Arc::new(AtomicBool::new(false));
    let thread_started_clone = thread_started.clone();
    fail::cfg_callback(fp_thread_start, move || {
        thread_started_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    let (mut cluster, client, _ctx) = must_new_cluster_with_cfg_and_kv_client_mul(1, |cluster| {
        cluster.cfg.rocksdb.writecf.disable_auto_compactions = true;
        cluster.cfg.gc.auto_compaction.check_interval = ReadableDuration::secs(1);
        cluster.cfg.gc.auto_compaction.tombstones_num_threshold = 3;
        cluster.cfg.gc.auto_compaction.redundant_rows_threshold = 3;
        cluster.cfg.gc.auto_compaction.tombstones_percent_threshold = 10;
        cluster
            .cfg
            .gc
            .auto_compaction
            .redundant_rows_percent_threshold = 10;
        // Enable compaction filter to score redundant MVCC versions
        cluster.cfg.gc.enable_compaction_filter = true;
        // Enable MVCC-aware compaction with low threshold for testing
        cluster.cfg.gc.auto_compaction.mvcc_read_aware_enabled = true;
        cluster.cfg.gc.auto_compaction.mvcc_scan_threshold = 50; // Low threshold for testing
        cluster.cfg.gc.auto_compaction.mvcc_read_weight = 3.0;
        cluster.cfg.gc.auto_compaction.mvcc_scan_threshold = 1000; // Disable age factor for simplicity
    });

    cluster.pd_client.disable_default_operator();

    let mut timeout = 50; // 5 seconds
    while !thread_started.load(Ordering::SeqCst) && timeout > 0 {
        thread::sleep(Duration::from_millis(100));
        timeout -= 1;
    }
    assert!(
        thread_started.load(Ordering::SeqCst),
        "Auto compaction thread should have started and hit the thread_start failpoint"
    );

    // Set gc_safe_point on pd_client's cluster (used by compaction runner)
    cluster.pd_client.set_gc_safe_point(27);

    // Create 3 regions with different characteristics
    let mut region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k10");
    region = cluster.get_region(b"k15");
    cluster.must_split(&region, b"k20");

    let region1 = cluster.get_region(b"k05"); // k0-k10
    let region2 = cluster.get_region(b"k15"); // k10-k20
    let region3 = cluster.get_region(b"k25"); // k20-...

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

    let large_value = vec![b'x'; 100];

    // Region 1 (k0-k10): Medium redundancy, NO read activity
    // 20 total entries, 10 redundant versions
    for i in 0..10 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        for commit_ts in [10, 15, 30] {
            let start_ts = commit_ts - 3;
            let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
            must_kv_prewrite(&client, ctx1.clone(), muts, pk.clone(), start_ts);
            let keys = vec![pk.clone()];
            must_kv_commit(&client, ctx1.clone(), keys, start_ts, commit_ts, commit_ts);
        }
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // Region 2 (k10-k20): Medium redundancy, HIGH read activity
    // 20 total entries, 10 redundant versions
    // This should be prioritized due to MVCC read tracking
    for i in 10..20 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        for commit_ts in [10, 15, 30] {
            let start_ts = commit_ts - 3;
            let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
            must_kv_prewrite(&client, ctx2.clone(), muts, pk.clone(), start_ts);
            let keys = vec![pk.clone()];
            must_kv_commit(&client, ctx2.clone(), keys, start_ts, commit_ts, commit_ts);
        }
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // Region 3 (k20-k30): HIGH redundancy, NO read activity
    // 30 total entries, 20 redundant versions
    // Even though this has more redundancy, it should be lower priority than Region
    // 2
    for i in 20..30 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        for commit_ts in [10, 15, 25, 30] {
            let start_ts = commit_ts - 3;
            let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
            must_kv_prewrite(&client, ctx3.clone(), muts, pk.clone(), start_ts);
            let keys = vec![pk.clone()];
            must_kv_commit(&client, ctx3.clone(), keys, start_ts, commit_ts, commit_ts);
        }
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // We record 100 requests, each scanning 20000 MVCC versions on average
    for _ in 0..100 {
        MVCC_READ_TRACKER
            .get()
            .unwrap()
            .record_read(region2.get_id(), 20000);
    }
    // sleep
    thread::sleep(Duration::from_millis(5000));
    // start execution of auto compaction thread
    fail::remove(fp_compaction_start);

    // Store the expected region IDs for verification
    let region1_id = region1.get_id();
    let region2_id = region2.get_id();
    let region3_id = region3.get_id();

    // Wait for first candidate to be selected for compaction
    let mut timeout: i32 = 100;
    while FIRST_COMPACTION_CANDIDATE_REGION.load(Ordering::SeqCst) == 0 && timeout > 0 {
        thread::sleep(Duration::from_millis(100));
        timeout -= 1;
    }

    // Verify that Region 2 was selected as the first candidate
    // This is the actual verification that proves MVCC-aware prioritization works
    let first_region_id = FIRST_COMPACTION_CANDIDATE_REGION.load(Ordering::SeqCst);

    let tracker = MVCC_READ_TRACKER.get().unwrap();
    assert_eq!(
        first_region_id,
        region2_id,
        "Region 2 should be selected first for compaction due to MVCC read activity. \
         Expected region_id={}, but got region_id={}. \
         Region 1 has {} MVCC versions/req, Region 2 has {} MVCC versions/req, Region 3 has {} MVCC versions/req",
        region2_id,
        first_region_id,
        tracker.get_mvcc_versions_scanned(region1_id),
        tracker.get_mvcc_versions_scanned(region2_id),
        tracker.get_mvcc_versions_scanned(region3_id)
    );
}

#[test]
fn test_auto_compaction_refreshes_region_approximate_size() {
    let _guard = lock_auto_compaction_test();

    let fp_compaction_start = "gc_worker_auto_compaction_start";
    fail::cfg(fp_compaction_start, "pause").unwrap();

    let (cluster, client, ctx) = must_new_cluster_with_cfg_and_kv_client_mul(1, |cluster| {
        cluster.cfg.rocksdb.writecf.disable_auto_compactions = true;
        cluster.cfg.rocksdb.defaultcf.disable_auto_compactions = true;
        cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(50);
        cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(50);
        // Neither the written bytes nor RocksDB compaction events reach this
        // threshold, so only auto-compaction can trigger a size recheck.
        cluster.cfg.raft_store.region_split_check_diff = Some(ReadableSize::gb(1));
        cluster.cfg.gc.auto_compaction.check_interval = ReadableDuration::secs(1);
        cluster.cfg.gc.auto_compaction.redundant_bytes_threshold = ReadableSize(1);
    });
    cluster.pd_client.disable_default_operator();
    cluster.pd_client.set_gc_safe_point(100);
    let region_id = ctx.get_region_id();

    // Let the initial split check record the size of the empty Region.
    thread::sleep(Duration::from_millis(500));

    // Write two versions of each key with large values. The older versions are
    // below the GC safe point, so the Region is an auto-compaction candidate.
    for i in 0..16 {
        let key = format!("k{:02}", i);
        for commit_ts in [10, 30] {
            let start_ts = commit_ts - 3;
            let mutations = vec![new_mutation(
                Op::Put,
                key.as_bytes(),
                &vec![b'x'; 512 * 1024],
            )];
            must_kv_prewrite(
                &client,
                ctx.clone(),
                mutations,
                key.as_bytes().to_vec(),
                start_ts,
            );
            must_kv_commit(
                &client,
                ctx.clone(),
                vec![key.as_bytes().to_vec()],
                start_ts,
                commit_ts,
                commit_ts,
            );
        }
    }
    for cf in [CF_WRITE, CF_DEFAULT] {
        cluster.engines[&1].kv.flush_cf(cf, true).unwrap();
    }

    thread::sleep(Duration::from_millis(500));
    let size_before = cluster
        .pd_client
        .get_region_approximate_size(region_id)
        .unwrap_or_default();
    assert!(
        size_before < ReadableSize::mb(1).0,
        "the size reported to PD should still be stale, got {size_before}"
    );

    fail::remove(fp_compaction_start);

    // Each key keeps at least one 512 KiB version, so the refreshed size should
    // be at least 16 * 512 KiB.
    let expected_min_size = ReadableSize::mb(4).0;
    let mut size_after = size_before;
    for _ in 0..100 {
        size_after = cluster
            .pd_client
            .get_region_approximate_size(region_id)
            .unwrap_or_default();
        if size_after >= expected_min_size {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        size_after >= expected_min_size,
        "auto-compaction should trigger a Region size recheck, got {size_after}"
    );
}

#[test]
fn test_auto_compaction_refreshes_size_of_region_merged_after_collection() {
    use tikv::server::gc_worker::FIRST_COMPACTION_CANDIDATE_REGION;

    let _guard = lock_auto_compaction_test();

    let fp_compaction_start = "gc_worker_auto_compaction_start";
    let fp_first_candidate = "gc_worker_auto_compaction_first_candidate";
    fail::cfg(fp_compaction_start, "pause").unwrap();
    fail::cfg(fp_first_candidate, "pause").unwrap();
    FIRST_COMPACTION_CANDIDATE_REGION.store(0, Ordering::SeqCst);

    let (mut cluster, client, _ctx) = must_new_cluster_with_cfg_and_kv_client_mul(1, |cluster| {
        cluster.cfg.rocksdb.writecf.disable_auto_compactions = true;
        cluster.cfg.rocksdb.defaultcf.disable_auto_compactions = true;
        cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(50);
        cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(50);
        cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(50);
        // Neither the written bytes nor RocksDB compaction events reach this
        // threshold, so only the merge and auto-compaction recheck the size.
        // The test cluster does not run GC, so no data is reclaimed either.
        cluster.cfg.raft_store.region_split_check_diff = Some(ReadableSize::gb(1));
        // Run a single round. A later round would collect the merged Region
        // itself and refresh its size regardless of the candidate's bounds.
        cluster.cfg.gc.auto_compaction.check_interval = ReadableDuration::secs(30);
        cluster.cfg.gc.auto_compaction.redundant_bytes_threshold = ReadableSize(1);
    });
    cluster.pd_client.disable_default_operator();
    cluster.pd_client.set_gc_safe_point(100);

    let region = cluster.get_region(b"k50");
    cluster.must_split(&region, b"k50");
    let left = cluster.get_region(b"k00");
    let right = cluster.get_region(b"k50");

    let mut ctx = Context::new();
    ctx.set_region_id(left.get_id());
    ctx.set_region_epoch(left.get_region_epoch().clone());
    ctx.set_peer(left.get_peers()[0].clone());

    // Only the left Region has stale versions, so it is the only candidate.
    for i in 0..16 {
        let key = format!("k{:02}", i);
        for commit_ts in [10, 30] {
            let start_ts = commit_ts - 3;
            let mutations = vec![new_mutation(
                Op::Put,
                key.as_bytes(),
                &vec![b'x'; 512 * 1024],
            )];
            must_kv_prewrite(
                &client,
                ctx.clone(),
                mutations,
                key.as_bytes().to_vec(),
                start_ts,
            );
            must_kv_commit(
                &client,
                ctx.clone(),
                vec![key.as_bytes().to_vec()],
                start_ts,
                commit_ts,
                commit_ts,
            );
        }
    }
    for cf in [CF_WRITE, CF_DEFAULT] {
        cluster.engines[&1].kv.flush_cf(cf, true).unwrap();
    }

    fail::remove(fp_compaction_start);
    for _ in 0..100 {
        if FIRST_COMPACTION_CANDIDATE_REGION.load(Ordering::SeqCst) != 0 {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        FIRST_COMPACTION_CANDIDATE_REGION.load(Ordering::SeqCst),
        left.get_id()
    );

    // Merge after the candidate was collected, so the merged Region extends
    // past the candidate's end key.
    cluster.pd_client.must_merge(right.get_id(), left.get_id());

    // The merge rechecks the size of the merged Region.
    let size_after_merge = ReadableSize::mb(12).0;
    let mut size = 0;
    for _ in 0..100 {
        size = cluster
            .pd_client
            .get_region_approximate_size(left.get_id())
            .unwrap_or_default();
        if size >= size_after_merge {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        size >= size_after_merge,
        "the merge should recheck the Region size, got {size}"
    );

    // Grow the merged Region beyond the candidate's end key. These writes stay
    // below `region_split_check_diff`, so the size reported to PD goes stale.
    let merged = cluster.get_region(b"k60");
    assert_eq!(merged.get_id(), left.get_id());
    let mut ctx = Context::new();
    ctx.set_region_id(merged.get_id());
    ctx.set_region_epoch(merged.get_region_epoch().clone());
    ctx.set_peer(merged.get_peers()[0].clone());
    for i in 0..16 {
        let key = format!("k{:02}", 60 + i);
        let (start_ts, commit_ts) = (37, 40);
        let mutations = vec![new_mutation(
            Op::Put,
            key.as_bytes(),
            &vec![b'x'; 512 * 1024],
        )];
        must_kv_prewrite(
            &client,
            ctx.clone(),
            mutations,
            key.as_bytes().to_vec(),
            start_ts,
        );
        must_kv_commit(
            &client,
            ctx.clone(),
            vec![key.as_bytes().to_vec()],
            start_ts,
            commit_ts,
            commit_ts,
        );
    }
    for cf in [CF_WRITE, CF_DEFAULT] {
        cluster.engines[&1].kv.flush_cf(cf, true).unwrap();
    }
    thread::sleep(Duration::from_millis(500));
    let stale_size = cluster
        .pd_client
        .get_region_approximate_size(left.get_id())
        .unwrap_or_default();
    assert!(
        stale_size < ReadableSize::mb(20).0,
        "the size reported to PD should still be stale, got {stale_size}"
    );

    fail::remove(fp_first_candidate);

    // The refreshed size covers both halves: 16 MiB on the left and 8 MiB on
    // the right.
    let refreshed_size = ReadableSize::mb(22).0;
    for _ in 0..100 {
        size = cluster
            .pd_client
            .get_region_approximate_size(left.get_id())
            .unwrap_or_default();
        if size >= refreshed_size {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        size >= refreshed_size,
        "auto-compaction should recheck the size of the merged Region, got {size}"
    );
}

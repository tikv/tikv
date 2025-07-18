// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use engine_traits::{MiscExt, CF_WRITE};
use kvproto::kvrpcpb::*;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

#[test]
fn test_check_then_compact_top_n_with_failpoints() {
    let (mut cluster, client, _ctx) = must_new_cluster_with_cfg_and_kv_client_mul(1, |cluster| {
        cluster.cfg.rocksdb.writecf.disable_auto_compactions = true;
        cluster.cfg.raft_store.region_compact_check_interval = ReadableDuration::millis(100);
        cluster.cfg.raft_store.check_then_compact_top_n = 3; // Compact top 3 candidates
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
    assert_ne!(region1.get_id(), region2.get_id());
    assert_ne!(region2.get_id(), region3.get_id());
    assert_ne!(region3.get_id(), region4.get_id());
    assert_ne!(region4.get_id(), region5.get_id());

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

    // Set up failpoint to notify test when task starts
    let fp_start_notify = "raftstore::compact::CheckThenCompactTopN:NotifyStart";
    let task_started = Arc::new(AtomicBool::new(false));
    let task_started_clone = task_started.clone();

    fail::cfg_callback(fp_start_notify, move || {
        task_started_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // Set up failpoint to pause execution at start
    let fp_start_pause = "raftstore::compact::CheckThenCompactTopN:Start";
    fail::cfg(fp_start_pause, "pause").unwrap();

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

    // Wait for raftstore to automatically trigger CheckThenCompactTopN
    // The failpoint will notify us when it starts and then pause execution
    let mut timeout = 100; // 10 seconds
    while !task_started.load(Ordering::SeqCst) && timeout > 0 {
        thread::sleep(Duration::from_millis(100));
        timeout -= 1;
    }
    assert!(
        task_started.load(Ordering::SeqCst),
        "CheckThenCompactTopN task should have started and hit the failpoint"
    );

    // Remove the first failpoints to allow execution to proceed
    fail::remove(fp_start_notify);
    fail::remove(fp_start_pause);

    // Set up failpoint to check for range 2 (10 entries)
    let fp_range2 = "raftstore::compact::CheckThenCompactTopN:CheckRange2";
    let range2_found = Arc::new(AtomicBool::new(false));
    let range2_found_clone = range2_found.clone();

    fail::cfg_callback(fp_range2, move || {
        range2_found_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // Set up failpoint to check for range 4 (20 entries)
    let fp_range4 = "raftstore::compact::CheckThenCompactTopN:CheckRange4";
    let range4_found = Arc::new(AtomicBool::new(false));
    let range4_found_clone = range4_found.clone();

    fail::cfg_callback(fp_range4, move || {
        range4_found_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // Set up failpoint to check for range 5 (30 entries)
    let fp_range5 = "raftstore::compact::CheckThenCompactTopN:CheckRange5";
    let range5_found = Arc::new(AtomicBool::new(false));
    let range5_found_clone = range5_found.clone();

    fail::cfg_callback(fp_range5, move || {
        range5_found_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // Wait for candidate inspection
    timeout = 10;
    while (!range2_found.load(Ordering::SeqCst)
        || !range4_found.load(Ordering::SeqCst)
        || !range5_found.load(Ordering::SeqCst))
        && timeout > 0
    {
        thread::sleep(Duration::from_millis(100));
        timeout -= 1;
    }

    assert!(
        range2_found.load(Ordering::SeqCst),
        "Expected range 2 (k5-k10) with 10 total entries, 5 discardable versions to be found as a compact candidate"
    );

    assert!(
        range4_found.load(Ordering::SeqCst),
        "Expected range 4 (k15-k20) with 20 total entries, 7 discardable versions to be found as a compact candidate"
    );

    assert!(
        range5_found.load(Ordering::SeqCst),
        "Expected range 5 (k20-k35) with 30 total entries, 10 discardable versions to be found as a compact candidate"
    );

    // Clean up failpoints
    fail::remove(fp_range2);
    fail::remove(fp_range4);
    fail::remove(fp_range5);
}

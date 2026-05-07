// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

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
fn test_check_then_compact_top_n_with_failpoints() {
    let (mut cluster, client, _ctx) = must_new_cluster_with_cfg_and_kv_client_mul(1, |cluster| {
        cluster.cfg.rocksdb.writecf.disable_auto_compactions = true;
        cluster.cfg.raft_store.region_compact_check_interval = ReadableDuration::millis(100);
        cluster.cfg.raft_store.check_then_compact_top_n = 2; // Compact top 2 candidates
    });

    cluster.pd_client.disable_default_operator();

    let mut region = cluster.get_region(b"k1");

    cluster.must_split(&region, b"k05");
    region = cluster.get_region(b"k10");
    cluster.must_split(&region, b"k10");
    let region1 = cluster.get_region(b"k01");
    let region2 = cluster.get_region(b"k05");
    let region3 = cluster.get_region(b"k10");
    assert_ne!(region1.get_id(), region2.get_id());
    assert_ne!(region2.get_id(), region3.get_id());

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
        let version = 10;
        let commit_ts = version + 5;
        let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
        must_kv_prewrite(&client, ctx1.clone(), muts, pk.clone(), version);
        let keys = vec![pk.clone()];
        must_kv_commit(&client, ctx1.clone(), keys, version, commit_ts, commit_ts);
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // Range 2: k5-k10 (medium redundancy)
    for i in 5..10 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        // Create fewer versions
        for version in [10, 20] {
            let commit_ts = version + 5;
            let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
            must_kv_prewrite(&client, ctx2.clone(), muts, pk.clone(), version);
            let keys = vec![pk.clone()];
            must_kv_commit(&client, ctx2.clone(), keys, version, commit_ts, commit_ts);
        }
    }
    cluster.engines[&1].kv.flush_cf(CF_WRITE, true).unwrap();

    // Range 3: k10-k15 (high redundancy)
    for i in 10..15 {
        let key = format!("k{:02}", i);
        let pk = key.as_bytes().to_vec();

        // Create multiple versions
        for version in [10, 20, 30] {
            let commit_ts = version + 5;
            let muts = vec![new_mutation(Op::Put, &pk, &large_value)];
            must_kv_prewrite(&client, ctx3.clone(), muts, pk.clone(), version);
            let keys = vec![pk.clone()];
            must_kv_commit(&client, ctx3.clone(), keys, version, commit_ts, commit_ts);
        }

        // Add some deletes to create tombstones
        let version = 40;
        let commit_ts = version + 5;
        let muts = vec![new_mutation(Op::Del, &pk, &[])];
        must_kv_prewrite(&client, ctx3.clone(), muts, pk.clone(), version);
        let keys = vec![pk.clone()];
        must_kv_commit(&client, ctx3.clone(), keys, version, commit_ts, commit_ts);
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

    // Set up failpoint to check for range 2 (10 versions)
    let fp_range2 = "raftstore::compact::CheckThenCompactTopN:CheckRange2";
    let range2_found = Arc::new(AtomicBool::new(false));
    let range2_found_clone = range2_found.clone();

    fail::cfg_callback(fp_range2, move || {
        range2_found_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // Set up failpoint to check for range 3 (15 versions)
    let fp_range3 = "raftstore::compact::CheckThenCompactTopN:CheckRange3";
    let range3_found = Arc::new(AtomicBool::new(false));
    let range3_found_clone = range3_found.clone();

    fail::cfg_callback(fp_range3, move || {
        range3_found_clone.store(true, Ordering::SeqCst);
    })
    .unwrap();

    // Wait for candidate inspection
    timeout = 10;
    while (!range2_found.load(Ordering::SeqCst) || !range3_found.load(Ordering::SeqCst))
        && timeout > 0
    {
        thread::sleep(Duration::from_millis(100));
        timeout -= 1;
    }

    // Verify that range 2 was found as a candidate (10 versions)
    assert!(
        range2_found.load(Ordering::SeqCst),
        "Expected range 2 (k5-k10) with 10 versions to be found as a compact candidate"
    );

    // Verify that range 3 was found as a candidate (20 versions)
    assert!(
        range3_found.load(Ordering::SeqCst),
        "Expected range 3 (k10-k15) with 20 versions to be found as a compact candidate"
    );

    // Clean up failpoints
    fail::remove(fp_range2);
    fail::remove(fp_range3);
}

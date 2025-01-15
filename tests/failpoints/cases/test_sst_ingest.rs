// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        mpsc::{sync_channel, Receiver, RecvTimeoutError},
        Arc,
    },
    thread,
    thread::JoinHandle,
    time::Duration,
};

use engine_traits::MiscExt;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{Context, Op},
    metapb::{Peer, Region},
    tikvpb::TikvClient,
};
use test_pd_client::TestPdClient;
use test_raftstore::*;
use tikv::server::gc_worker::TestGcRunner;
use tikv_util::HandyRwLock;

// Prepares test data for verifying the behavior of the compaction filter.
//
// This function creates 3 regions and simulates some key-value writes with
// timestamps. Specifically, it splits the keyspace into the following regions:
// - Region 1: Covers the range `[-infinite, zb)`, where key `za` has values
//   written at timestamps 101 and 103.
// - Region 2: Covers the range `[zb, zc)`, where key `zb` has values written at
//   timestamps 101 and 103.
// - Region 3: Covers the range `[zc, +infinite)`.
//
// The function also flushes the data to disk for debugging purposes, allowing
// tools like `tikv-ctl` to inspect the on-disk data. This ensures the data is
// ready for compaction filter tests.
fn prepare_data_used_by_compaction_filter(
    client: &TikvClient,
    ctx: &Context,
    cluster: &mut Cluster<ServerCluster>,
) {
    let large_value = vec![b'x'; 300];

    {
        let keys = vec![b"a".to_vec(), b"b".to_vec()];
        for start_ts in [101, 103] {
            let commit_ts = start_ts + 1;

            for pk in &keys {
                let muts = vec![new_mutation(Op::Put, pk.as_slice(), &large_value)];
                must_kv_prewrite(client, ctx.clone(), muts, pk.clone(), start_ts);
            }
            must_kv_commit(
                client,
                ctx.clone(),
                keys.clone(),
                start_ts,
                commit_ts,
                commit_ts,
            );
        }
    }

    // Region C needs to have data to trigger apply snapshot, but this data will not
    // trigger the compaction filter.
    let pk = b"c";
    let muts = vec![new_mutation(Op::Put, pk.as_slice(), &large_value)];
    must_kv_prewrite(client, ctx.clone(), muts, pk.to_vec(), 101);
    must_kv_commit(client, ctx.clone(), vec![pk.to_vec()], 101, 102, 102);

    let region_a = cluster.get_region(b"a");
    cluster.must_split(&region_a, "b".as_bytes());
    let region_b = cluster.get_region(b"b");
    cluster.must_split(&region_b, "c".as_bytes());
    let _ = cluster.get_region(b"c");
    cluster.check_regions_number(3);

    // Flush data for debugging so that `tikv-ctl` can be used to inspect the data.
    cluster
        .get_engine(1)
        .flush_cfs(&["default", "write"], true)
        .unwrap();
    cluster
        .get_engine(2)
        .flush_cfs(&["default", "write"], true)
        .unwrap();
    cluster
        .get_engine(3)
        .flush_cfs(&["default", "write"], true)
        .unwrap();
}

fn setup_cluster(
    region_to_migrate: &[u8],
) -> (Cluster<ServerCluster>, Region, Peer, Arc<TestPdClient>) {
    let env = Arc::new(Environment::new(1));
    let (mut cluster, leader, ctx) = must_new_cluster_mul(3);
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    prepare_data_used_by_compaction_filter(&client, &ctx, &mut cluster);

    // Get the region dynamically based on the `region_to_migrate` parameter.
    let region = cluster.get_region(region_to_migrate);
    let peer = region.get_peers()[1].clone();

    (cluster, region, peer, pd_client)
}

fn start_apply_snapshot(
    region_id: u64,
    pd_client: Arc<TestPdClient>,
    peer: Peer,
    pause: bool,
) -> JoinHandle<()> {
    thread::spawn(move || {
        fail::cfg("apply_cf_without_ingest_false", "return").unwrap();
        fail::cfg("before_clean_stale_ranges", "return").unwrap();
        fail::cfg("before_clean_overlap_ranges", "return").unwrap();
        if pause {
            // fail::cfg("after_apply_snapshot_ingest_latch_acquired",
            // "pause").unwrap();
        }

        pd_client.must_remove_peer(region_id, peer.clone());
        pd_client.must_add_peer(region_id, peer.clone());
    })
}

fn start_compaction_filter(cluster: &Cluster<ServerCluster>, store_id: u64) -> JoinHandle<()> {
    let gc_engine = cluster.get_engine(store_id);
    thread::spawn(move || {
        let mut gc_runner = TestGcRunner::new(200);
        gc_runner.gc(&gc_engine);
    })
}

fn verify_pending(rx: &Receiver<bool>, duration: u64) {
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(duration)),
        Err(RecvTimeoutError::Timeout)
    );
}

fn verify_completed(rx: &Receiver<bool>) {
    assert_eq!(rx.recv_timeout(Duration::from_millis(1000)), Ok(true));
}

// Tests the behavior of the compaction filter GC when it is blocked by an
// ongoing snapshot ingestion.
//
// Overview:
// This test simulates a scenario where a snapshot ingestion process acquires a
// range latch and blocks the compaction filter GC from proceeding. It
// validates the following behaviors:
// 1. When a snapshot is in progress and holding the range latch, the compaction
//    filter GC remains pending and cannot proceed to the next phase.
// 2. Once the snapshot process releases the range latch, the compaction filter
//    GC can acquire the latch and complete its operation.
//
// Steps:
// - Prepare 3 regions with test data and ensure the environment is set up.
// - Simulate a snapshot ingestion process that acquires the range latch and
//   pauses.
// - Start the compaction filter GC and verify that it cannot acquire the latch
//   while the snapshot process holds it.
// - Resume the snapshot ingestion process and verify that the compaction filter
//   GC can acquire the latch and complete its operation.
//
// The test runs the above validation twice:
// - First, for region "b", where the compaction filter GC and the apply
//   snapshot process overlap. This ensures that the compaction filter GC
//   remains blocked while the range latch is held by the snapshot process.
// - Second, for region "c", where the compaction filter GC and the apply
//   snapshot process do not overlap. This verifies that the compaction filter
//   GC can finish without being blocked.
fn compaction_filter_gc_blocked_by_ingest_test(region_to_migrate: &[u8]) {
    let (cluster, region, peer, pd_client) = setup_cluster(region_to_migrate);
    let region_id = region.id;

    // Start and pause the apply snapshot process.
    fail::cfg("after_apply_snapshot_ingest_latch_acquired", "pause").unwrap();
    let apply_snap_handle = start_apply_snapshot(region_id, pd_client, peer.clone(), true);

    // Wait for snapshot to acquire the latch.
    sleep_ms(1000);

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("compaction_filter_ingest_latch_acquired_flush", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    // Start GC, which might be blocked by the apply snapshot thread, depending
    // on whether the ranges overlap.
    let gc_handle = start_compaction_filter(&cluster, peer.store_id);

    if region_to_migrate != b"c" {
        // Other regions overlap, so the GC thread will be blocked.
        verify_pending(&rx, 1000);
    } else {
        // Region "c" does not overlap with the snapshot process.
        verify_completed(&rx);
        fail::remove("after_apply_snapshot_ingest_latch_acquired");
        gc_handle.join().expect("GC thread panicked");
        apply_snap_handle
            .join()
            .expect("apply snapshot thread panicked");
        fail::remove("compaction_filter_ingest_latch_acquired_flush");
        return;
    }

    // Resume the snapshot process.
    fail::remove("after_apply_snapshot_ingest_latch_acquired");
    // Wait for the snapshot process to release the latch.
    sleep_ms(1000);
    // The GC thread can continue after the snapshot process releases the latch.
    verify_completed(&rx);
    gc_handle.join().expect("GC thread panicked");
    apply_snap_handle
        .join()
        .expect("apply snapshot thread panicked");
    fail::remove("compaction_filter_ingest_latch_acquired_flush");
}

#[test]
fn test_compaction_filter_gc_blocked_by_ingest_basic() {
    compaction_filter_gc_blocked_by_ingest_test(b"a");
}

// Test that the `end_key` used by the compaction filter matches the
// `Region.start_key`.
#[test]
fn test_compaction_filter_gc_blocked_by_ingest_range_boundaries() {
    compaction_filter_gc_blocked_by_ingest_test(b"b");
}

#[test]
fn test_compaction_filter_gc_blocked_by_ingest_no_overlap() {
    compaction_filter_gc_blocked_by_ingest_test(b"c");
}

// Similar as test_compaction_filter_gc_blocked_by_ingest.
fn compaction_filter_gc_blocks_ingest_test(region_to_migrate: &[u8]) {
    let (cluster, region, peer, pd_client) = setup_cluster(region_to_migrate);

    // Start and pause the GC thread.
    fail::cfg("compaction_filter_ingest_latch_acquired_flush", "pause").unwrap();
    let gc_handle = start_compaction_filter(&cluster, peer.store_id);

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("after_apply_snapshot_ingest_latch_acquired", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    // Start the apply snapshot thread, which might be blocked by the GC thread,
    // depending on whether the ranges overlap.
    let region_id = region.id;
    let apply_snap_handle = start_apply_snapshot(region_id, pd_client, peer.clone(), false);
    // Wait for snapshot to acquire the latch.
    sleep_ms(1000);
    if region_to_migrate != b"c" {
        // Other regions overlap, so the snapshot process will be blocked.
        verify_pending(&rx, 1000);
    } else {
        // Region "c" does not overlap with the GC thread.
        verify_completed(&rx);
        fail::remove("compaction_filter_ingest_latch_acquired_flush");
        gc_handle.join().expect("GC thread panicked");
        apply_snap_handle
            .join()
            .expect("apply snapshot thread panicked");
        fail::remove("after_apply_snapshot_ingest_latch_acquired");
        return;
    }

    // Resume the compaction filter process.
    fail::remove("compaction_filter_ingest_latch_acquired_flush");
    // Wait for the GC thread to release the latch.
    sleep_ms(1000);
    // The apply snapshot thread can continue after the GC thread releases the
    // latch.
    verify_completed(&rx);
    gc_handle.join().expect("GC thread panicked");
    apply_snap_handle
        .join()
        .expect("apply snapshot thread panicked");
    fail::remove("after_apply_snapshot_ingest_latch_acquired");
}

#[test]
fn test_compaction_filter_gc_blocks_ingest() {
    compaction_filter_gc_blocks_ingest_test(b"a");
}

// Test that the `end_key` used by the compaction filter matches the
// `Region.start_key`.
#[test]
fn test_compaction_filter_gc_blocks_ingest_range_boundaries() {
    compaction_filter_gc_blocks_ingest_test(b"b");
}

#[test]
fn test_compaction_filter_gc_blocks_ingest_test_no_overlap() {
    compaction_filter_gc_blocks_ingest_test(b"c");
}

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
// - Region A: Covers the range `[-infinite, zb)`, where key `za` has values
//   written at timestamps 101 and 103.
// - Region B: Covers the range `[zb, zc)`, where key `zb` has values written at
//   timestamps 101 and 103.
// - Region C: Covers the range `[zc, +infinite)`.
//
// The function also flushes the data to disk for debugging purposes, allowing
// tools like `tikv-ctl` to inspect the on-disk data.
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

    // Region c needs to contain data to trigger the apply-snapshot process.
    // However, this data will not trigger the compaction filter, resulting in
    // only one version being created.
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

fn start_region_migrate(
    region_id: u64,
    pd_client: Arc<TestPdClient>,
    peer: Peer,
) -> JoinHandle<()> {
    thread::spawn(move || {
        // Force SST ingestion instead of using RocksDB writes.
        fail::cfg("apply_cf_without_ingest_false", "return").unwrap();
        // Disabled for scenarios that are not relevant.
        fail::cfg("before_clean_stale_ranges", "return").unwrap();

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
    assert_eq!(rx.recv_timeout(Duration::from_millis(500)), Ok(true));
}

// Test whether ingestion blocks the compaction filter based on
// whether they have overlapping keys.
//
// Steps:
// - Prepare 3 regions with test data and ensure the environment is set up.
// - Simulate a apply-snapshot process that acquires the range latch and pauses.
// - Start the compaction filter GC and verify that it cannot acquire the latch
//   while the apply-snapshot process holds it.
// - Resume the apply-snapshot ingestion process and verify that the compaction
//   filter GC can acquire the latch and complete its operation.
fn blocked_by_ingest_test(region_to_migrate: &[u8], ingest_type: &str) {
    let (cluster, region, peer, pd_client) = setup_cluster(region_to_migrate);
    let region_id = region.id;

    // If testing `clean_overlap_ingest`, skip `apply_snapshot`; otherwise, skip
    // `clean_overlap_ingest`.
    if ingest_type == "clean_overlap_ingest" {
        fail::cfg("apply_snap_cleanup_range", "return").unwrap();
        fail::cfg("manually_set_max_delete_count_by_key", "return").unwrap();
    } else {
        fail::cfg("before_clean_overlap_ranges", "return").unwrap();
    }

    // Start and pause the apply-snapshot process.
    fail::cfg("after_apply_snapshot_ingest_latch_acquired", "pause").unwrap();
    let apply_snap_handle = start_region_migrate(region_id, pd_client, peer.clone());

    // Wait for snapshot to acquire the latch.
    sleep_ms(500);

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("compaction_filter_ingest_latch_acquired_flush", move || {
        // Ignore the return value because the ingest process triggers twice. At the
        // second trigger, the channel might already be dropped, causing send to fail.
        // However, this does not affect the test functionality.
        let _ = tx.send(true);
    })
    .unwrap();

    // Start GC, which might be blocked by the apply-snapshot thread, depending
    // on whether the ranges overlap.
    let gc_handle = start_compaction_filter(&cluster, peer.store_id);

    if region_to_migrate != b"c" {
        // Other regions overlap, so the GC thread will be blocked.
        verify_pending(&rx, 500);
    } else {
        // Region c does not overlap with the snapshot process.
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
    sleep_ms(500);
    // The GC thread can continue after the snapshot process releases the latch.
    verify_completed(&rx);
    gc_handle.join().expect("GC thread panicked");
    apply_snap_handle
        .join()
        .expect("apply snapshot thread panicked");
    fail::remove("compaction_filter_ingest_latch_acquired_flush");
}

// Test Region A: Verify that the compaction filter is blocked because it
// overlaps with the region's data.
#[test]
fn test_compaction_filter_blocked_by_snapshot_ingest_basic() {
    blocked_by_ingest_test(b"a", "snapshot_ingest");
}

// Test Region B: Verify that the compaction filter is blocked because its
// `end_key` matches the `Region.start_key` and overlaps with this region's
// data.
#[test]
fn test_compaction_filter_blocked_by_snapshot_ingest_boundary() {
    blocked_by_ingest_test(b"b", "snapshot_ingest");
}

// Test Region C: Verify that the compaction filter is not blocked because it
// does not overlap with this region's data.
#[test]
fn test_compaction_filter_not_blocked_by_snapshot_ingest() {
    blocked_by_ingest_test(b"c", "snapshot_ingest");
}

// Similar to `test_compaction_filter_blocked_by_snapshot_ingest_basic`, but
// tests the `clean_overlap_range`.
#[test]
fn test_compaction_filter_blocked_by_clean_overlap_ingest_basic() {
    blocked_by_ingest_test(b"a", "clean_overlap_ingest");
}

// Similar to `test_compaction_filter_blocked_by_snapshot_ingest_boundary`, but
// tests the `clean_overlap_range`.
#[test]
fn test_compaction_filter_blocked_by_clean_overlap_ingest_boundary() {
    blocked_by_ingest_test(b"b", "clean_overlap_ingest");
}

// Similar to `test_compaction_filter_not_blocked_by_snapshot_ingest`, but
// tests the `clean_overlap_range`.
#[test]
fn test_compaction_filter_not_blocked_by_clean_overlap_ingest() {
    blocked_by_ingest_test(b"c", "clean_overlap_ingest");
}

///////////////////////////////////////////////////////////////////////////////
// The following tests is similar to above tests, but tests the reverse scenario
// where compaction filter blocks ingestion.
///////////////////////////////////////////////////////////////////////////////

fn blocks_ingest_test(region_to_migrate: &[u8], ingest_type: &str) {
    let (cluster, region, peer, pd_client) = setup_cluster(region_to_migrate);

    // Start and pause the GC thread.
    fail::cfg("compaction_filter_ingest_latch_acquired_flush", "pause").unwrap();
    let gc_handle = start_compaction_filter(&cluster, peer.store_id);

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("after_apply_snapshot_ingest_latch_acquired", move || {
        // Ignore the return value because the ingest process triggers twice. At the
        // second trigger, the channel might already be dropped, causing send to fail.
        // However, this does not affect the test functionality.
        let _ = tx.send(true);
    })
    .unwrap();

    let region_id = region.id;

    // If testing `clean_overlap_ingest`, skip `apply_snapshot`; otherwise, skip
    // `clean_overlap_ingest`.
    if ingest_type == "clean_overlap_ingest" {
        fail::cfg("apply_snap_cleanup_range", "return").unwrap();
        fail::cfg("manually_set_max_delete_count_by_key", "return").unwrap();
    } else {
        fail::cfg("before_clean_overlap_ranges", "return").unwrap();
    }
    // Start the apply-snapshot thread, which might be blocked by the GC thread,
    // depending on whether the ranges overlap.
    let apply_snap_handle = start_region_migrate(region_id, pd_client, peer.clone());
    // Wait for snapshot to acquire the latch.
    sleep_ms(500);
    if region_to_migrate != b"c" {
        // Other regions overlap, so the snapshot process will be blocked.
        verify_pending(&rx, 500);
    } else {
        // Region c does not overlap with the GC thread.
        verify_completed(&rx);
        fail::remove("compaction_filter_ingest_latch_acquired_flush");
        gc_handle.join().expect("GC thread panicked");
        apply_snap_handle
            .join()
            .expect("apply-snapshot thread panicked");
        fail::remove("after_apply_snapshot_ingest_latch_acquired");
        return;
    }

    // Resume the compaction filter process.
    fail::remove("compaction_filter_ingest_latch_acquired_flush");
    // Wait for the GC thread to release the latch.
    sleep_ms(500);
    // The apply-snapshot thread can continue after the GC thread releases the
    // latch.
    verify_completed(&rx);
    gc_handle.join().expect("GC thread panicked");
    apply_snap_handle
        .join()
        .expect("apply snapshot thread panicked");
    fail::remove("after_apply_snapshot_ingest_latch_acquired");
}

#[test]
fn test_compaction_filter_blocks_snapshot_ingest_basic() {
    blocks_ingest_test(b"a", "snapshot_ingest");
}

#[test]
fn test_compaction_filter_blocks_snapshot_ingest_boundary() {
    blocks_ingest_test(b"b", "snapshot_ingest");
}

#[test]
fn test_compaction_filter_not_blocks_snapshot_ingest() {
    blocks_ingest_test(b"c", "snapshot_ingest");
}

#[test]
fn test_compaction_filter_blocks_clean_overlap_ingest_basic() {
    blocks_ingest_test(b"a", "clean_overlap_ingest");
}

#[test]
fn test_compaction_filter_blocks_clean_overlap_ingest_boundary() {
    blocks_ingest_test(b"b", "clean_overlap_ingest");
}

#[test]
fn test_compaction_filter_not_blocks_clean_overlap_ingest() {
    blocks_ingest_test(b"c", "clean_overlap_ingest");
}

// This test verifies that when a region is migrated out and then migrated back,
// the destroy-peer task must complete before the apply-snapshot task.
//
// Currently, since the region worker operates on a single thread, when a
// region is migrated out and back again, the destroy-peer task is enqueued
// before the apply-snapshot task, ensuring sequential execution without
// concurrency.
//
// As the apply-snapshot ingestion uses RocksDB
// IngestExternalFileOption.allow_write = true, concurrent writes are not
// expected during apply-snapshot ingestion. Therefore, this test ensures that
// there are no concurrent writes from destroy-peer during this process.
#[test]
fn test_apply_snapshot_must_wait_destroy_peer() {
    let (mut cluster, ..) = must_new_cluster_mul(3);

    cluster.must_put(b"k1", b"v1");
    let peer = new_peer(2, 2);
    let pk = b"k1".to_vec();
    let region = cluster.get_region(&pk);
    let region_id = region.get_id();
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    // Start the destroy-peer process, which will pause at
    // before_clean_stale_ranges.
    let pd_client_clone1 = pd_client.clone();
    let peer_clone1 = peer.clone();
    let destroy_handle = thread::spawn(move || {
        fail::cfg("before_clean_stale_ranges", "pause").unwrap();
        pd_client_clone1.must_remove_peer(region_id, peer_clone1);
    });
    // Wait for the destroy-peer process to pause.
    sleep_ms(500);

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("apply_snapshot_finished", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    let pd_client_clone2 = pd_client.clone();
    let peer_clone2 = peer.clone();
    let apply_handle = thread::spawn(move || {
        pd_client_clone2.must_add_peer(region_id, peer_clone2);
    });

    // Verify that apply-snapshot is pending.
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(500)),
        Err(RecvTimeoutError::Timeout)
    );

    // Resume the destroy-peer process.
    fail::remove("before_clean_stale_ranges");
    // Verify that apply-snapshot is finished
    assert_eq!(rx.recv_timeout(Duration::from_millis(1000)), Ok(true));
    destroy_handle.join().expect("destroy handle panicked");
    apply_handle.join().expect("apply handle panicked");
}

// This test ensures that the destroy-peer process will wait for the ongoing
// foreground writes of the same region to finish.
//
// Since test_apply_snapshot_must_wait_destroy_peer() has already verified that
// when a region is migrated out and then back, the apply-snapshot process will
// wait for the destroy-peer process to complete first, it can be considered
// that apply-snapshot will also wait for the ongoing foreground writes to
// finish.
#[test]
fn test_destroy_peer_must_wait_ongoing_foreground_writes() {
    let (mut cluster, ..) = must_new_cluster_mul(3);
    // Use peer_id 3 since on_apply_write_cmd only works on peer 3.
    let peer = new_peer(3, 3);
    let pk = b"k1".to_vec();
    let region = cluster.get_region(&pk);
    let region_id = region.get_id();
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    fail::cfg("on_apply_write_cmd", "pause").unwrap();
    cluster.put(b"k1", b"v1").unwrap();
    // Wait for foreground writes to pause.
    sleep_ms(500);

    let (tx, rx) = sync_channel(0);
    fail::cfg_callback("raft_store_after_destroy_peer", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    // Start the destroy-peer process, which will pause at
    // before_clean_stale_ranges.
    let pd_client_clone1 = pd_client.clone();
    let peer_clone1 = peer.clone();
    let destroy_handle = thread::spawn(move || {
        pd_client_clone1.must_remove_peer(region_id, peer_clone1);
    });

    // Verify that apply-snapshot is pending.
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(500)),
        Err(RecvTimeoutError::Timeout)
    );

    // Resume the foreground writes.
    fail::remove("on_apply_write_cmd");
    // Verify that apply-snapshot is finished
    assert_eq!(rx.recv_timeout(Duration::from_millis(500)), Ok(true));
    destroy_handle.join().expect("destroy thread panicked");
}

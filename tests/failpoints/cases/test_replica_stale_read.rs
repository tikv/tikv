// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::{kvrpcpb::Op, metapb::Peer};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;

fn prepare_for_stale_read(leader: Peer) -> (Cluster<ServerCluster>, Arc<TestPdClient>, PeerClient) {
    prepare_for_stale_read_before_run(leader, None)
}

fn prepare_for_stale_read_before_run(
    leader: Peer,
    before_run: Option<Box<dyn Fn(&mut Cluster<ServerCluster>)>>,
) -> (Cluster<ServerCluster>, Arc<TestPdClient>, PeerClient) {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    if let Some(f) = before_run {
        f(&mut cluster);
    };
    cluster.cfg.resolved_ts.enable = true;
    cluster.run();

    cluster.must_transfer_leader(1, leader.clone());
    let leader_client = PeerClient::new(&cluster, 1, leader);

    // There should be no read index message while handling stale read request
    let on_step_read_index_msg = "on_step_read_index_msg";
    fail::cfg(on_step_read_index_msg, "panic").unwrap();

    (cluster, pd_client, leader_client)
}

// Testing how data replication could effect stale read service
#[test]
fn test_stale_read_basic_flow_replicate() {
    let (mut cluster, pd_client, mut leader_client) = prepare_for_stale_read(new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    // Set the `stale_read` flag
    leader_client.ctx.set_stale_read(true);
    follower_client2.ctx.set_stale_read(true);

    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Can read `value1` with the newest ts
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));

    // Stop replicate data to follower 2
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Update `key1`
    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    // Follower 2 can still read `value1`, but can not read `value2` due
    // to it don't have enough data
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
    let resp1 = follower_client2.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp1.get_region_error().has_data_is_not_ready());

    // Leader have up to date data so it can read `value2`
    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), get_tso(&pd_client));

    // clear the `MsgAppend` filter
    cluster.clear_send_filters();

    // Now we can read `value2` with the newest ts
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), get_tso(&pd_client));
}

// Testing how mvcc locks could effect stale read service
#[test]
fn test_stale_read_basic_flow_lock() {
    let (cluster, pd_client, leader_client) = prepare_for_stale_read(new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);

    // Write `(key1, value1)`
    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Prewrite on `key2` but not commit yet
    let k2_prewrite_ts = get_tso(&pd_client);
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        k2_prewrite_ts,
    );
    // Update `key1`
    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    // Assert `(key1, value2)` can't be readed with `commit_ts2` due to it's larger than the `start_ts` of `key2`.
    let resp = follower_client2.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready());
    // Still can read `(key1, value1)` since `commit_ts1` is less than the `key2` lock's `start_ts`
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);

    // Prewrite on `key3` but not commit yet
    let k3_prewrite_ts = get_tso(&pd_client);
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key3"[..], &b"value1"[..])],
        b"key3".to_vec(),
        k3_prewrite_ts,
    );
    // Commit on `key2`
    let k2_commit_ts = get_tso(&pd_client);
    leader_client.must_kv_commit(vec![b"key2".to_vec()], k2_prewrite_ts, k2_commit_ts);

    // Although there is still lock on the region, but the min lock is refreshed
    // to the `key3`'s lock, now we can read `(key1, value2)` but not `(key2, value1)`
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);
    let resp = follower_client2.kv_read(b"key2".to_vec(), k2_commit_ts);
    assert!(resp.get_region_error().has_data_is_not_ready());

    // Commit on `key3`
    let k3_commit_ts = get_tso(&pd_client);
    leader_client.must_kv_commit(vec![b"key3".to_vec()], k3_prewrite_ts, k3_commit_ts);

    // Now there is not lock on the region, we can read any
    // up to date data
    follower_client2.must_kv_read_equal(b"key2".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
    follower_client2.must_kv_read_equal(b"key3".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that even leader's `apply_index` updated before sync the `(apply_index, safe_ts)`
// item to other replica, the `apply_index` in the `(apply_index, safe_ts)` item should not
// be updated
#[test]
fn test_update_apply_index_before_sync_read_state() {
    let (mut cluster, pd_client, mut leader_client) = prepare_for_stale_read(new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);
    leader_client.ctx.set_stale_read(true);

    // Stop node 3 to ensure data must replicated to follower 2 before write return
    cluster.stop_node(3);

    // Stop sync `(apply_index, safe_ts)` item to the replica
    let before_sync_replica_read_state = "before_sync_replica_read_state";
    fail::cfg(before_sync_replica_read_state, "return()").unwrap();

    // Write `(key1, value1)`
    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );
    // Leave a lock on `key2` so the item's `safe_ts` won't be updated
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value2"[..])],
        b"key2".to_vec(),
        get_tso(&pd_client),
    );
    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);

    cluster.run_node(3).unwrap();
    // Stop replicate data to follower 2
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Write `(key3, value3)` to update the leader `apply_index`
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key3"[..], &b"value3"[..])],
        b"key3".to_vec(),
    );

    // Sync `(apply_index, safe_ts)` item to the replica
    fail::remove(before_sync_replica_read_state);
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
}

// Testing that if `resolved_ts` updated before `apply_index` update, the `safe_ts`
// won't be updated, hence the leader won't broadcast a wrong `(apply_index, safe_ts)`
// item to other replicas
#[test]
fn test_update_resoved_ts_before_apply_index() {
    let (mut cluster, pd_client, mut leader_client) = prepare_for_stale_read(new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    leader_client.ctx.set_stale_read(true);
    follower_client2.ctx.set_stale_read(true);

    // Write `(key1, value1)`
    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);

    // Return before handling `apply_res`, to stop the leader updating the apply index
    let on_apply_res_fp = "on_apply_res";
    fail::cfg(on_apply_res_fp, "return()").unwrap();
    // Stop replicate data to follower 2
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Write `(key1, value2)`
    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    // Wait `resolved_ts` be updated
    sleep_ms(100);

    // The leader can't handle stale read with `commit_ts2` because its `safe_ts`
    // can't update due to its `apply_index` not update
    let resp = leader_client.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready(),);
    // The follower can't handle stale read with `commit_ts2` because it don't
    // have enough data
    let resp = follower_client2.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready());

    fail::remove(on_apply_res_fp);
    cluster.clear_send_filters();

    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);
}

// Testing that the new elected leader should initialize the `resolver` correctly
#[test]
fn test_new_leader_init_resolver() {
    let (mut cluster, pd_client, mut peer_client1) = prepare_for_stale_read(new_peer(1, 1));
    let mut peer_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    peer_client1.ctx.set_stale_read(true);
    peer_client2.ctx.set_stale_read(true);

    // Write `(key1, value1)`
    let commit_ts1 = peer_client1.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // There are no lock in the region, the `safe_ts` should keep updating by the new leader,
    // so we can read `key1` with the newest ts
    cluster.must_transfer_leader(1, new_peer(2, 2));
    peer_client1.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));

    // Prewrite on `key2` but not commit yet
    peer_client2.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        get_tso(&pd_client),
    );

    // There are locks in the region, the `safe_ts` can't be updated, so we can't read
    // `key1` with the newest ts
    cluster.must_transfer_leader(1, new_peer(1, 1));
    let resp = peer_client2.kv_read(b"key1".to_vec(), get_tso(&pd_client));
    assert!(resp.get_region_error().has_data_is_not_ready());
    // But we can read `key1` with `commit_ts1`
    peer_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
}

// Testing that while applying snapshot the follower should reset its `safe_ts` to 0 and
// reject incoming stale read request, then resume the `safe_ts` after applying snapshot
#[test]
fn test_stale_read_while_applying_snapshot() {
    let (mut cluster, pd_client, leader_client) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_snapshot)));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);

    let k1_commit_ts = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), k1_commit_ts);

    // Stop replicate data to follower 2
    cluster.add_send_filter(IsolationFilterFactory::new(2));

    // Prewrite on `key3` but not commit yet
    let k2_prewrite_ts = get_tso(&pd_client);
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        k2_prewrite_ts,
    );

    // Compact logs to force requesting snapshot after clearing send filters.
    let gc_limit = cluster.cfg.raft_store.raft_log_gc_count_limit();
    let state = cluster.truncated_state(1, 1);
    for i in 1..gc_limit * 10 {
        let (k, v) = (
            format!("k{}", i).into_bytes(),
            format!("v{}", i).into_bytes(),
        );
        leader_client.must_kv_write(&pd_client, vec![new_mutation(Op::Put, &k, &v)], k);
    }
    cluster.wait_log_truncated(1, 1, state.get_index() + 5 * gc_limit);

    // Pasuse before applying snapshot is finish
    let raft_before_applying_snap_finished = "raft_before_applying_snap_finished";
    fail::cfg(raft_before_applying_snap_finished, "pause").unwrap();
    cluster.clear_send_filters();

    // Wait follower 2 start applying snapshot
    cluster.wait_log_truncated(1, 2, state.get_index() + 5 * gc_limit);
    sleep_ms(100);

    // We can't read while applying snapshot and the `safe_ts` should reset to 0
    let resp = follower_client2.kv_read(b"key1".to_vec(), k1_commit_ts);
    assert!(resp.get_region_error().has_data_is_not_ready());
    assert_eq!(
        0,
        resp.get_region_error()
            .get_data_is_not_ready()
            .get_safe_ts()
    );

    // Resume applying snapshot
    fail::remove(raft_before_applying_snap_finished);

    // We can read `key1` after applied snapshot
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), k1_commit_ts);
    // There is still lock on the region, we can't read `key1` with the newest ts
    let resp = follower_client2.kv_read(b"key1".to_vec(), get_tso(&pd_client));
    assert!(resp.get_region_error().has_data_is_not_ready());

    // Commit `key2`
    leader_client.must_kv_commit(vec![b"key2".to_vec()], k2_prewrite_ts, get_tso(&pd_client));
    // We can read `key1` with the newest ts now
    follower_client2.must_kv_read_equal(b"key2".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that after region merged the region's `safe_ts` should reset to
// min(`target_safe_ts`, `source_safe_ts`)
#[test]
fn test_stale_read_while_region_merge() {
    let (mut cluster, pd_client, _) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_merge)));

    cluster.must_split(&cluster.get_region(&[]), b"key3");
    let source = pd_client.get_region(b"key1").unwrap();
    let target = pd_client.get_region(b"key5").unwrap();

    cluster.must_transfer_leader(target.get_id(), new_peer(1, 1));
    let target_leader = PeerClient::new(&cluster, target.get_id(), new_peer(1, 1));
    // Write `(key5, value1)`
    target_leader.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key5"[..], &b"value1"[..])],
        b"key5".to_vec(),
    );

    let source_leader = cluster.leader_of_region(source.get_id()).unwrap();
    let source_leader = PeerClient::new(&cluster, source.get_id(), source_leader);
    // Prewrite on `key1` but not commit yet
    let k1_prewrite_ts = get_tso(&pd_client);
    source_leader.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
        k1_prewrite_ts,
    );

    // Write `(key5, value2)`
    let k5_commit_ts = target_leader.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key5"[..], &b"value2"[..])],
        b"key5".to_vec(),
    );

    // Merge source region into target region, the lock on source region should also merge
    // into the target region and cause the target region's `safe_ts` decrease
    pd_client.must_merge(source.get_id(), target.get_id());

    let mut follower_client2 = PeerClient::new(&cluster, target.get_id(), new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);
    // We can read `(key5, value1)` with `k1_prewrite_ts`
    follower_client2.must_kv_read_equal(b"key5".to_vec(), b"value1".to_vec(), k1_prewrite_ts);
    // Can't read `key5` with `k5_commit_ts` because `k1_prewrite_ts` is smaller than `k5_commit_ts`
    let resp = follower_client2.kv_read(b"key5".to_vec(), k5_commit_ts);
    assert!(resp.get_region_error().has_data_is_not_ready());

    let target_leader = PeerClient::new(&cluster, target.get_id(), new_peer(1, 1));
    // Commit on `key1`
    target_leader.must_kv_commit(vec![b"key1".to_vec()], k1_prewrite_ts, get_tso(&pd_client));
    // We can read `(key5, value2)` now
    follower_client2.must_kv_read_equal(b"key5".to_vec(), b"value2".to_vec(), get_tso(&pd_client));
}

// Testing that after region merge, the `safe_ts` could be advanced even without any incoming write
#[test]
fn test_stale_read_after_merge() {
    let (mut cluster, pd_client, _) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_merge)));

    cluster.must_split(&cluster.get_region(&[]), b"key3");
    let source = pd_client.get_region(b"key1").unwrap();
    let target = pd_client.get_region(b"key5").unwrap();

    cluster.must_transfer_leader(target.get_id(), new_peer(1, 1));
    let target_leader = PeerClient::new(&cluster, target.get_id(), new_peer(1, 1));
    // Write `(key5, value1)`
    target_leader.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key5"[..], &b"value1"[..])],
        b"key5".to_vec(),
    );

    pd_client.must_merge(source.get_id(), target.get_id());

    let mut follower_client2 = PeerClient::new(&cluster, target.get_id(), new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);
    // We can read `(key5, value1)` with the newest ts
    follower_client2.must_kv_read_equal(b"key5".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that during the merge, the leader of the source region won't not update the
// `safe_ts` since it can't know when the merge is completed and whether there are new
// kv write into its key range
#[test]
fn test_read_source_region_after_target_region_merged() {
    let (mut cluster, pd_client, leader_client) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_merge)));

    // Write on source region
    let k1_commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    cluster.must_split(&cluster.get_region(&[]), b"key3");
    let source = pd_client.get_region(b"key1").unwrap();
    let target = pd_client.get_region(b"key5").unwrap();
    // Transfer the target region leader to store 1 and the source region leader to store 2
    cluster.must_transfer_leader(target.get_id(), new_peer(1, 1));
    cluster.must_transfer_leader(source.get_id(), find_peer(&source, 2).unwrap().clone());
    // Get the source region follower on store 3
    let mut source_follower_client3 = PeerClient::new(
        &cluster,
        source.get_id(),
        find_peer(&source, 3).unwrap().clone(),
    );
    source_follower_client3.ctx.set_stale_read(true);
    source_follower_client3.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), k1_commit_ts1);

    // Pause on source region `prepare_merge` on store 2 and store 3
    let apply_before_prepare_merge_2_3 = "apply_before_prepare_merge_2_3";
    fail::cfg(apply_before_prepare_merge_2_3, "pause").unwrap();

    // Merge source region into target region
    pd_client.must_merge(source.get_id(), target.get_id());

    // Leave a lock on the original source region key range through the target region leader
    let target_leader = PeerClient::new(&cluster, target.get_id(), new_peer(1, 1));
    let k1_prewrite_ts2 = get_tso(&pd_client);
    target_leader.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
        k1_prewrite_ts2,
    );

    // Wait for the source region leader to update `safe_ts` (if it can)
    sleep_ms(50);

    // We still can read `key1` with `k1_commit_ts1` through source region
    source_follower_client3.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), k1_commit_ts1);
    // But can't read `key2` with `k1_prewrite_ts2` because the source leader can't update
    // `safe_ts` after source region is merged into target region even though the source leader
    // didn't know the merge is complement
    let resp = source_follower_client3.kv_read(b"key1".to_vec(), k1_prewrite_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready());

    fail::remove(apply_before_prepare_merge_2_3);
}

// Testing that altough the source region's `safe_ts` wont't be updated during merge, after merge
// rollbacked it should resume updating
#[test]
fn test_stale_read_after_rollback_merge() {
    let (mut cluster, pd_client, leader_client) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_merge)));

    // Write on source region
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    cluster.must_split(&cluster.get_region(&[]), b"key3");
    let source = pd_client.get_region(b"key1").unwrap();
    let target = pd_client.get_region(b"key5").unwrap();

    // Trigger merge rollback
    let on_schedule_merge = "on_schedule_merge";
    fail::cfg(on_schedule_merge, "return()").unwrap();
    cluster.must_try_merge(source.get_id(), target.get_id());
    // Change the epoch of target region and the merge will fail
    pd_client.must_remove_peer(target.get_id(), new_peer(3, 3));
    fail::remove(on_schedule_merge);

    // Make sure the rollback is done, it is okey to use raw kv here
    cluster.must_put(b"key2", b"value2");

    let mut source_client3 = PeerClient::new(
        &cluster,
        source.get_id(),
        find_peer(&source, 3).unwrap().clone(),
    );
    source_client3.ctx.set_stale_read(true);
    // the `safe_ts` should resume updating after merge rollback so we can read `key1` with the newest ts
    source_client3.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that the new leader should ignore the pessimistic lock that wrote by the previous
// leader and keep updating the `safe_ts`
#[test]
fn test_new_leader_ignore_pessimistic_lock() {
    let (mut cluster, pd_client, leader_client) = prepare_for_stale_read(new_peer(1, 1));

    // Write (`key1`, `value1`)
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Leave a pessimistic lock on the region
    leader_client.must_kv_pessimistic_lock(b"key2".to_vec(), get_tso(&pd_client));

    // Transfer to a new leader
    cluster.must_transfer_leader(1, new_peer(2, 2));

    let mut follower_client3 = PeerClient::new(&cluster, 1, new_peer(3, 3));
    follower_client3.ctx.set_stale_read(true);
    // The new leader should be able to update `safe_ts` so we can read `key1` with the newest ts
    follower_client3.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that we perform stale read on learner
#[test]
fn test_stale_read_on_learner() {
    let (cluster, pd_client, leader_client) = prepare_for_stale_read(new_peer(1, 1));

    // Write `(key1, value1)`
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Replace peer 2 with learner
    pd_client.must_remove_peer(1, new_peer(2, 2));
    pd_client.must_add_peer(1, new_learner_peer(2, 4));
    let mut learner_client2 = PeerClient::new(&cluster, 1, new_learner_peer(2, 4));
    learner_client2.ctx.set_stale_read(true);

    // We can read on the learner with the newst ts
    learner_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that stale read request with a future ts should not update the `concurency_manager`'s `max_ts`
#[test]
fn test_stale_read_future_ts_not_update_max_ts() {
    let (_cluster, pd_client, mut leader_client) = prepare_for_stale_read(new_peer(1, 1));
    leader_client.ctx.set_stale_read(true);

    // Write `(key1, value1)`
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Perform stale read with a future ts should return error
    let read_ts = get_tso(&pd_client) + 10000000;
    let resp = leader_client.kv_read(b"key1".to_vec(), read_ts);
    assert!(resp.get_region_error().has_data_is_not_ready());

    // The `max_ts` should not updated by the stale read request, so we can prewrite and commit
    // `async_commit` transaction with a ts that smaller than the `read_ts`
    let prewrite_ts = get_tso(&pd_client);
    assert!(prewrite_ts < read_ts);
    leader_client.must_kv_prewrite_async_commit(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        prewrite_ts,
    );
    let commit_ts = get_tso(&pd_client);
    assert!(commit_ts < read_ts);
    leader_client.must_kv_commit(vec![b"key2".to_vec()], prewrite_ts, commit_ts);
    leader_client.must_kv_read_equal(b"key2".to_vec(), b"value1".to_vec(), get_tso(&pd_client));

    // Perform stale read with a future ts should return error
    let read_ts = get_tso(&pd_client) + 10000000;
    let resp = leader_client.kv_read(b"key1".to_vec(), read_ts);
    assert!(resp.get_region_error().has_data_is_not_ready());

    // The `max_ts` should not updated by the stale read request, so 1pc transaction with a ts that smaller
    // than the `read_ts` should not be fallbacked to 2pc
    let prewrite_ts = get_tso(&pd_client);
    assert!(prewrite_ts < read_ts);
    leader_client.must_kv_prewrite_one_pc(
        vec![new_mutation(Op::Put, &b"key3"[..], &b"value1"[..])],
        b"key3".to_vec(),
        prewrite_ts,
    );
    // `key3` is write as 1pc transaction so we can read `key3` without commit
    leader_client.must_kv_read_equal(b"key3".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

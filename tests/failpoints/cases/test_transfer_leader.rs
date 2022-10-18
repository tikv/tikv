// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use crossbeam::channel;
use engine_traits::CF_LOCK;
use futures::executor::block_on;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv::storage::Snapshot;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    HandyRwLock,
};
use txn_types::{Key, PessimisticLock};

/// When a follower applies log slowly, leader should not transfer leader
/// to it. Otherwise, new leader may wait a long time to serve read/write
/// requests.
#[test]
fn test_transfer_leader_slow_apply() {
    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);

    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 1002));
    pd_client.must_add_peer(r1, new_peer(3, 1003));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let fp = "on_handle_apply_1003";
    fail::cfg(fp, "pause").unwrap();
    for i in 0..=cluster.cfg.raft_store.leader_transfer_max_log_lag {
        let bytes = format!("k{:03}", i).into_bytes();
        cluster.must_put(&bytes, &bytes);
    }
    cluster.transfer_leader(r1, new_peer(3, 1003));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    assert_ne!(cluster.leader_of_region(r1).unwrap(), new_peer(3, 1003));
    fail::remove(fp);
    cluster.must_transfer_leader(r1, new_peer(3, 1003));
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
}

#[test]
fn test_prewrite_before_max_ts_is_synced() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let do_prewrite = |cluster: &mut Cluster<ServerCluster>| {
        let region_id = 1;
        let leader = cluster.leader_of_region(region_id).unwrap();
        let epoch = cluster.get_region_epoch(region_id);
        let mut ctx = Context::default();
        ctx.set_region_id(region_id);
        ctx.set_peer(leader);
        ctx.set_region_epoch(epoch);

        let mut req = PrewriteRequest::default();
        req.set_context(ctx);
        req.set_primary_lock(b"key".to_vec());
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.set_key(b"key".to_vec());
        mutation.set_value(b"value".to_vec());
        req.mut_mutations().push(mutation);
        req.set_start_version(100);
        req.set_lock_ttl(20000);
        req.set_use_async_commit(true);
        client.kv_prewrite(&req).unwrap()
    };

    cluster.must_transfer_leader(1, new_peer(2, 2));
    fail::cfg("test_raftstore_get_tso", "return(50)").unwrap();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    let resp = do_prewrite(&mut cluster);
    assert!(resp.get_region_error().has_max_timestamp_not_synced());
    fail::remove("test_raftstore_get_tso");
    thread::sleep(Duration::from_millis(200));
    let resp = do_prewrite(&mut cluster);
    assert!(!resp.get_region_error().has_max_timestamp_not_synced());
}

#[test]
fn test_delete_lock_proposed_after_proposing_locks_1() {
    test_delete_lock_proposed_after_proposing_locks_impl(1);
}

#[test]
fn test_delete_lock_proposed_after_proposing_locks_2() {
    // Repeated transfer leader command before proposing the write command
    test_delete_lock_proposed_after_proposing_locks_impl(2);
}

fn test_delete_lock_proposed_after_proposing_locks_impl(transfer_msg_count: usize) {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let region_id = 1;
    cluster.must_transfer_leader(1, new_peer(1, 1));
    let leader = cluster.leader_of_region(region_id).unwrap();

    let snapshot = cluster.must_get_snapshot_of_region(region_id);
    let txn_ext = snapshot.txn_ext.unwrap();
    txn_ext
        .pessimistic_locks
        .write()
        .insert(vec![(
            Key::from_raw(b"key"),
            PessimisticLock {
                primary: b"key".to_vec().into_boxed_slice(),
                start_ts: 10.into(),
                ttl: 1000,
                for_update_ts: 10.into(),
                min_commit_ts: 20.into(),
            },
        )])
        .unwrap();

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut req = CleanupRequest::default();
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_region_epoch(cluster.get_region_epoch(region_id));
    ctx.set_peer(leader);
    req.set_context(ctx);
    req.set_key(b"key".to_vec());
    req.set_start_version(10);
    req.set_current_ts(u64::MAX);

    // Pause the command after it mark the lock as deleted.
    fail::cfg("raftkv_async_write", "pause").unwrap();
    let (tx, resp_rx) = mpsc::channel();
    thread::spawn(move || tx.send(client.kv_cleanup(&req).unwrap()).unwrap());

    thread::sleep(Duration::from_millis(200));
    resp_rx.try_recv().unwrap_err();

    for _ in 0..transfer_msg_count {
        cluster.transfer_leader(1, new_peer(2, 2));
    }
    thread::sleep(Duration::from_millis(200));

    // Transfer leader will not make the command fail.
    fail::remove("raftkv_async_write");
    let resp = resp_rx.recv().unwrap();
    assert!(!resp.has_region_error());

    for _ in 0..10 {
        thread::sleep(Duration::from_millis(100));
        cluster.reset_leader_of_region(region_id);
        if cluster.leader_of_region(region_id).unwrap().id == 2 {
            let snapshot = cluster.must_get_snapshot_of_region(1);
            assert!(
                snapshot
                    .get_cf(CF_LOCK, &Key::from_raw(b"key"))
                    .unwrap()
                    .is_none()
            );
            return;
        }
    }
    panic!("region should succeed to transfer leader to peer 2");
}

#[test]
fn test_delete_lock_proposed_before_proposing_locks() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let region_id = 1;
    cluster.must_transfer_leader(1, new_peer(1, 1));
    let leader = cluster.leader_of_region(region_id).unwrap();

    let snapshot = cluster.must_get_snapshot_of_region(region_id);
    let txn_ext = snapshot.txn_ext.unwrap();
    txn_ext
        .pessimistic_locks
        .write()
        .insert(vec![(
            Key::from_raw(b"key"),
            PessimisticLock {
                primary: b"key".to_vec().into_boxed_slice(),
                start_ts: 10.into(),
                ttl: 1000,
                for_update_ts: 10.into(),
                min_commit_ts: 20.into(),
            },
        )])
        .unwrap();

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut req = CleanupRequest::default();
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_region_epoch(cluster.get_region_epoch(region_id));
    ctx.set_peer(leader);
    req.set_context(ctx);
    req.set_key(b"key".to_vec());
    req.set_start_version(10);
    req.set_current_ts(u64::MAX);

    // Pause the command before it actually removes locks.
    fail::cfg("scheduler_async_write_finish", "pause").unwrap();
    let (tx, resp_rx) = mpsc::channel();
    thread::spawn(move || tx.send(client.kv_cleanup(&req).unwrap()).unwrap());

    thread::sleep(Duration::from_millis(200));
    resp_rx.try_recv().unwrap_err();

    cluster.transfer_leader(1, new_peer(2, 2));
    thread::sleep(Duration::from_millis(200));

    // Transfer leader will not make the command fail.
    fail::remove("scheduler_async_write_finish");
    let resp = resp_rx.recv().unwrap();
    assert!(!resp.has_region_error());

    for _ in 0..10 {
        thread::sleep(Duration::from_millis(100));
        cluster.reset_leader_of_region(region_id);
        if cluster.leader_of_region(region_id).unwrap().id == 2 {
            let snapshot = cluster.must_get_snapshot_of_region(1);
            assert!(
                snapshot
                    .get_cf(CF_LOCK, &Key::from_raw(b"key"))
                    .unwrap()
                    .is_none()
            );
            return;
        }
    }
    panic!("region should succeed to transfer leader to peer 2");
}

#[test]
fn test_read_lock_after_become_follower() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let region_id = 1;
    cluster.must_transfer_leader(1, new_peer(3, 3));

    let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();

    // put kv after get start ts, then this commit will cause a
    // PessimisticLockNotFound if the pessimistic lock get missing.
    cluster.must_put(b"key", b"value");

    let leader = cluster.leader_of_region(region_id).unwrap();
    let snapshot = cluster.must_get_snapshot_of_region(region_id);
    let txn_ext = snapshot.txn_ext.unwrap();
    let for_update_ts = block_on(cluster.pd_client.get_tso()).unwrap();
    txn_ext
        .pessimistic_locks
        .write()
        .insert(vec![(
            Key::from_raw(b"key"),
            PessimisticLock {
                primary: b"key".to_vec().into_boxed_slice(),
                start_ts,
                ttl: 1000,
                for_update_ts,
                min_commit_ts: for_update_ts,
            },
        )])
        .unwrap();

    let addr = cluster.sim.rl().get_addr(3);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut req = PrewriteRequest::default();
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_region_epoch(cluster.get_region_epoch(region_id));
    ctx.set_peer(leader);
    req.set_context(ctx);
    req.set_primary_lock(b"key".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"key".to_vec());
    mutation.set_value(b"value2".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(start_ts.into_inner());
    req.set_lock_ttl(20000);

    // Pause the command before it executes prewrite.
    fail::cfg("txn_before_process_write", "pause").unwrap();
    let (tx, resp_rx) = mpsc::channel();
    thread::spawn(move || tx.send(client.kv_prewrite(&req).unwrap()).unwrap());

    thread::sleep(Duration::from_millis(200));
    resp_rx.try_recv().unwrap_err();

    // And pause applying the write on the leader.
    fail::cfg("on_apply_write_cmd", "pause").unwrap();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    thread::sleep(Duration::from_millis(200));

    // Transfer leader will not make the command fail.
    fail::remove("txn_before_process_write");
    let resp = resp_rx.recv().unwrap();
    // The term has changed, so we should get a stale command error instead a
    // PessimisticLockNotFound.
    assert!(resp.get_region_error().has_stale_command());
}

/// This function does the following things
///
/// 0. Transfer the region's(id=1) leader to store 1.
/// 1. Inserted 5 entries and make all stores commit and apply them.
/// 2. Prevent the store 3 from append following logs.
/// 3. Insert another 20 entries.
/// 4. Wait for some time so that part of the entry cache are compacted
///    on the leader(store 1).
fn run_cluster_for_test_warmup_entry_cache(cluster: &mut Cluster<NodeCluster>) {
    // Let the leader compact the entry cache.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    for i in 1..5u32 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        must_get_equal(&cluster.get_engine(3), &k, &v);
    }

    // Let store 3 fall behind.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 3).direction(Direction::Recv),
    ));

    for i in 1..20u32 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        must_get_equal(&cluster.get_engine(2), &k, &v);
    }

    // Wait until part of the leader's entry cache is compacted.
    sleep_ms(cluster.cfg.raft_store.raft_log_gc_tick_interval.as_millis() * 2);
}

fn prevent_from_gc_raft_log(cluster: &mut Cluster<NodeCluster>) {
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(100000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    cluster.cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    cluster.cfg.raft_store.raft_log_reserve_max_ticks = 20;
}

fn run_cluster_and_warm_up_cache_for_store2() -> Cluster<NodeCluster> {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::secs(1000);
    prevent_from_gc_raft_log(&mut cluster);
    run_cluster_for_test_warmup_entry_cache(&mut cluster);

    let (sx, rx) = channel::unbounded();
    let recv_filter = Box::new(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgTransferLeader)
            .set_msg_callback(Arc::new(move |m| {
                sx.send(m.get_message().get_from()).unwrap();
            })),
    );
    cluster.sim.wl().add_recv_filter(1, recv_filter);

    let (sx2, rx2) = channel::unbounded();
    fail::cfg_callback("on_entry_cache_warmed_up", move || sx2.send(true).unwrap()).unwrap();
    cluster.transfer_leader(1, new_peer(2, 2));

    // Cache should be warmed up.
    assert!(rx2.recv_timeout(Duration::from_millis(500)).unwrap());
    // It should ack the message just after cache is warmed up.
    assert_eq!(rx.recv_timeout(Duration::from_millis(500)).unwrap(), 2);
    cluster.sim.wl().clear_recv_filters(1);
    cluster
}

/// Leader should carry a correct index in TransferLeaderMsg so that
/// the follower can warm up the entry cache with this index.
#[test]
fn test_transfer_leader_msg_index() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::secs(1000);
    prevent_from_gc_raft_log(&mut cluster);
    run_cluster_for_test_warmup_entry_cache(&mut cluster);

    let (sx, rx) = channel::unbounded();
    let recv_filter = Box::new(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgTransferLeader)
            .set_msg_callback(Arc::new(move |m| {
                sx.send(m.get_message().get_index()).unwrap();
            })),
    );
    cluster.sim.wl().add_recv_filter(2, recv_filter);

    // TransferLeaderMsg.index should be equal to the store3's replicated_index.
    cluster.transfer_leader(1, new_peer(2, 2));
    let replicated_index = cluster.raft_local_state(1, 3).last_index;
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        replicated_index,
    );
}

/// The store should ack the transfer leader msg immediately
/// when the warmup range start is larger than it's last index.
#[test]
fn test_when_warmup_range_start_is_larger_than_last_index() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::secs(1000);
    prevent_from_gc_raft_log(&mut cluster);
    run_cluster_for_test_warmup_entry_cache(&mut cluster);
    cluster.pd_client.disable_default_operator();

    let s4 = cluster.add_new_engine();

    // Prevent peer 4 from appending logs, so it's last index should
    // be really small.
    let recv_filter_s4 = Box::new(
        RegionPacketFilter::new(1, s4)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    );
    cluster.sim.wl().add_recv_filter(s4, recv_filter_s4);

    let (sx, rx) = channel::unbounded();
    let recv_filter_1 = Box::new(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgTransferLeader)
            .set_msg_callback(Arc::new(move |m| {
                sx.send(m.get_message().get_from()).unwrap();
            })),
    );
    cluster.sim.wl().add_recv_filter(1, recv_filter_1);

    cluster.pd_client.must_add_peer(1, new_peer(s4, s4));
    cluster.transfer_leader(1, new_peer(s4, s4));
    // Store(s4) should ack the transfer leader msg immediately.
    assert_eq!(rx.recv_timeout(Duration::from_millis(500)).unwrap(), s4);
}

/// When the start index of warmup range is compacted, the follower should
/// still warm up and use the compacted_idx as the start index.
#[test]
fn test_when_warmup_range_start_is_compacted() {
    let mut cluster = new_node_cluster(0, 3);
    // GC raft log aggressively.
    cluster.cfg.raft_store.merge_max_log_gap = 1;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(5);
    cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::secs(1000);
    run_cluster_for_test_warmup_entry_cache(&mut cluster);
    cluster.pd_client.disable_default_operator();

    // Case `test_transfer_leader_msg_index` already proves that
    // the warmup_range_start is equal to the replicated_index.
    let warmup_range_start = cluster.raft_local_state(1, 3).last_index;
    cluster.wait_log_truncated(1, 2, warmup_range_start + 10);
    let s2_truncated_index = cluster.truncated_state(1, 2).get_index();
    let s2_last_index = cluster.raft_local_state(1, 2).last_index;
    assert!(warmup_range_start < s2_truncated_index);
    assert!(s2_truncated_index + 5 <= s2_last_index);

    // Cache should be warmed up successfully.
    let (sx, rx) = channel::unbounded();
    fail::cfg_callback("on_entry_cache_warmed_up", move || sx.send(true).unwrap()).unwrap();
    cluster.transfer_leader(1, new_peer(2, 2));
    rx.recv_timeout(Duration::from_millis(500)).unwrap();
}

/// Transfer leader should work as normal when disable warming up entry cache.
#[test]
fn test_turnoff_warmup_entry_cache() {
    let mut cluster = new_node_cluster(0, 3);
    prevent_from_gc_raft_log(&mut cluster);
    run_cluster_for_test_warmup_entry_cache(&mut cluster);
    cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::secs(0);
    fail::cfg("worker_async_fetch_raft_log", "pause").unwrap();
    cluster.must_transfer_leader(1, new_peer(2, 2));
}

/// When the follower has not warmed up the entry cache and the timeout of
/// warmup is very long, then the leadership transfer can never succeed.
#[test]
fn test_when_warmup_fail_and_its_timeout_is_too_long() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::secs(1000);
    prevent_from_gc_raft_log(&mut cluster);
    run_cluster_for_test_warmup_entry_cache(&mut cluster);

    fail::cfg("worker_async_fetch_raft_log", "pause").unwrap();
    cluster.transfer_leader(1, new_peer(2, 2));
    // Theoretically, the leader transfer can't succeed unless it sleeps
    // max_entry_cache_warmup_duration.
    sleep_ms(50);
    let leader = cluster.leader_of_region(1).unwrap();
    assert_eq!(leader.get_id(), 1);
}

/// When the follower has not warmed up the entry cache and the timeout of
/// warmup is pretty short, then the leadership transfer should succeed quickly.
#[test]
fn test_when_warmup_fail_and_its_timeout_is_short() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::millis(10);
    prevent_from_gc_raft_log(&mut cluster);
    run_cluster_for_test_warmup_entry_cache(&mut cluster);

    fail::cfg("worker_async_fetch_raft_log", "pause").unwrap();
    cluster.must_transfer_leader(1, new_peer(2, 2));
}

/// The follower should ack the msg when the cache is warmed up.
/// Besides, the cache should be kept for a period of time.
#[test]
fn test_when_warmup_succeed_and_become_leader() {
    let mut cluster = run_cluster_and_warm_up_cache_for_store2();

    // Generally, the cache will be compacted during post_apply.
    // However, if the cache is warmed up recently, the cache should be kept.
    let applied_index = cluster.apply_state(1, 2).applied_index;
    cluster.must_put(b"kk1", b"vv1");
    cluster.wait_applied_index(1, 2, applied_index + 1);

    // It should ack the message when cache is already warmed up.
    // It needs not to fetch raft log anymore.
    fail::cfg("worker_async_fetch_raft_log", "pause").unwrap();
    cluster.sim.wl().clear_recv_filters(1);
    cluster.must_transfer_leader(1, new_peer(2, 2));
}

/// The follower should exit warmup state if it does not become leader
/// in a period of time.
#[test]
fn test_when_warmup_succeed_and_not_become_leader() {
    let mut cluster = run_cluster_and_warm_up_cache_for_store2();

    let (sx, rx) = channel::unbounded();
    fail::cfg_callback("worker_async_fetch_raft_log", move || {
        sx.send(true).unwrap()
    })
    .unwrap();
    fail::cfg("entry_cache_warmed_up_state_is_stale", "return").unwrap();

    // Since the warmup state is stale, the peer should exit warmup state,
    // and the entry cache should be compacted during post_apply.
    let applied_index = cluster.apply_state(1, 2).applied_index;
    cluster.must_put(b"kk1", b"vv1");
    cluster.wait_applied_index(1, 2, applied_index + 1);
    // The peer should warm up cache again when it receives a new TransferLeaderMsg.
    cluster.transfer_leader(1, new_peer(2, 2));
    assert!(rx.recv_timeout(Duration::from_millis(500)).unwrap());
}

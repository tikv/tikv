// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        mpsc::{self, channel},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use crossbeam::channel;
use engine_traits::CF_LOCK;
use futures::executor::block_on;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, metapb::PeerRole, pdpb, tikvpb::TikvClient};
use pd_client::PdClient;
use raft::eraftpb::{ConfChangeType, MessageType};
use raftstore::store::Callback;
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv::storage::Snapshot;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    HandyRwLock,
};
use txn_types::{Key, LastChange, PessimisticLock};

/// When a follower applies log slowly, leader should not transfer leader
/// to it. Otherwise, new leader may wait a long time to serve read/write
/// requests.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_transfer_leader_slow_apply() {
    // 3 nodes cluster.
    let mut cluster = new_cluster(0, 3);

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

#[test_case(test_raftstore::new_server_cluster)]
#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_prewrite_before_max_ts_is_synced() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let do_prewrite = |region_id, leader, epoch| {
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
    let epoch = cluster.get_region_epoch(1);
    let resp = do_prewrite(1, new_peer(1, 1), epoch.clone());
    assert!(resp.get_region_error().has_max_timestamp_not_synced());
    fail::remove("test_raftstore_get_tso");
    thread::sleep(Duration::from_millis(200));
    let resp = do_prewrite(1, new_peer(1, 1), epoch);
    assert!(!resp.get_region_error().has_max_timestamp_not_synced());
}

macro_rules! test_delete_lock_proposed_after_proposing_locks_impl {
    ($cluster:expr, $transfer_msg_count:expr) => {
        $cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
        $cluster.run();

        let region_id = 1;
        $cluster.must_transfer_leader(1, new_peer(1, 1));
        let leader = $cluster.leader_of_region(region_id).unwrap();

        let snapshot = $cluster.must_get_snapshot_of_region(region_id);
        let txn_ext = snapshot.txn_ext.unwrap();
        txn_ext
            .pessimistic_locks
            .write()
            .insert(
                vec![(
                    Key::from_raw(b"key"),
                    PessimisticLock {
                        primary: b"key".to_vec().into_boxed_slice(),
                        start_ts: 10.into(),
                        ttl: 1000,
                        for_update_ts: 10.into(),
                        min_commit_ts: 20.into(),
                        last_change: LastChange::make_exist(5.into(), 3),
                        is_locked_with_conflict: false,
                    },
                )],
                512 << 10,
                100 << 20,
            )
            .unwrap();

        let addr = $cluster.sim.rl().get_addr(1);
        let env = Arc::new(Environment::new(1));
        let channel = ChannelBuilder::new(env).connect(&addr);
        let client = TikvClient::new(channel);

        let mut req = CleanupRequest::default();
        let mut ctx = Context::default();
        ctx.set_region_id(region_id);
        ctx.set_region_epoch($cluster.get_region_epoch(region_id));
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

        for _ in 0..$transfer_msg_count {
            $cluster.transfer_leader(1, new_peer(2, 2));
        }
        thread::sleep(Duration::from_millis(200));

        // Transfer leader will not make the command fail.
        fail::remove("raftkv_async_write");
        let resp = resp_rx.recv().unwrap();
        assert!(!resp.has_region_error());

        for _ in 0..10 {
            thread::sleep(Duration::from_millis(100));
            $cluster.reset_leader_of_region(region_id);
            if $cluster.leader_of_region(region_id).unwrap().id == 2 {
                let snapshot = $cluster.must_get_snapshot_of_region(1);
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
    };
}

#[test_case(test_raftstore::new_server_cluster)]
#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_delete_lock_proposed_after_proposing_locks_1() {
    let mut cluster = new_cluster(0, 3);
    test_delete_lock_proposed_after_proposing_locks_impl!(cluster, 1);
}

#[test_case(test_raftstore::new_server_cluster)]
#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_delete_lock_proposed_after_proposing_locks_2() {
    // Repeated transfer leader command before proposing the write command
    let mut cluster = new_cluster(0, 3);
    test_delete_lock_proposed_after_proposing_locks_impl!(cluster, 2);
}

#[test_case(test_raftstore::new_server_cluster)]
#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_delete_lock_proposed_before_proposing_locks() {
    let peer_size_limit = 512 << 10;
    let instance_size_limit = 100 << 20;
    let mut cluster = new_cluster(0, 3);
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
        .insert(
            vec![(
                Key::from_raw(b"key"),
                PessimisticLock {
                    primary: b"key".to_vec().into_boxed_slice(),
                    start_ts: 10.into(),
                    ttl: 1000,
                    for_update_ts: 10.into(),
                    min_commit_ts: 20.into(),
                    last_change: LastChange::make_exist(5.into(), 3),
                    is_locked_with_conflict: false,
                },
            )],
            peer_size_limit,
            instance_size_limit,
        )
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

#[test_case(test_raftstore::new_server_cluster)]
#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_read_lock_after_become_follower() {
    let peer_size_limit = 512 << 10;
    let instance_size_limit = 100 << 20;
    let mut cluster = new_cluster(0, 3);
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
        .insert(
            vec![(
                Key::from_raw(b"key"),
                PessimisticLock {
                    primary: b"key".to_vec().into_boxed_slice(),
                    start_ts,
                    ttl: 1000,
                    for_update_ts,
                    min_commit_ts: for_update_ts,
                    last_change: LastChange::make_exist(start_ts.prev(), 1),
                    is_locked_with_conflict: false,
                },
            )],
            peer_size_limit,
            instance_size_limit,
        )
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
/// 4. Wait for some time so that part of the entry cache are compacted on the
///    leader(store 1).
macro_rules! run_cluster_for_test_warmup_entry_cache {
    ($cluster:expr) => {
        // Let the leader compact the entry cache.
        $cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
        $cluster.run();

        $cluster.must_transfer_leader(1, new_peer(1, 1));

        for i in 1..5u32 {
            let k = i.to_string().into_bytes();
            let v = k.clone();
            $cluster.must_put(&k, &v);
            must_get_equal(&$cluster.get_engine(3), &k, &v);
        }

        // Let store 3 fall behind.
        $cluster.add_send_filter(CloneFilterFactory(
            RegionPacketFilter::new(1, 3).direction(Direction::Recv),
        ));

        for i in 1..20u32 {
            let k = i.to_string().into_bytes();
            let v = k.clone();
            $cluster.must_put(&k, &v);
            must_get_equal(&$cluster.get_engine(2), &k, &v);
        }

        // Wait until part of the leader's entry cache is compacted.
        sleep_ms(
            $cluster
                .cfg
                .raft_store
                .raft_log_gc_tick_interval
                .as_millis()
                * 2,
        );
    };
}

fn prevent_from_gc_raft_log(cfg: &mut Config) {
    cfg.raft_store.raft_log_gc_count_limit = Some(100000);
    cfg.raft_store.raft_log_gc_threshold = 1000;
    cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::mb(20));
    cfg.raft_store.raft_log_reserve_max_ticks = 20;
}

macro_rules! run_cluster_and_warm_up_cache_for_store2 {
    ($cluster:expr) => {
        $cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::secs(1000);
        prevent_from_gc_raft_log(&mut $cluster.cfg);
        run_cluster_for_test_warmup_entry_cache!($cluster);

        let (sx, rx) = channel::unbounded();
        let recv_filter = Box::new(
            RegionPacketFilter::new(1, 1)
                .direction(Direction::Recv)
                .msg_type(MessageType::MsgTransferLeader)
                .set_msg_callback(Arc::new(move |m| {
                    sx.send(m.get_message().get_from()).unwrap();
                })),
        );
        $cluster.sim.wl().add_recv_filter(1, recv_filter);

        let (sx2, rx2) = channel::unbounded();
        fail::cfg_callback("on_entry_cache_warmed_up", move || sx2.send(true).unwrap()).unwrap();
        $cluster.transfer_leader(1, new_peer(2, 2));

        // Cache should be warmed up.
        assert!(rx2.recv_timeout(Duration::from_millis(500)).unwrap());
        // It should ack the message just after cache is warmed up.
        assert_eq!(rx.recv_timeout(Duration::from_millis(500)).unwrap(), 2);
        $cluster.sim.wl().clear_recv_filters(1);
    };
}

/// Leader should carry a correct index in TransferLeaderMsg so that
/// the follower can warm up the entry cache with this index.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_transfer_leader_msg_index() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::secs(1000);
    prevent_from_gc_raft_log(&mut cluster.cfg);
    run_cluster_for_test_warmup_entry_cache!(cluster);

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
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_when_warmup_range_start_is_larger_than_last_index() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.raft_entry_cache_life_time = ReadableDuration::secs(1000);
    prevent_from_gc_raft_log(&mut cluster.cfg);
    run_cluster_for_test_warmup_entry_cache!(cluster);
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
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_when_warmup_range_start_is_compacted() {
    let mut cluster = new_cluster(0, 3);
    // GC raft log aggressively.
    cluster.cfg.raft_store.merge_max_log_gap = 1;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(5);
    cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::secs(1000);
    run_cluster_for_test_warmup_entry_cache!(cluster);
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
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_turnoff_warmup_entry_cache() {
    let mut cluster = new_cluster(0, 3);
    prevent_from_gc_raft_log(&mut cluster.cfg);
    run_cluster_for_test_warmup_entry_cache!(cluster);
    cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::secs(0);
    fail::cfg("worker_async_fetch_raft_log", "pause").unwrap();
    cluster.must_transfer_leader(1, new_peer(2, 2));
}

/// When the follower has not warmed up the entry cache and the timeout of
/// warmup is very long, then the leadership transfer can never succeed.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_when_warmup_fail_and_its_timeout_is_too_long() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::secs(u64::MAX / 2);
    prevent_from_gc_raft_log(&mut cluster.cfg);
    run_cluster_for_test_warmup_entry_cache!(cluster);

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
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_when_warmup_fail_and_its_timeout_is_short() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.raft_store.max_entry_cache_warmup_duration = ReadableDuration::millis(10);
    prevent_from_gc_raft_log(&mut cluster.cfg);
    run_cluster_for_test_warmup_entry_cache!(cluster);

    fail::cfg("worker_async_fetch_raft_log", "pause").unwrap();
    cluster.must_transfer_leader(1, new_peer(2, 2));
}

/// The follower should ack the msg when the cache is warmed up.
/// Besides, the cache should be kept for a period of time.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_when_warmup_succeed_and_become_leader() {
    let mut cluster = new_cluster(0, 3);
    run_cluster_and_warm_up_cache_for_store2!(cluster);

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
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_when_warmup_succeed_and_not_become_leader() {
    let mut cluster = new_cluster(0, 3);
    run_cluster_and_warm_up_cache_for_store2!(cluster);

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

/// Leader transferee should only ack MsgTransferLeader once.
// TODO: It may need to retry sending MsgTransferLeader in case the ack is lost.
#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_warmup_entry_ack_transfer_leader_once() {
    let mut cluster = new_cluster(0, 3);
    prevent_from_gc_raft_log(&mut cluster.cfg);
    run_cluster_for_test_warmup_entry_cache!(cluster);

    // Wait follower compact the cache after applying the logs.
    let applied_index = cluster.apply_state(1, 2).applied_index;
    cluster.must_put(b"kk1", b"vv1");
    cluster.wait_applied_index(1, 2, applied_index + 1);

    let (tx, rx) = channel::unbounded();
    let recv_filter = Box::new(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgTransferLeader)
            .set_msg_callback(Arc::new(move |m| {
                tx.send(m.get_message().get_from()).unwrap();
            })),
    );
    cluster.sim.wl().add_recv_filter(1, recv_filter);

    // The peer should only ack transfer leader once.
    cluster.transfer_leader(1, new_peer(2, 2));
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap_err();
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_check_long_uncommitted_proposals_after_became_leader() {
    let mut cluster = new_cluster(0, 3);
    let base_tick_ms = 50;
    configure_for_lease_read(&mut cluster.cfg, Some(base_tick_ms), Some(1000));
    cluster.cfg.raft_store.check_long_uncommitted_interval = ReadableDuration::millis(200);
    cluster.cfg.raft_store.long_uncommitted_base_threshold = ReadableDuration::millis(500);

    cluster.pd_client.disable_default_operator();
    let r = cluster.run_conf_change();
    cluster.pd_client.must_add_peer(r, new_peer(2, 2));
    cluster.pd_client.must_add_peer(r, new_peer(3, 3));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    cluster.transfer_leader(1, new_peer(2, 2));

    // Must not tick CheckLongUncommitted after became follower.
    thread::sleep(2 * cluster.cfg.raft_store.long_uncommitted_base_threshold.0);
    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    fail::cfg_callback("on_check_long_uncommitted_proposals_1", move || {
        let _ = tx.lock().unwrap().send(());
    })
    .unwrap();
    rx.recv_timeout(2 * cluster.cfg.raft_store.long_uncommitted_base_threshold.0)
        .unwrap_err();

    // Must keep ticking CheckLongUncommitted after became leader.
    cluster.transfer_leader(1, new_peer(1, 1));
    rx.recv_timeout(2 * cluster.cfg.raft_store.long_uncommitted_base_threshold.0)
        .unwrap();
}

// This test simulates a scenario where a configuration change has been applied
// on the transferee, allowing a leader transfer to that peer even if the
// change hasn't been applied on the current leader.
//
// The setup involves a 4-node cluster where peer-1 starts as the leader. A
// configuration change is initiated to remove peer-2. This change commits
// successfully but only applies on peer-2 and peer-4.
//
// The expected result for leader transfer is:
//   - It will fail to peer-2 because it has been removed.
//   - It will fail to peer-3 because it has unapplied configuration change.
//   - It will succeed to peer-4 because it has already applied the
//     configuration change.
#[test]
fn test_when_applied_conf_change_on_transferee() {
    let mut cluster = new_server_cluster(0, 4);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    pd_client.must_add_peer(region_id, new_peer(4, 4));

    cluster.must_put(b"k1", b"v1");

    fail::cfg("apply_on_conf_change_1_1", "pause").unwrap();
    fail::cfg("apply_on_conf_change_3_1", "pause").unwrap();

    pd_client.remove_peer(region_id, new_peer(2, 2));
    sleep_ms(300);
    // Peer 2 still exists since the leader hasn't applied the ConfChange
    // yet.
    pd_client.must_have_peer(region_id, new_peer(2, 2));

    // Use async_put for insertion here to avoid timeout errors, as synchronize put
    // would hang due to the leader's apply process being paused.
    let _ = cluster.async_put(b"k2", b"v2").unwrap();

    pd_client.transfer_leader(region_id, new_peer(2, 2), vec![]);
    sleep_ms(300);
    assert_eq!(
        pd_client.check_region_leader(region_id, new_peer(2, 2)),
        false
    );

    pd_client.transfer_leader(region_id, new_peer(3, 3), vec![]);
    sleep_ms(300);
    assert_eq!(
        pd_client.check_region_leader(region_id, new_peer(3, 3)),
        false
    );

    pd_client.transfer_leader(region_id, new_peer(4, 4), vec![]);
    pd_client.region_leader_must_be(region_id, new_peer(4, 4));

    // Verify the data completeness on the new leader.
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(4), b"k2", b"v2");

    pd_client.must_none_peer(region_id, new_peer(2, 2));
}

// This test verifies that a leader transfer is rejected when the transferee
// has been demoted to a learner but the leader has not yet applied this
// configuration change.
#[test]
fn test_when_applied_conf_change_on_learner_transferee() {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    pd_client.region_leader_must_be(region_id, new_peer(1, 1));

    fail::cfg("apply_on_conf_change_1_1", "pause").unwrap();

    // Demote peer-2 to be a learner.
    pd_client.joint_confchange(
        region_id,
        vec![(ConfChangeType::AddLearnerNode, new_learner_peer(2, 2))],
    );
    sleep_ms(300);

    pd_client.transfer_leader(region_id, new_peer(2, 2), vec![]);
    sleep_ms(300);
    assert_eq!(
        pd_client.check_region_leader(region_id, new_peer(2, 2)),
        false
    );

    pd_client.transfer_leader(region_id, new_peer(3, 3), vec![]);
    pd_client.region_leader_must_be(region_id, new_peer(3, 3));
    let region = block_on(pd_client.get_region_by_id(region_id))
        .unwrap()
        .unwrap();
    assert_eq!(region.get_peers()[1].get_role(), PeerRole::Learner);
}

// This test verifies that a leader transfer is allowed when the transferee
// has applied a conf change but the leader has not yet applied.
#[test]
fn test_when_applied_conf_change_on_transferee_pessimistic_lock() {
    let mut cluster = new_server_cluster(0, 4);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    pd_client.region_leader_must_be(region_id, new_peer(1, 1));

    fail::cfg("apply_on_conf_change_1_1", "pause").unwrap();
    fail::cfg("propose_locks_before_transfer_leader", "return").unwrap();

    pd_client.remove_peer(region_id, new_peer(2, 2));
    sleep_ms(300);
    // Peer 2 still exists since the leader hasn't applied the ConfChange
    // yet.
    pd_client.must_have_peer(region_id, new_peer(2, 2));

    pd_client.transfer_leader(region_id, new_peer(3, 3), vec![]);
    pd_client.region_leader_must_be(region_id, new_peer(3, 3));

    pd_client.must_none_peer(region_id, new_peer(2, 2));
}

// This test verifies that a leader transfer is allowed when the transferee
// has applied a region split but the leader has not yet applied.
#[test]
fn test_when_applied_region_split_on_transferee_pessimistic_lock() {
    let mut cluster = new_server_cluster(0, 3);
    // To enable the transferee to quickly report the split region information.
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(50);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    // Use peer_id 4 as the leader since we want to reuse the failpoint
    // apply_before_split_1_3.
    pd_client.transfer_leader(region_id, new_peer(3, 3), vec![]);
    pd_client.region_leader_must_be(region_id, new_peer(3, 3));

    fail::cfg("apply_before_split_1_1", "pause").unwrap();
    fail::cfg("apply_before_split_1_3", "pause").unwrap();
    fail::cfg("propose_locks_before_transfer_leader", "return").unwrap();

    let region = pd_client.get_region(b"x1").unwrap();
    cluster.split_region(&region, "x2".as_bytes(), Callback::None);
    sleep_ms(300);
    // Expect split is pending on the current leader.
    assert_eq!(pd_client.get_regions_number(), 1);

    pd_client.transfer_leader(region_id, new_peer(2, 2), vec![]);
    sleep_ms(300);
    pd_client.region_leader_must_be(region_id, new_peer(2, 2));
    sleep_ms(300);
    // TODO(hwy): We cannot enable this assertion yet since https://github.com/tikv/tikv/issues/12410.
    // Expect split is finished on the new leader.
    // assert_eq!(pd_client.get_regions_number(), 2);
}

// This test verifies that a leader transfer is:
// - Not allowed for the source region when the transferee has applied a region
//   commit-merge but the leader has not yet applied.
// - Allowed for the source region when the transferee has applied a region
//   prepare-merge but the leader has not yet applied.
// - Allowed for the target region in both scenarios above.
#[test]
fn test_when_applied_region_merge_on_transferee_pessimistic_lock() {
    let mut cluster = new_server_cluster(0, 4);
    // To enable the transferee to quickly report the merged region information.
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(50);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    // Use peer_id 4 since we want to reuse the failpoint
    // apply_before_commit_merge_except_1_4.
    pd_client.must_add_peer(region_id, new_peer(4, 4));
    pd_client.region_leader_must_be(region_id, new_peer(1, 1));

    let region = cluster.get_region(b"x2");
    let region_id = region.id;
    pd_client.split_region(region, pdpb::CheckPolicy::Usekey, vec![b"x2".to_vec()]);
    sleep_ms(300);
    let left_region = cluster.get_region(b"x1");
    let right_region = cluster.get_region(b"x3");
    assert_eq!(region_id, right_region.get_id());
    let left_region_peer_on_store1 = new_peer(
        left_region.get_peers()[0].store_id,
        left_region.get_peers()[0].id,
    );
    pd_client.region_leader_must_be(left_region.get_id(), left_region_peer_on_store1);
    pd_client.region_leader_must_be(right_region.get_id(), new_peer(1, 1));

    fail::cfg("apply_before_commit_merge_except_1_4", "pause").unwrap();
    fail::cfg("propose_locks_before_transfer_leader", "return").unwrap();

    assert_eq!(pd_client.get_regions_number(), 2);
    // Merge right to left.
    pd_client.merge_region(right_region.get_id(), left_region.get_id());
    sleep_ms(300);

    pd_client.transfer_leader(right_region.get_id(), new_peer(4, 4), vec![]);
    sleep_ms(300);
    assert_eq!(
        pd_client.check_region_leader(right_region.get_id(), new_peer(4, 4)),
        false
    );

    pd_client.transfer_leader(right_region.get_id(), new_peer(2, 2), vec![]);
    sleep_ms(300);
    assert_eq!(
        pd_client.check_region_leader(right_region.get_id(), new_peer(2, 2)),
        false
    );

    assert_eq!(left_region.get_peers()[2].store_id, 4);
    let left_region_peer_on_store4 = new_peer(
        left_region.get_peers()[2].store_id,
        left_region.get_peers()[2].id,
    );
    pd_client.transfer_leader(
        left_region.get_id(),
        left_region_peer_on_store4.clone(),
        vec![],
    );
    pd_client.region_leader_must_be(left_region.get_id(), left_region_peer_on_store4);
    sleep_ms(300);

    let left_region_peer_on_store2 = new_peer(
        left_region.get_peers()[1].store_id,
        left_region.get_peers()[1].id,
    );
    pd_client.transfer_leader(
        left_region.get_id(),
        left_region_peer_on_store2.clone(),
        vec![],
    );
    pd_client.region_leader_must_be(left_region.get_id(), left_region_peer_on_store2);
    sleep_ms(300);

    assert_eq!(pd_client.get_regions_number(), 1);
}

// This test verifies that a leader transfer is allowed when the transferee
// has applied a witness switch but the leader has not yet applied.
#[test]
fn test_when_applied_witness_switch_on_transferee_pessimistic_lock() {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    pd_client.transfer_leader(region_id, new_peer(2, 2), vec![]);
    pd_client.region_leader_must_be(region_id, new_peer(2, 2));

    // Pause applying on the current leader (peer-2).
    fail::cfg("before_exec_batch_switch_witness", "pause").unwrap();
    fail::cfg("propose_locks_before_transfer_leader", "return").unwrap();

    // Demote peer-3 to be a witness.
    pd_client.switch_witnesses(region_id, vec![3], vec![true]);
    sleep_ms(300);

    pd_client.transfer_leader(region_id, new_peer(3, 3), vec![]);
    sleep_ms(300);
    assert_eq!(
        pd_client.check_region_leader(region_id, new_peer(3, 3)),
        false
    );

    pd_client.transfer_leader(region_id, new_peer(1, 1), vec![]);
    pd_client.region_leader_must_be(region_id, new_peer(1, 1));
    let region = block_on(pd_client.get_region_by_id(region_id))
        .unwrap()
        .unwrap();
    assert!(region.get_peers()[2].is_witness);
}

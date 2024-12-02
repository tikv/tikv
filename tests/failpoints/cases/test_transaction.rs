// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, sync_channel},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use engine_traits::CF_DEFAULT;
use futures::{executor::block_on, StreamExt};
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{
        self as pb, AssertionLevel, Context, GetRequest, Op, PessimisticLockRequest,
        PrewriteRequest, PrewriteRequestPessimisticAction::*,
    },
    raft_serverpb::RaftMessage,
    tikvpb::TikvClient,
};
use raft::prelude::{ConfChangeType, MessageType};
use raftstore::store::LocksStatus;
use storage::{
    mvcc::{
        self,
        tests::{must_get, must_locked},
    },
    txn::{self, commands},
};
use test_raftstore::{
    configure_for_lease_read, new_learner_peer, new_server_cluster, try_kv_prewrite,
    DropMessageFilter,
};
use tikv::{
    server::gc_worker::gc_by_compact,
    storage::{
        self,
        kv::SnapshotExt,
        lock_manager::MockLockManager,
        txn::tests::{
            must_acquire_pessimistic_lock, must_acquire_pessimistic_lock_return_value, must_commit,
            must_pessimistic_prewrite_put, must_pessimistic_prewrite_put_err, must_prewrite_put,
            must_prewrite_put_err, must_rollback,
        },
        Snapshot, TestEngineBuilder, TestStorageBuilderApiV1,
    },
};
use tikv_kv::{Engine, Modify, WriteData, WriteEvent};
use tikv_util::{
    config::ReadableDuration,
    store::{new_peer, peer::new_incoming_voter},
    HandyRwLock,
};
use txn_types::{Key, LastChange, Mutation, PessimisticLock, TimeStamp};

#[test]
fn test_txn_failpoints() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let (k, v) = (b"k", b"v");
    fail::cfg("prewrite", "return(WriteConflict)").unwrap();
    must_prewrite_put_err(&mut engine, k, v, k, 10);
    fail::remove("prewrite");
    must_prewrite_put(&mut engine, k, v, k, 10);
    fail::cfg("commit", "delay(100)").unwrap();
    must_commit(&mut engine, k, 10, 20);
    fail::remove("commit");

    let v1 = b"v1";
    let (k2, v2) = (b"k2", b"v2");
    must_acquire_pessimistic_lock(&mut engine, k, k, 30, 30);
    fail::cfg("pessimistic_prewrite", "return()").unwrap();
    must_pessimistic_prewrite_put_err(&mut engine, k, v1, k, 30, 30, DoPessimisticCheck);
    must_prewrite_put(&mut engine, k2, v2, k2, 31);
    fail::remove("pessimistic_prewrite");
    must_pessimistic_prewrite_put(&mut engine, k, v1, k, 30, 30, DoPessimisticCheck);
    must_commit(&mut engine, k, 30, 40);
    must_commit(&mut engine, k2, 31, 41);
    must_get(&mut engine, k, 50, v1);
    must_get(&mut engine, k2, 50, v2);
}

#[test]
fn test_atomic_getting_max_ts_and_storing_memory_lock() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();

    let (prewrite_tx, prewrite_rx) = channel();
    let (fp_tx, fp_rx) = sync_channel(1);
    // sleep a while between getting max ts and store the lock in memory
    fail::cfg_callback("before-set-lock-in-memory", move || {
        fp_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(200));
    })
    .unwrap();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::make_put(Key::from_raw(b"k"), b"v".to_vec())],
                b"k".to_vec(),
                40.into(),
                20000,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    fp_rx.recv().unwrap();
    match block_on(storage.get(Context::default(), Key::from_raw(b"k"), 100.into())) {
        // In this case, min_commit_ts is smaller than the start ts, but the lock is visible
        // to the get.
        Err(storage::Error(box storage::ErrorInner::Txn(txn::Error(
            box txn::ErrorInner::Mvcc(mvcc::Error(box mvcc::ErrorInner::KeyIsLocked(lock))),
        )))) => {
            assert_eq!(lock.get_min_commit_ts(), 41);
        }
        res => panic!("unexpected result: {:?}", res),
    }
    let res = prewrite_rx.recv().unwrap().unwrap();
    assert_eq!(res.min_commit_ts, 41.into());
}

#[test]
fn test_snapshot_must_be_later_than_updating_max_ts() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();

    // Suppose snapshot was before updating max_ts, after sleeping for 500ms the
    // following prewrite should complete.
    fail::cfg("after-snapshot", "sleep(500)").unwrap();
    let read_ts = 20.into();
    let get_fut = storage.get(Context::default(), Key::from_raw(b"j"), read_ts);
    thread::sleep(Duration::from_millis(100));
    fail::remove("after-snapshot");
    let (prewrite_tx, prewrite_rx) = channel();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::make_put(Key::from_raw(b"j"), b"v".to_vec())],
                b"j".to_vec(),
                10.into(),
                20000,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    let has_lock = block_on(get_fut).is_err();
    let res = prewrite_rx.recv().unwrap().unwrap();
    // We must make sure either the lock is visible to the reader or min_commit_ts >
    // read_ts.
    assert!(res.min_commit_ts > read_ts || has_lock);
}

#[test]
fn test_update_max_ts_before_scan_memory_locks() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();

    fail::cfg("before-storage-check-memory-locks", "sleep(500)").unwrap();
    let get_fut = storage.get(Context::default(), Key::from_raw(b"k"), 100.into());

    thread::sleep(Duration::from_millis(200));

    let (prewrite_tx, prewrite_rx) = channel();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::make_put(Key::from_raw(b"k"), b"v".to_vec())],
                b"k".to_vec(),
                10.into(),
                20000,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();

    // The prewritten lock is not seen by the reader
    assert_eq!(block_on(get_fut).unwrap().0, None);
    // But we make sure in this case min_commit_ts is greater than start_ts.
    let res = prewrite_rx.recv().unwrap().unwrap();
    assert_eq!(res.min_commit_ts, 101.into());
}

/// Generates a test that checks the correct behavior of holding and dropping
/// locks, during the process of a single prewrite command.
macro_rules! lock_release_test {
    (
        $test_name:ident,
        $lock_exists:ident,
        $before_actions:expr,
        $middle_actions:expr,
        $after_actions:expr,
        $should_succeed:expr
    ) => {
        #[test]
        fn $test_name() {
            let engine = TestEngineBuilder::new().build().unwrap();
            let storage =
                TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
                    .build()
                    .unwrap();

            let key = Key::from_raw(b"k");
            let cm = storage.get_concurrency_manager();
            let $lock_exists = || cm.read_key_check(&key, |_| Err(())).is_err();

            $before_actions;

            let (prewrite_tx, prewrite_rx) = channel();
            storage
                .sched_txn_command(
                    commands::Prewrite::new(
                        vec![Mutation::make_put(key.clone(), b"v".to_vec())],
                        b"k".to_vec(),
                        10.into(),
                        20000,
                        false,
                        1,
                        TimeStamp::default(),
                        TimeStamp::default(),
                        Some(vec![]),
                        false,
                        AssertionLevel::Off,
                        Context::default(),
                    ),
                    Box::new(move |res| {
                        prewrite_tx.send(res).unwrap();
                    }),
                )
                .unwrap();
            $middle_actions;
            let res = prewrite_rx.recv();
            assert_eq!(res.unwrap().is_ok(), $should_succeed);
            $after_actions;
        }
    };
}

// Must release lock after prewrite fails.
lock_release_test!(
    test_lock_lifetime_on_prewrite_failure,
    lock_exists,
    {
        fail::cfg(
            "rockskv_async_write",
            "return(Err(KvError::from(KvErrorInner::EmptyRequest)))",
        )
        .unwrap();
        assert!(!lock_exists());
    },
    {},
    assert!(!lock_exists()),
    false
);

// Must hold lock until prewrite ends. Must release lock after prewrite
// succeeds.
lock_release_test!(
    test_lock_lifetime_on_prewrite_success,
    lock_exists,
    {
        fail::cfg("rockskv_async_write", "sleep(500)").unwrap();
        assert!(!lock_exists());
    },
    {
        thread::sleep(Duration::from_millis(200));
        assert!(lock_exists());
    },
    assert!(!lock_exists()),
    true
);

#[test]
fn test_max_commit_ts_error() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();
    let cm = storage.get_concurrency_manager();

    fail::cfg("after_prewrite_one_key", "sleep(500)").unwrap();
    let (prewrite_tx, prewrite_rx) = channel();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![
                    Mutation::make_put(Key::from_raw(b"k1"), b"v".to_vec()),
                    Mutation::make_put(Key::from_raw(b"k2"), b"v".to_vec()),
                ],
                b"k1".to_vec(),
                10.into(),
                20000,
                false,
                2,
                TimeStamp::default(),
                100.into(),
                Some(vec![b"k2".to_vec()]),
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    thread::sleep(Duration::from_millis(200));
    cm.read_key_check(&Key::from_raw(b"k1"), |_| Err(()))
        .unwrap_err();
    let _ = cm.update_max_ts(200.into());

    let res = prewrite_rx.recv().unwrap().unwrap();
    assert!(res.min_commit_ts.is_zero());
    assert!(res.one_pc_commit_ts.is_zero());

    // There should not be any memory lock left.
    cm.read_range_check(None, None, |_, _| Err(())).unwrap();

    // Two locks should be written, the second one does not async commit.
    let l1 = must_locked(&mut storage.get_engine(), b"k1", 10);
    let l2 = must_locked(&mut storage.get_engine(), b"k2", 10);
    assert!(l1.use_async_commit);
    assert!(!l2.use_async_commit);
}

#[test]
fn test_exceed_max_commit_ts_in_the_middle_of_prewrite() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();
    let cm = storage.get_concurrency_manager();

    let (prewrite_tx, prewrite_rx) = channel();
    // Pause between getting max ts and store the lock in memory
    fail::cfg("before-set-lock-in-memory", "pause").unwrap();

    let _ = cm.update_max_ts(40.into());
    let mutations = vec![
        Mutation::make_put(Key::from_raw(b"k1"), b"v".to_vec()),
        Mutation::make_put(Key::from_raw(b"k2"), b"v".to_vec()),
    ];
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                mutations.clone(),
                b"k1".to_vec(),
                10.into(),
                20000,
                false,
                2,
                11.into(),
                50.into(),
                Some(vec![]),
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    // sleep a while so the first key gets max ts.
    thread::sleep(Duration::from_millis(200));

    let _ = cm.update_max_ts(51.into());
    fail::remove("before-set-lock-in-memory");
    let res = prewrite_rx.recv().unwrap().unwrap();
    assert!(res.min_commit_ts.is_zero());
    assert!(res.one_pc_commit_ts.is_zero());

    let locks = block_on(storage.scan_lock(
        Context::default(),
        20.into(),
        Some(Key::from_raw(b"k1")),
        None,
        2,
    ))
    .unwrap();
    assert_eq!(locks.len(), 2);
    assert_eq!(locks[0].get_key(), b"k1");
    assert!(locks[0].get_use_async_commit());
    assert_eq!(locks[0].get_min_commit_ts(), 41);
    assert_eq!(locks[1].get_key(), b"k2");
    assert!(!locks[1].get_use_async_commit());

    // Send a duplicated request to test the idempotency of prewrite when falling
    // back to 2PC.
    let (prewrite_tx, prewrite_rx) = channel();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                mutations,
                b"k1".to_vec(),
                10.into(),
                20000,
                false,
                2,
                11.into(),
                50.into(),
                Some(vec![]),
                false,
                AssertionLevel::Off,
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    let res = prewrite_rx.recv().unwrap().unwrap();
    assert!(res.min_commit_ts.is_zero());
    assert!(res.one_pc_commit_ts.is_zero());
}

#[test]
fn test_pessimistic_lock_check_epoch() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();

    let epoch = cluster.get_region_epoch(region.id);
    let mut ctx = Context::default();
    ctx.set_region_id(region.id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(epoch);

    let (fp_tx, fp_rx) = sync_channel(0);
    fail::cfg_callback("acquire_pessimistic_lock", move || {
        fp_tx.send(()).unwrap();
    })
    .unwrap();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

    let mut mutation = pb::Mutation::default();
    mutation.set_op(Op::PessimisticLock);
    mutation.key = b"key".to_vec();
    let mut req = PessimisticLockRequest::default();
    req.set_context(ctx.clone());
    req.set_mutations(vec![mutation].into());
    req.set_start_version(10);
    req.set_for_update_ts(10);
    req.set_primary_lock(b"key".to_vec());

    let lock_resp = thread::spawn(move || client.kv_pessimistic_lock(&req).unwrap());
    thread::sleep(Duration::from_millis(300));

    // Transfer leader out and back, so the term should have changed.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fp_rx.recv().unwrap();

    let resp = lock_resp.join().unwrap();
    // Region leader changes, so we should get a StaleCommand error.
    assert!(resp.get_region_error().has_stale_command());
}

#[test]
fn test_pessimistic_lock_check_valid() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    let txn_ext = cluster
        .must_get_snapshot_of_region(1)
        .ext()
        .get_txn_ext()
        .unwrap()
        .clone();

    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();
    fail::cfg("acquire_pessimistic_lock", "pause").unwrap();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

    let mut mutation = pb::Mutation::default();
    mutation.set_op(Op::PessimisticLock);
    mutation.key = b"key".to_vec();
    let mut req = PessimisticLockRequest::default();
    req.set_context(ctx.clone());
    req.set_mutations(vec![mutation].into());
    req.set_start_version(10);
    req.set_for_update_ts(10);
    req.set_primary_lock(b"key".to_vec());

    let lock_resp = thread::spawn(move || client.kv_pessimistic_lock(&req).unwrap());
    thread::sleep(Duration::from_millis(300));
    // Set `status` to `TransferringLeader` to make the locks table not writable,
    // but the region remains available to serve.
    txn_ext.pessimistic_locks.write().status = LocksStatus::TransferringLeader;
    fail::remove("acquire_pessimistic_lock");

    let resp = lock_resp.join().unwrap();
    // There should be no region error.
    assert!(!resp.has_region_error());
    // The lock should not be written to the in-memory pessimistic lock table.
    assert!(txn_ext.pessimistic_locks.read().is_empty());
}

#[test]
fn test_concurrent_write_after_transfer_leader_invalidates_locks() {
    let peer_size_limit = 512 << 10;
    let instance_size_limit = 100 << 20;
    let mut cluster = new_server_cluster(0, 1);
    cluster.cfg.pessimistic_txn.pipelined = true;
    cluster.cfg.pessimistic_txn.in_memory = true;
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    let txn_ext = cluster
        .must_get_snapshot_of_region(1)
        .ext()
        .get_txn_ext()
        .unwrap()
        .clone();

    let lock = PessimisticLock {
        primary: b"key".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
        last_change: LastChange::make_exist(5.into(), 3),
        is_locked_with_conflict: false,
    };
    txn_ext
        .pessimistic_locks
        .write()
        .insert(
            vec![(Key::from_raw(b"key"), lock.clone())],
            peer_size_limit,
            instance_size_limit,
        )
        .unwrap();

    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();
    fail::cfg("invalidate_locks_before_transfer_leader", "pause").unwrap();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

    let mut mutation = pb::Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = b"key".to_vec();
    let mut req = PrewriteRequest::default();
    req.set_context(ctx);
    req.set_mutations(vec![mutation].into());
    // Set a different start_ts. It should fail because the memory lock is still
    // visible.
    req.set_start_version(20);
    req.set_primary_lock(b"key".to_vec());

    // Prewrite should not be blocked because we have downgrade the write lock
    // to a read lock, and it should return a locked error because it encounters
    // the memory lock.
    let resp = client.kv_prewrite(&req).unwrap();
    assert_eq!(
        resp.get_errors()[0].get_locked(),
        &lock.into_lock().into_lock_info(b"key".to_vec())
    );
}

#[test]
fn test_read_index_with_max_ts() {
    let mut cluster = new_server_cluster(0, 3);
    // Increase the election tick to make this test case running reliably.
    // Use async apply prewrite to let tikv response before applying on the leader
    // peer.
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(10_000));
    cluster.cfg.storage.enable_async_apply_prewrite = true;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let k0 = b"k0";
    let v0 = b"v0";
    let r1 = cluster.run_conf_change();
    let p2 = new_peer(2, 2);
    cluster.pd_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.pd_client.must_add_peer(r1, p3.clone());
    cluster.must_put(k0, v0);
    cluster.pd_client.must_none_pending_peer(p2.clone());
    cluster.pd_client.must_none_pending_peer(p3.clone());

    let region = cluster.get_region(k0);
    cluster.must_transfer_leader(region.get_id(), p3.clone());

    // Block all write cmd applying of Peer 3(leader), then start to write to it.
    let k1 = b"k1";
    let v1 = b"v1";
    let mut ctx_p3 = Context::default();
    ctx_p3.set_region_id(region.get_id());
    ctx_p3.set_region_epoch(region.get_region_epoch().clone());
    ctx_p3.set_peer(p3.clone());
    let mut ctx_p2 = ctx_p3.clone();
    ctx_p2.set_peer(p2.clone());

    let start_ts = 10;
    let mut mutation = pb::Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k1.to_vec();
    mutation.value = v1.to_vec();
    let mut req = PrewriteRequest::default();
    req.set_context(ctx_p3);
    req.set_mutations(vec![mutation].into());
    req.set_start_version(start_ts);
    req.try_one_pc = true;
    req.set_primary_lock(k1.to_vec());

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env.clone()).connect(&cluster.sim.rl().get_addr(p3.get_store_id()));
    let client_p3 = TikvClient::new(channel);
    fail::cfg("on_apply_write_cmd", "sleep(2000)").unwrap();
    client_p3.kv_prewrite(&req).unwrap();

    // The apply is blocked on leader, so the read index request with max ts should
    // see the memory lock as it would be dropped after finishing apply.
    let channel = ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(p2.get_store_id()));
    let client_p2 = TikvClient::new(channel);
    let mut req = GetRequest::new();
    req.key = k1.to_vec();
    req.version = u64::MAX;
    ctx_p2.replica_read = true;
    req.set_context(ctx_p2);
    let resp = client_p2.kv_get(&req).unwrap();
    assert!(resp.region_error.is_none());
    assert_eq!(resp.error.unwrap().locked.unwrap().lock_version, start_ts);
    fail::remove("on_apply_write_cmd");
}

// This test mocks the situation described in the PR#14863
#[test]
fn test_proposal_concurrent_with_conf_change_and_transfer_leader() {
    let (mut cluster, _, mut ctx) = test_raftstore_v2::must_new_cluster_mul(4);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    pd_client.add_peer(1, new_learner_peer(4, 4));

    std::thread::sleep(Duration::from_millis(500));

    pd_client.joint_confchange(
        1,
        vec![
            (ConfChangeType::AddNode, new_peer(4, 4)),
            (ConfChangeType::AddLearnerNode, new_learner_peer(1, 1)),
        ],
    );

    std::thread::sleep(Duration::from_millis(500));

    let leader = cluster.leader_of_region(1).unwrap();
    let epoch = cluster.get_region_epoch(1);
    ctx.set_region_id(1);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(epoch);

    let env = Arc::new(Environment::new(1));
    let ch = ChannelBuilder::new(env)
        .connect(&cluster.sim.read().unwrap().get_addr(leader.get_store_id()));
    let client = TikvClient::new(ch);

    cluster.add_send_filter_on_node(
        1,
        Box::new(DropMessageFilter::new(Arc::new(move |m| {
            let msg_type = m.get_message().get_msg_type();
            let to_store = m.get_to_peer().get_store_id();
            !(msg_type == MessageType::MsgAppend && (to_store == 2 || to_store == 3))
        }))),
    );

    cluster.add_send_filter_on_node(
        4,
        Box::new(DropMessageFilter::new(Arc::new(move |m| {
            let msg_type = m.get_message().get_msg_type();
            let to_store = m.get_to_peer().get_store_id();
            !(msg_type == MessageType::MsgAppend && to_store == 1)
        }))),
    );

    let (tx, rx) = channel::<()>();
    let tx = Arc::new(Mutex::new(tx));
    // ensure the cmd is proposed before transfer leader
    fail::cfg_callback("after_propose_pending_writes", move || {
        tx.lock().unwrap().send(()).unwrap();
    })
    .unwrap();

    let handle = std::thread::spawn(move || {
        let mut mutations = vec![];
        for key in [b"key3".to_vec(), b"key4".to_vec()] {
            let mut mutation = kvproto::kvrpcpb::Mutation::default();
            mutation.set_op(Op::Put);
            mutation.set_key(key);
            mutations.push(mutation);
        }
        let _ = try_kv_prewrite(&client, ctx, mutations, b"key3".to_vec(), 10);
    });

    rx.recv_timeout(std::time::Duration::from_secs(50)).unwrap();
    pd_client.transfer_leader(1, new_peer(4, 4), vec![]);

    pd_client.region_leader_must_be(1, new_incoming_voter(4, 4));
    pd_client.must_leave_joint(1);

    pd_client.must_joint_confchange(
        1,
        vec![(ConfChangeType::RemoveNode, new_learner_peer(1, 1))],
    );
    pd_client.must_leave_joint(1);

    cluster.clear_send_filter_on_node(1);
    cluster.clear_send_filter_on_node(4);

    handle.join().unwrap();
}

#[test]
fn test_next_last_change_info_called_when_gc() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let k = b"zk";

    must_prewrite_put(&mut engine, k, b"v", k, 5);
    must_commit(&mut engine, k, 5, 6);

    must_rollback(&mut engine, k, 10, true);

    fail::cfg("before_get_write_in_next_last_change_info", "pause").unwrap();

    let mut engine2 = engine.clone();
    let h = thread::spawn(move || {
        must_acquire_pessimistic_lock_return_value(&mut engine2, k, k, 30, 30, false)
    });
    thread::sleep(Duration::from_millis(200));
    assert!(!h.is_finished());

    gc_by_compact(&mut engine, &[], 20);

    fail::remove("before_get_write_in_next_last_change_info");

    assert_eq!(h.join().unwrap().unwrap().as_slice(), b"v");
}

fn must_put<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) {
    engine.put(ctx, Key::from_raw(key), value.to_vec()).unwrap();
}

fn must_delete<E: Engine>(ctx: &Context, engine: &E, key: &[u8]) {
    engine.delete(ctx, Key::from_raw(key)).unwrap();
}

// Before the fix, a proposal can be proposed twice, which is caused by that
// write proposal validation and propose are not atomic. So a raft message with
// higher term between them can make the proposal goes to msg proposal
// forwarding logic. However, raft proposal forawrd logic is not compatible with
// the raft store, as the failed proposal makes client retry. The retried
// proposal coupled with forward proposal makes the propsal applied twice.
#[test]
fn test_forbid_forward_propose() {
    use test_raftstore_v2::*;
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.run();

    let region = cluster.get_region(b"");
    let peer1 = new_peer(1, 1);
    let peer2 = new_peer(2, 2);
    cluster.must_transfer_leader(region.id, peer2.clone());
    let storage = cluster.sim.rl().storages[&1].clone();
    let storage2 = cluster.sim.rl().storages[&2].clone();

    let p = Arc::new(AtomicBool::new(false));
    let p2 = p.clone();
    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    cluster.add_recv_filter_on_node(
        2,
        Box::new(DropMessageFilter::new(Arc::new(move |_| {
            if p2.load(Ordering::Relaxed) {
                tx.lock().unwrap().send(()).unwrap();
                // One msg is enough
                p2.store(false, Ordering::Relaxed);
                true
            } else {
                false
            }
        }))),
    );

    let k = Key::from_raw(b"k");
    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(peer2);

    // block node when collecting message to make async write proposal and a raft
    // message with higher term occured in a single batch.
    fail::cfg("on_peer_collect_message_2", "pause").unwrap();
    let mut res = storage2.async_write(
        &ctx,
        WriteData::from_modifies(vec![Modify::Put(CF_DEFAULT, k.clone(), b"val".to_vec())]),
        WriteEvent::EVENT_PROPOSED,
        None,
    );

    // Make node 1 become leader
    let router = cluster.get_router(1).unwrap();
    let mut raft_msg = RaftMessage::default();
    raft_msg.set_region_id(1);
    raft_msg.set_to_peer(peer1.clone());
    raft_msg.set_region_epoch(region.get_region_epoch().clone());
    raft_msg
        .mut_message()
        .set_msg_type(MessageType::MsgTimeoutNow);
    router.send_raft_message(Box::new(raft_msg)).unwrap();

    std::thread::sleep(Duration::from_secs(1));

    ctx.set_peer(peer1);
    must_put(&ctx, &storage, b"k", b"val");
    must_delete(&ctx, &storage, b"k");

    p.store(true, Ordering::Release);
    rx.recv().unwrap();
    // Ensure the msg is sent by router.
    std::thread::sleep(Duration::from_millis(100));
    fail::remove("on_peer_collect_message_2");

    let r = block_on(async { res.next().await }).unwrap();
    assert!(matches!(r, WriteEvent::Finished(Err { .. })));

    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(cluster.get(k.as_encoded()), None);
}

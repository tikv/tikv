// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc::channel, Arc},
    thread,
    time::Duration,
};

use futures::executor::block_on;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{self as pb, AssertionLevel, Context, Op, PessimisticLockRequest, PrewriteRequest},
    tikvpb::TikvClient,
};
use raftstore::store::{util::new_peer, LocksStatus};
use storage::{
    mvcc::{
        self,
        tests::{must_get, must_locked},
    },
    txn::{self, commands},
};
use test_raftstore::new_server_cluster;
use tikv::storage::{
    self,
    kv::SnapshotExt,
    lock_manager::DummyLockManager,
    txn::tests::{
        must_acquire_pessimistic_lock, must_commit, must_pessimistic_prewrite_put,
        must_pessimistic_prewrite_put_err, must_prewrite_put, must_prewrite_put_err,
    },
    Snapshot, TestEngineBuilder, TestStorageBuilderApiV1,
};
use tikv_util::HandyRwLock;
use txn_types::{Key, Mutation, PessimisticLock, TimeStamp};

#[test]
fn test_txn_failpoints() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let (k, v) = (b"k", b"v");
    fail::cfg("prewrite", "return(WriteConflict)").unwrap();
    must_prewrite_put_err(&engine, k, v, k, 10);
    fail::remove("prewrite");
    must_prewrite_put(&engine, k, v, k, 10);
    fail::cfg("commit", "delay(100)").unwrap();
    must_commit(&engine, k, 10, 20);
    fail::remove("commit");

    let v1 = b"v1";
    let (k2, v2) = (b"k2", b"v2");
    must_acquire_pessimistic_lock(&engine, k, k, 30, 30);
    fail::cfg("pessimistic_prewrite", "return()").unwrap();
    must_pessimistic_prewrite_put_err(&engine, k, v1, k, 30, 30, true);
    must_prewrite_put(&engine, k2, v2, k2, 31);
    fail::remove("pessimistic_prewrite");
    must_pessimistic_prewrite_put(&engine, k, v1, k, 30, 30, true);
    must_commit(&engine, k, 30, 40);
    must_commit(&engine, k2, 31, 41);
    must_get(&engine, k, 50, v1);
    must_get(&engine, k2, 50, v2);
}

#[test]
fn test_atomic_getting_max_ts_and_storing_memory_lock() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, DummyLockManager)
        .build()
        .unwrap();

    let (prewrite_tx, prewrite_rx) = channel();
    // sleep a while between getting max ts and store the lock in memory
    fail::cfg("before-set-lock-in-memory", "sleep(500)").unwrap();
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
    // sleep a while so prewrite gets max ts before get is triggered
    thread::sleep(Duration::from_millis(200));
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
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, DummyLockManager)
        .build()
        .unwrap();

    // Suppose snapshot was before updating max_ts, after sleeping for 500ms the following prewrite should complete.
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
    // We must make sure either the lock is visible to the reader or min_commit_ts > read_ts.
    assert!(res.min_commit_ts > read_ts || has_lock);
}

#[test]
fn test_update_max_ts_before_scan_memory_locks() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, DummyLockManager)
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

/// Generates a test that checks the correct behavior of holding and dropping locks,
/// during the process of a single prewrite command.
macro_rules! lock_release_test {
    ($test_name:ident, $lock_exists:ident, $before_actions:expr, $middle_actions:expr, $after_actions:expr, $should_succeed:expr) => {
        #[test]
        fn $test_name() {
            let engine = TestEngineBuilder::new().build().unwrap();
            let storage =
                TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, DummyLockManager)
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

// Must hold lock until prewrite ends. Must release lock after prewrite succeeds.
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
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, DummyLockManager)
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
    assert!(
        cm.read_key_check(&Key::from_raw(b"k1"), |_| Err(()))
            .is_err()
    );
    cm.update_max_ts(200.into());

    let res = prewrite_rx.recv().unwrap().unwrap();
    assert!(res.min_commit_ts.is_zero());
    assert!(res.one_pc_commit_ts.is_zero());

    // There should not be any memory lock left.
    assert!(cm.read_range_check(None, None, |_, _| Err(())).is_ok());

    // Two locks should be written, the second one does not async commit.
    let l1 = must_locked(&storage.get_engine(), b"k1", 10);
    let l2 = must_locked(&storage.get_engine(), b"k2", 10);
    assert!(l1.use_async_commit);
    assert!(!l2.use_async_commit);
}

#[test]
fn test_exceed_max_commit_ts_in_the_middle_of_prewrite() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, DummyLockManager)
        .build()
        .unwrap();
    let cm = storage.get_concurrency_manager();

    let (prewrite_tx, prewrite_rx) = channel();
    // Pause between getting max ts and store the lock in memory
    fail::cfg("before-set-lock-in-memory", "pause").unwrap();

    cm.update_max_ts(40.into());
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

    cm.update_max_ts(51.into());
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

    // Send a duplicated request to test the idempotency of prewrite when falling back to 2PC.
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

    // Transfer leader out and back, so the term should have changed.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));
    fail::remove("acquire_pessimistic_lock");

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
    };
    assert!(
        txn_ext
            .pessimistic_locks
            .write()
            .insert(vec![(Key::from_raw(b"key"), lock.clone())])
            .is_ok()
    );

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
    // Set a different start_ts. It should fail because the memory lock is still visible.
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

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use kvproto::kvrpcpb::Context;
use std::{sync::mpsc::channel, thread, time::Duration};
use storage::mvcc::{self, tests::must_locked};
use tikv::storage::txn::commands;
use tikv::storage::txn::tests::{must_commit, must_prewrite_put, must_prewrite_put_err};
use tikv::storage::{self, lock_manager::DummyLockManager, TestEngineBuilder, TestStorageBuilder};
use txn_types::{Key, Mutation, TimeStamp};

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
}

#[test]
fn test_atomic_getting_max_ts_and_storing_memory_lock() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
        engine,
        DummyLockManager {},
    )
    .build()
    .unwrap();

    let (prewrite_tx, prewrite_rx) = channel();
    // sleep a while between getting max ts and store the lock in memory
    fail::cfg("before-set-lock-in-memory", "sleep(500)").unwrap();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
                b"k".to_vec(),
                40.into(),
                20000,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
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
        Err(storage::Error(box storage::ErrorInner::Mvcc(mvcc::Error(
            box mvcc::ErrorInner::KeyIsLocked(lock),
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
    let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
        engine,
        DummyLockManager {},
    )
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
                vec![Mutation::Put((Key::from_raw(b"j"), b"v".to_vec()))],
                b"j".to_vec(),
                10.into(),
                20000,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
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
    let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
        engine,
        DummyLockManager {},
    )
    .build()
    .unwrap();

    fail::cfg("before-storage-check-memory-locks", "sleep(500)").unwrap();
    let get_fut = storage.get(Context::default(), Key::from_raw(b"k"), 100.into());

    thread::sleep(Duration::from_millis(200));

    let (prewrite_tx, prewrite_rx) = channel();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
                b"k".to_vec(),
                10.into(),
                20000,
                false,
                1,
                TimeStamp::default(),
                TimeStamp::default(),
                Some(vec![]),
                false,
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
            let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
                engine,
                DummyLockManager {},
            )
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
                        vec![Mutation::Put((key.clone(), b"v".to_vec()))],
                        b"k".to_vec(),
                        10.into(),
                        20000,
                        false,
                        1,
                        TimeStamp::default(),
                        TimeStamp::default(),
                        Some(vec![]),
                        false,
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
    let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
        engine,
        DummyLockManager {},
    )
    .build()
    .unwrap();
    let cm = storage.get_concurrency_manager();

    fail::cfg("after_prewrite_one_key", "sleep(500)").unwrap();
    let (prewrite_tx, prewrite_rx) = channel();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![
                    Mutation::Put((Key::from_raw(b"k1"), b"v".to_vec())),
                    Mutation::Put((Key::from_raw(b"k2"), b"v".to_vec())),
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
                Context::default(),
            ),
            Box::new(move |res| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    thread::sleep(Duration::from_millis(200));
    assert!(cm
        .read_key_check(&Key::from_raw(b"k1"), |_| Err(()))
        .is_err());
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

// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Mutex,
        mpsc::{self, Receiver},
    },
    thread,
    time::Duration,
};

use kvproto::kvrpcpb::LockInfo;
use tikv::storage::{
    Error, ErrorInner, PessimisticLockKeyResult, PessimisticLockParameters, PessimisticLockResults,
    Result, StorageCallback,
    lock_manager::{
        LockManager, MockLockManager,
        lock_wait_context::LockWaitContext,
        lock_waiting_queue::{LockWaitEntry, LockWaitQueues},
    },
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
    txn::{Error as TxnError, ErrorInner as TxnErrorInner},
};
use txn_types::Key;

fn make_waiter(
    mgr: &MockLockManager,
    queues: &LockWaitQueues<MockLockManager>,
    start_ts: u64,
) -> (
    LockWaitContext<MockLockManager>,
    Box<LockWaitEntry>,
    Receiver<Result<Result<PessimisticLockResults>>>,
) {
    let key = Key::from_raw(b"cancel-requeue");
    let token = mgr.allocate_token();
    let (tx, rx) = mpsc::channel();
    let cb = StorageCallback::PessimisticLock(Box::new(move |res| tx.send(res).unwrap()));
    let ctx = LockWaitContext::new(key.clone(), queues.clone(), token, cb, true);
    let entry = Box::new(LockWaitEntry {
        key: key.clone(),
        lock_hash: key.gen_hash(),
        parameters: PessimisticLockParameters {
            pb_ctx: Default::default(),
            primary: b"cancel-requeue".to_vec(),
            start_ts: start_ts.into(),
            lock_ttl: 1000,
            for_update_ts: start_ts.into(),
            wait_timeout: None,
            return_values: false,
            min_commit_ts: 0.into(),
            check_existence: false,
            is_first_lock: false,
            lock_only_if_exists: false,
            allow_lock_with_conflict: true,
        },
        should_not_exist: false,
        is_shared_lock: false,
        lock_wait_token: token,
        req_states: ctx.get_shared_states().clone(),
        legacy_wake_up_index: None,
        key_cb: Some(ctx.get_callback_for_blocked_key().into()),
    });
    (ctx, entry, rx)
}

#[derive(Clone, Copy)]
enum CancelOrder {
    BeforeRequeue,
    DuringRequeue,
    AfterRequeue,
}

fn check_cancel_requeue(order: CancelOrder) {
    for occupied in [false, true] {
        for key_is_locked in [true, false] {
            let mgr = MockLockManager::new();
            let queues = LockWaitQueues::new(mgr.clone());
            let (ctx, entry, result_rx) = make_waiter(&mgr, &queues, 2);
            let key = entry.key.clone();
            let token = entry.lock_wait_token;
            let mut original_lock = LockInfo::default();
            original_lock.set_key(b"cancel-requeue".to_vec());
            original_lock.set_lock_version(1);
            queues.push_lock_wait(entry, original_lock.clone());
            let (mut entries, delayed) = queues.pop_for_waking_up(&key, 1.into(), 3.into(), 0);
            assert_eq!(entries.len(), 1);
            assert!(delayed.is_none());
            assert!(queues.is_empty());
            let entry = entries.pop().unwrap();

            // Keep another waiter on this key to cover Occupied as well as Vacant.
            let mut latest_lock = original_lock.clone();
            latest_lock.set_lock_version(3);
            let other = occupied.then(|| {
                let (ctx, entry, rx) = make_waiter(&mgr, &queues, 4);
                let token = entry.lock_wait_token;
                queues.push_lock_wait(entry, latest_lock.clone());
                (ctx, token, rx)
            });
            let error = if key_is_locked {
                Error::from(TxnError::from(MvccError::from(
                    MvccErrorInner::KeyIsLocked(original_lock.clone()),
                )))
            } else {
                Error::from(ErrorInner::SchedTooBusy)
            };
            let cancel = ctx.get_callback_for_cancellation();
            let mut threads = Vec::new();
            match order {
                CancelOrder::BeforeRequeue => {
                    cancel(error);
                    queues.push_lock_wait(entry, LockInfo::default());
                }
                CancelOrder::DuringRequeue => {
                    let (marked_tx, marked_rx) = mpsc::channel();
                    let (allow_remove_tx, allow_remove_rx) = mpsc::channel();
                    let allow_remove_rx = Mutex::new(allow_remove_rx);
                    let (entry_held_tx, entry_held_rx) = mpsc::channel();
                    fail::cfg_callback("lock_wait_context_after_cancel_marked", move || {
                        marked_tx.send(()).unwrap();
                        allow_remove_rx
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(10))
                            .unwrap();
                    })
                    .unwrap();
                    fail::cfg_callback("lock_wait_queue_before_canceled_error", move || {
                        entry_held_tx.send(()).unwrap();
                    })
                    .unwrap();
                    threads.push(thread::spawn(move || cancel(error)));
                    marked_rx.recv_timeout(Duration::from_secs(3)).unwrap();
                    let requeue_queues = queues.clone();
                    threads.push(thread::spawn(move || {
                        requeue_queues.push_lock_wait(entry, LockInfo::default());
                    }));
                    // Cancellation cannot remove the entry until requeue has observed
                    // the flag while holding the shard lock.
                    entry_held_rx.recv_timeout(Duration::from_secs(3)).unwrap();
                    allow_remove_tx.send(()).unwrap();
                }
                CancelOrder::AfterRequeue => {
                    queues.push_lock_wait(entry, LockInfo::default());
                    cancel(error);
                }
            }
            let mut results = result_rx
                .recv_timeout(Duration::from_secs(3))
                .expect("cancellation and requeue must complete without waiting on each other")
                .unwrap()
                .unwrap();
            assert_eq!(results.0.len(), 1);
            let PessimisticLockKeyResult::Failed(error) = results.0.pop().unwrap() else {
                panic!("expected cancellation error");
            };
            if key_is_locked {
                let ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                    box MvccErrorInner::KeyIsLocked(info),
                )))) = error.inner()
                else {
                    panic!("expected KeyIsLocked, got {error:?}");
                };
                let expected = if occupied && !matches!(order, CancelOrder::AfterRequeue) {
                    &latest_lock
                } else {
                    &original_lock
                };
                assert_eq!(info, expected);
            } else {
                assert!(matches!(error.inner(), ErrorInner::SchedTooBusy));
            }
            for thread in threads {
                thread.join().unwrap();
            }
            fail::remove("lock_wait_context_after_cancel_marked");
            fail::remove("lock_wait_queue_before_canceled_error");
            // The callback has been consumed, and the canceled token was not requeued.
            assert!(matches!(
                result_rx.try_recv(),
                Err(mpsc::TryRecvError::Disconnected)
            ));
            assert!(queues.remove_by_token(&key, token).is_none());
            assert_eq!(queues.entry_count(), usize::from(occupied));
            if let Some((other_ctx, other_token, other_rx)) = other {
                assert!(matches!(
                    other_rx.try_recv(),
                    Err(mpsc::TryRecvError::Empty)
                ));
                let other_entry = queues.remove_by_token(&key, other_token).unwrap();
                assert_eq!(other_entry.parameters.start_ts, 4.into());
                drop((other_ctx, other_entry));
            }
            assert!(queues.is_empty());
        }
    }
}

#[test]
fn test_cancel_during_resumable_requeue() {
    check_cancel_requeue(CancelOrder::DuringRequeue);
}

#[test]
fn test_cancel_before_resumable_requeue() {
    check_cancel_requeue(CancelOrder::BeforeRequeue);
}

#[test]
fn test_cancel_after_resumable_requeue() {
    check_cancel_requeue(CancelOrder::AfterRequeue);
}

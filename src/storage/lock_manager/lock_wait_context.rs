// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! Holds the state of a lock-waiting `AcquirePessimisticLock` request.
//!
//! When an `AcquirePessimisticLock` request meets a lock and enters
//! lock-waiting state, it then may be either woken up by popping from the
//! [`LockWaitingQueue`](super::lock_waiting_queue::LockWaitQueues),
//! or cancelled by the
//! [`WaiterManager`](crate::server::lock_manager::WaiterManager) due to
//! timeout. [`LockWaitContext`] is therefore used to share the necessary state
//! of a single `AcquirePessimisticLock` request, and ensuring the internal
//! callback for returning response through RPC is called at most only once.

use std::{
    convert::TryInto,
    result::Result,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
};

use parking_lot::Mutex;
use txn_types::Key;

use crate::storage::{
    errors::SharedError,
    lock_manager::{lock_waiting_queue::LockWaitQueues, LockManager, LockWaitToken},
    types::PessimisticLockKeyResult,
    Error as StorageError, PessimisticLockResults, ProcessResult, StorageCallback,
};

// The arguments are: (result, is_canceled_before_enqueueing).
pub type PessimisticLockKeyCallback =
    Box<dyn FnOnce(Result<PessimisticLockKeyResult, SharedError>, bool) + Send + 'static>;
pub type CancellationCallback = Box<dyn FnOnce(StorageError) + Send + 'static>;

pub struct LockWaitContextInner {
    /// The callback for finishing the current AcquirePessimisticLock request.
    /// Usually, requests are accepted from RPC, and in this case calling
    /// the callback means returning the response to the client via RPC.
    cb: StorageCallback,
}

/// The content of the `LockWaitContext` that needs to be shared among all
/// clones.
///
/// When a AcquirePessimisticLock request meets lock and enters lock waiting
/// state, a `LockWaitContext` will be created, and the
/// `LockWaitContextSharedState` will be shared in these places:
/// * Callbacks created from the `lockWaitContext` and distributed to the lock
///   waiting queue and the `LockManager`. When one of the callbacks is called
///   and the request is going to be finished, they need to take the
///   [`LockWaitContextInner`] to call the callback.
/// * The [`LockWaitEntry`](crate::storage::lock_manager::lock_waiting_queue::LockWaitEntry), for
///   providing information
pub struct LockWaitContextSharedState {
    ctx_inner: Mutex<Option<LockWaitContextInner>>,

    /// The token to identify the waiter.
    lock_wait_token: LockWaitToken,

    /// The key on which lock waiting occurs.
    key: Key,

    /// When a lock-waiting request (allow_lock_with_conflict == true) is
    /// resumed, it's theoretically possible that the request meets lock
    /// again, therefore it may need to be pushed to the lock waiting queue
    /// again. Since the request is popped out from the queue when resuming
    /// (which means the lock wait entry doesn't exist in the lock waiting
    /// queue during the resumed execution), it's possible that timeout or
    /// deadlock happens from `WaiterManager` during that time, which will
    /// try to cancel the request. Therefore it leads to such a corner case:
    ///
    /// 1. (scheduler) A request enters lock waiting state, so an entry is
    ///   pushed to the `LockWaitQueues`, and a    message is sent to
    ///   `LockManager`.
    /// 2. (scheduler) After a while the entry is popped out and resumed
    ///   from the `LockWaitQueues`.
    /// 3. (scheduler) The request resumes execution but still finds lock
    ///   on the key.
    ///     * This is possible to be caused by delayed-waking up or encountering
    ///       error when writing a lock-releasing command to the engine.
    /// 4. (lock_manager) At the same time, `LockManager` tries to cancel
    ///   the request due to timeout. But when    calling `finish_request`,
    ///   the entry cannot be found from the `LockWaitQueues`. So it
    ///   believes that the entry is already popped out and resumed and does
    ///   nothing.
    /// 5. (scheduler) An entry is pushed to the `LockWaitQueues` due to
    ///   encountering lock at step 3. 6. Then the request becomes unable to
    ///   be canceled by timeout or other possible errors. In worst cases,
    ///   the request may stuck in TiKV forever.
    ///
    /// To solve this problem, a `is_canceled` flag should be set when
    /// `LockManager` tries to cancel it, before accessing the
    /// `LockWaitQueues`; when an entry is pushed to the `LockWaitQueues`,
    /// check if `is_canceled` is set after locking its inner map (ensures
    /// exclusive access with `LockManager`), and if it's set, cancel the
    /// request like how `LockManager` should have done.
    ///
    /// The request should be canceled with the error that occurs in
    /// `LockManager`. `external_error_tx` and `external_error_rx` are used
    /// to pass this error in this case.
    ///
    /// `is_canceled` marks if the request is canceled from outside. Usually
    /// this is caused by timeout or deadlock detected. When this flag is
    /// marked true, the request must not be put into the lock waiting queue
    /// since nobody will wake it up for timeout and it may stuck forever.
    is_canceled: AtomicBool,

    /// The sender for passing errors in some cancellation cases. See comments
    /// in [`is_canceled`](LockWaitContextSharedState::is_canceled) for details.
    /// It's only possible to be used in `LockManager`, so there's no contention
    /// on the mutex.
    external_error_tx: Mutex<Option<mpsc::Sender<StorageError>>>,

    /// The sender for passing errors in some cancellation cases. See comments
    /// in [`is_canceled`](LockWaitContextSharedState::is_canceled) for details.
    /// It's only possible to be used when scheduler tries to push to
    /// `LockWaitQueues`, so there's no contention on the mutex.
    external_error_rx: Mutex<Option<mpsc::Receiver<StorageError>>>,
}

impl LockWaitContextSharedState {
    fn new(lock_wait_token: LockWaitToken, key: Key, cb: StorageCallback) -> Self {
        let inner = LockWaitContextInner { cb };
        let (tx, rx) = mpsc::channel();
        Self {
            ctx_inner: Mutex::new(Some(inner)),
            key,
            lock_wait_token,
            is_canceled: AtomicBool::new(false),
            external_error_tx: Mutex::new(Some(tx)),
            external_error_rx: Mutex::new(Some(rx)),
        }
    }

    #[cfg(test)]
    pub fn new_dummy(lock_wait_token: LockWaitToken, key: Key) -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            ctx_inner: Mutex::new(None),
            key,
            lock_wait_token,
            is_canceled: AtomicBool::new(false),
            external_error_tx: Mutex::new(Some(tx)),
            external_error_rx: Mutex::new(Some(rx)),
        }
    }

    pub fn is_canceled(&self) -> bool {
        self.is_canceled.load(Ordering::Acquire)
    }

    /// Gets the external error. It's assumed that the external error must have
    /// been set and consumes it. This function is expected to be called at
    /// most only once. Only used to handle the case that cancelling and
    /// resuming happens concurrently.
    pub(in crate::storage) fn get_external_error(&self) -> StorageError {
        self.external_error_rx
            .lock()
            .take()
            .unwrap()
            .recv()
            .unwrap()
    }

    /// Stores the external error. This function is expected to be called at
    /// most only once. Only used to handle the case that cancelling and
    /// resuming happens concurrently.
    fn put_external_error(&self, error: StorageError) {
        if let Err(e) = self.external_error_tx.lock().take().unwrap().send(error) {
            debug!("failed to set external error"; "err" => ?e);
        }
    }
}

enum FinishRequestKind {
    Executed,
    Canceled,
    CanceledBeforeEnqueueing,
}

#[derive(Clone)]
pub struct LockWaitContext<L: LockManager> {
    shared_states: Arc<LockWaitContextSharedState>,
    lock_wait_queues: LockWaitQueues<L>,
    allow_lock_with_conflict: bool,
}

impl<L: LockManager> LockWaitContext<L> {
    pub fn new(
        key: Key,
        lock_wait_queues: LockWaitQueues<L>,
        lock_wait_token: LockWaitToken,
        cb: StorageCallback,
        allow_lock_with_conflict: bool,
    ) -> Self {
        Self {
            shared_states: Arc::new(LockWaitContextSharedState::new(lock_wait_token, key, cb)),
            lock_wait_queues,
            allow_lock_with_conflict,
        }
    }

    pub fn get_shared_states(&self) -> &Arc<LockWaitContextSharedState> {
        &self.shared_states
    }

    /// Get the callback that should be invoked when finishes executing the
    /// scheduler command that issued the lock-waiting.
    ///
    /// When we support partially finishing a pessimistic lock request (i.e.
    /// when acquiring lock multiple keys in one single request, allowing
    /// some keys to be locked successfully while the others are blocked or
    /// failed), this will be useful for handling the result of the first
    /// write batch. But currently, the first write batch of a lock-waiting
    /// request is always empty, so the callback is just noop.
    pub fn get_callback_for_first_write_batch(&self) -> StorageCallback {
        StorageCallback::Boolean(Box::new(|res| {
            res.unwrap();
        }))
    }

    /// Get the callback that should be called when the request is woken up on a
    /// key.
    pub fn get_callback_for_blocked_key(&self) -> PessimisticLockKeyCallback {
        let ctx = self.clone();
        Box::new(move |res, is_canceled_before_enqueueing| {
            let kind = if is_canceled_before_enqueueing {
                FinishRequestKind::CanceledBeforeEnqueueing
            } else {
                FinishRequestKind::Executed
            };
            ctx.finish_request(res, kind);
        })
    }

    /// Get the callback that's used to cancel a lock-waiting request. Usually
    /// called by
    /// [`WaiterManager`](crate::server::lock_manager::WaiterManager) due to
    /// timeout.
    ///
    /// This function is assumed to be called when the lock-waiting request is
    /// queueing but canceled outside, so it includes an operation to actively
    /// remove the entry from the lock waiting queue.
    pub fn get_callback_for_cancellation(&self) -> CancellationCallback {
        let ctx = self.clone();
        Box::new(move |e| {
            ctx.finish_request(Err(e.into()), FinishRequestKind::Canceled);
        })
    }

    fn finish_request(
        &self,
        result: Result<PessimisticLockKeyResult, SharedError>,
        finish_kind: FinishRequestKind,
    ) {
        match finish_kind {
            FinishRequestKind::Executed => {
                self.lock_wait_queues
                    .get_lock_mgr()
                    .remove_lock_wait(self.shared_states.lock_wait_token);
            }
            FinishRequestKind::Canceled => {
                self.shared_states
                    .is_canceled
                    .store(true, Ordering::Release);

                let entry = self
                    .lock_wait_queues
                    .remove_by_token(&self.shared_states.key, self.shared_states.lock_wait_token);
                if entry.is_none() {
                    // It's absent in the queue infers that it's already popped out from the queue
                    // so that it will be woken up normally. However
                    // it may still meet lock and tries to enter waiting state again. In such case,
                    // the request should be canceled. Store the error here so
                    // that it can be used for cancellation in that case, where
                    // there will be a `finish_request(None, false)` invocation).
                    self.shared_states
                        .put_external_error(result.unwrap_err().try_into().unwrap());
                    return;
                }
            }
            FinishRequestKind::CanceledBeforeEnqueueing => {}
        }

        // When this is executed, the waiter is either woken up from the queue or
        // canceled and removed from the queue. There should be no chance to try
        // to take the `ctx_inner` more than once.
        let ctx_inner = self.shared_states.ctx_inner.lock().take().unwrap();

        if !self.allow_lock_with_conflict {
            // The result must be an owned error.
            let err = result.unwrap_err().try_into().unwrap();
            ctx_inner.cb.execute(ProcessResult::Failed { err });
            return;
        }

        let key_res = match result {
            Ok(key_res) => {
                assert!(!matches!(key_res, PessimisticLockKeyResult::Waiting));
                key_res
            }
            Err(e) => PessimisticLockKeyResult::Failed(e),
        };

        let mut res = PessimisticLockResults::with_capacity(1);
        res.push(key_res);
        let pr = ProcessResult::PessimisticLockRes { res: Ok(res) };

        ctx_inner.cb.execute(pr);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        default::Default,
        sync::mpsc::{channel, Receiver},
        time::Duration,
    };

    use super::*;
    use crate::storage::{
        lock_manager::{lock_waiting_queue::LockWaitEntry, MockLockManager},
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
        txn::{Error as TxnError, ErrorInner as TxnErrorInner},
        types::PessimisticLockParameters,
        ErrorInner as StorageErrorInner, Result as StorageResult,
    };

    fn create_storage_cb() -> (
        StorageCallback,
        Receiver<StorageResult<StorageResult<PessimisticLockResults>>>,
    ) {
        let (tx, rx) = channel();
        let cb = StorageCallback::PessimisticLock(Box::new(move |r| tx.send(r).unwrap()));
        (cb, rx)
    }

    fn create_test_lock_wait_ctx(
        key: &Key,
        lock_wait_queues: &LockWaitQueues<impl LockManager>,
    ) -> (
        LockWaitToken,
        LockWaitContext<impl LockManager>,
        Receiver<StorageResult<StorageResult<PessimisticLockResults>>>,
    ) {
        let (cb, rx) = create_storage_cb();
        let token = lock_wait_queues.get_lock_mgr().allocate_token();
        let ctx = LockWaitContext::new(key.clone(), lock_wait_queues.clone(), token, cb, false);
        (token, ctx, rx)
    }

    #[test]
    fn test_lock_wait_context() {
        let write_conflict = || {
            StorageErrorInner::Txn(TxnError::from(TxnErrorInner::Mvcc(MvccError::from(
                MvccErrorInner::WriteConflict {
                    start_ts: 1.into(),
                    conflict_start_ts: 2.into(),
                    conflict_commit_ts: 2.into(),
                    key: b"k1".to_vec(),
                    primary: b"k1".to_vec(),
                    reason: kvproto::kvrpcpb::WriteConflictReason::PessimisticRetry,
                },
            ))))
        };
        let key_is_locked = || {
            StorageErrorInner::Txn(TxnError::from(TxnErrorInner::Mvcc(MvccError::from(
                MvccErrorInner::KeyIsLocked(kvproto::kvrpcpb::LockInfo::default()),
            ))))
        };

        let key = Key::from_raw(b"k");

        // TODO: Use `ProxyLockMgr` to check the correctness of the `remove_lock_wait`
        // invocation.
        let lock_wait_queues = LockWaitQueues::new(MockLockManager::new());

        let (_, ctx, rx) = create_test_lock_wait_ctx(&key, &lock_wait_queues);
        // Nothing happens currently.
        (ctx.get_callback_for_first_write_batch()).execute(ProcessResult::Res);
        rx.recv_timeout(Duration::from_millis(20)).unwrap_err();
        (ctx.get_callback_for_blocked_key())(Err(SharedError::from(write_conflict())), false);
        let res = rx.recv().unwrap().unwrap_err();
        assert!(matches!(
            &res,
            StorageError(box StorageErrorInner::Txn(TxnError(
                box TxnErrorInner::Mvcc(MvccError(box MvccErrorInner::WriteConflict { .. }))
            )))
        ));
        // The tx should be dropped.
        rx.recv().unwrap_err();
        // Nothing happens if the callback is double-called.
        (ctx.get_callback_for_cancellation())(StorageError::from(key_is_locked()));

        let (token, ctx, rx) = create_test_lock_wait_ctx(&key, &lock_wait_queues);
        // Add a corresponding entry to the lock waiting queue to test actively removing
        // the entry from the queue.
        lock_wait_queues.push_lock_wait(
            Box::new(LockWaitEntry {
                key: key.clone(),
                lock_hash: key.gen_hash(),
                parameters: PessimisticLockParameters {
                    start_ts: 1.into(),
                    for_update_ts: 1.into(),
                    ..Default::default()
                },
                should_not_exist: false,
                lock_wait_token: token,
                req_states: ctx.get_shared_states().clone(),
                legacy_wake_up_index: None,
                key_cb: None,
            }),
            kvproto::kvrpcpb::LockInfo::default(),
        );
        lock_wait_queues.must_have_next_entry(b"k", 1);
        (ctx.get_callback_for_cancellation())(StorageError::from(key_is_locked()));
        lock_wait_queues.must_not_contain_key(b"k");
        let res = rx.recv().unwrap().unwrap_err();
        assert!(matches!(
            &res,
            StorageError(box StorageErrorInner::Txn(TxnError(
                box TxnErrorInner::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked(_)))
            )))
        ));
        // Since the cancellation callback can fully execute only when it's successfully
        // removed from the lock waiting queues, it's impossible that `finish_request`
        // is called again after that.

        // The tx should be dropped.
        rx.recv().unwrap_err();
    }
}

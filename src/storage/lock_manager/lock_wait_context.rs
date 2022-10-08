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
//!
//! Note: The corresponding implementation in `WaiterManager` is not yet
//! implemented, and this mod is currently not used yet.

use std::{
    convert::TryInto,
    result::Result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use txn_types::TimeStamp;

use crate::storage::{
    errors::SharedError,
    lock_manager::{lock_waiting_queue::PessimisticLockKeyCallback, LockManager, LockWaitToken},
    Error as StorageError, PessimisticLockRes, ProcessResult, StorageCallback,
};

pub struct LockWaitContextInner {
    /// The callback for finishing the current AcquirePessimisticLock request.
    /// Usually, requests are accepted from RPC, and in this case calling
    /// the callback means returning the response to the client via RPC.
    cb: StorageCallback,

    /// The token of the corresponding waiter in `LockManager`.
    #[allow(dead_code)]
    lock_wait_token: LockWaitToken,
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
///   checking whether the request is already finished (cancelled).
pub struct LockWaitContextSharedState {
    ctx_inner: Mutex<Option<LockWaitContextInner>>,
    pub finished: AtomicBool,
}

impl LockWaitContextSharedState {
    /// Checks whether the lock-waiting request is already finished.
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub struct LockWaitContext<L: LockManager> {
    shared_states: Arc<LockWaitContextSharedState>,
    #[allow(dead_code)]
    lock_manager: L,
    allow_lock_with_conflict: bool,

    // Fields for logging:
    start_ts: TimeStamp,
    for_update_ts: TimeStamp,
}

impl<L: LockManager> LockWaitContext<L> {
    pub fn new(
        lock_manager: L,
        lock_wait_token: LockWaitToken,
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        cb: StorageCallback,
        allow_lock_with_conflict: bool,
    ) -> Self {
        let inner = LockWaitContextInner {
            cb,
            lock_wait_token,
        };
        Self {
            shared_states: Arc::new(LockWaitContextSharedState {
                ctx_inner: Mutex::new(Some(inner)),
                finished: AtomicBool::new(false),
            }),
            lock_manager,
            allow_lock_with_conflict,
            start_ts,
            for_update_ts,
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
        Box::new(move |res| {
            ctx.finish_request(res);
        })
    }

    /// Get the callback that's used to cancel a lock-waiting request. Usually
    /// called by
    /// [`WaiterManager`](crate::server::lock_manager::WaiterManager) due to
    /// timeout.
    pub fn get_callback_for_cancellation(&self) -> impl FnOnce(StorageError) {
        let ctx = self.clone();
        move |e| {
            ctx.finish_request(Err(e.into()));
        }
    }

    fn finish_request(&self, result: Result<PessimisticLockRes, SharedError>) {
        let ctx_inner = if let Some(inner) = self.shared_states.ctx_inner.lock().take() {
            inner
        } else {
            debug!("double invoking of finish_request of LockWaitContext";
                "start_ts" => self.start_ts,
                "for_update_ts" => self.for_update_ts
            );
            return;
        };

        self.shared_states.finished.store(true, Ordering::Release);

        // TODO: Uncomment this after the corresponding change of `LockManager` is done.
        // self.lock_manager
        //     .remove_lock_wait(ctx_inner.lock_wait_token);

        if !self.allow_lock_with_conflict {
            // The result must be an owned error.
            let err = result.unwrap_err().try_into().unwrap();
            ctx_inner.cb.execute(ProcessResult::Failed { err });
            return;
        }

        // The following code is only valid after implementing the new lock-waiting
        // model.
        unreachable!();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc::{channel, Receiver},
        time::Duration,
    };

    use super::*;
    use crate::storage::{
        lock_manager::DummyLockManager,
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
        txn::{Error as TxnError, ErrorInner as TxnErrorInner},
        ErrorInner as StorageErrorInner, Result as StorageResult,
    };

    fn create_storage_cb() -> (
        StorageCallback,
        Receiver<StorageResult<StorageResult<PessimisticLockRes>>>,
    ) {
        let (tx, rx) = channel();
        let cb = StorageCallback::PessimisticLock(Box::new(move |r| tx.send(r).unwrap()));
        (cb, rx)
    }

    fn create_test_lock_wait_ctx() -> (
        LockWaitContext<impl LockManager>,
        Receiver<StorageResult<StorageResult<PessimisticLockRes>>>,
    ) {
        // TODO: Use `ProxyLockMgr` to check the correctness of the `remove_lock_wait`
        // invocation.
        let lock_mgr = DummyLockManager {};
        let (cb, rx) = create_storage_cb();
        let ctx = LockWaitContext::new(
            lock_mgr,
            super::super::LockWaitToken(Some(1)),
            1.into(),
            1.into(),
            cb,
            false,
        );
        (ctx, rx)
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

        let (ctx, rx) = create_test_lock_wait_ctx();
        // Nothing happens currently.
        (ctx.get_callback_for_first_write_batch()).execute(ProcessResult::Res);
        rx.recv_timeout(Duration::from_millis(20)).unwrap_err();
        (ctx.get_callback_for_blocked_key())(Err(SharedError::from(write_conflict())));
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

        let (ctx, rx) = create_test_lock_wait_ctx();
        (ctx.get_callback_for_cancellation())(StorageError::from(key_is_locked()));
        let res = rx.recv().unwrap().unwrap_err();
        assert!(matches!(
            &res,
            StorageError(box StorageErrorInner::Txn(TxnError(
                box TxnErrorInner::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked(_)))
            )))
        ));
        // Nothing happens if the callback is double-called.
        (ctx.get_callback_for_blocked_key())(Err(SharedError::from(write_conflict())));
        // The tx should be dropped.
        rx.recv().unwrap_err();
    }
}

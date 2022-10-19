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

use std::{convert::TryInto, result::Result, sync::Arc};

use parking_lot::Mutex;
use txn_types::Key;

use crate::storage::{
    errors::SharedError,
    lock_manager::{
        lock_waiting_queue::{LockWaitQueues, PessimisticLockKeyCallback},
        LockManager, LockWaitToken,
    },
    Error as StorageError, PessimisticLockRes, ProcessResult, StorageCallback,
};

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
        let inner = LockWaitContextInner { cb };
        Self {
            shared_states: Arc::new(LockWaitContextSharedState {
                ctx_inner: Mutex::new(Some(inner)),
                key,
                lock_wait_token,
            }),
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
        Box::new(move |res| {
            ctx.finish_request(res, false);
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
    pub fn get_callback_for_cancellation(&self) -> impl FnOnce(StorageError) {
        let ctx = self.clone();
        move |e| {
            ctx.finish_request(Err(e.into()), true);
        }
    }

    fn finish_request(&self, result: Result<PessimisticLockRes, SharedError>, is_canceling: bool) {
        if is_canceling {
            let entry = self
                .lock_wait_queues
                .remove_by_token(&self.shared_states.key, self.shared_states.lock_wait_token);
            if entry.is_none() {
                // Already popped out from the queue so that it will be woken up normally. Do
                // nothing.
                return;
            }
        } else {
            // TODO: Uncomment this after the corresponding change of
            // `LockManager` is done. self.lock_wait_queues.
            // get_lock_mgr()     .remove_lock_wait(ctx_inner.
            // lock_wait_token);
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

        // The following code is only valid after implementing the new lock-waiting
        // model.
        unreachable!();
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
        lock_manager::{lock_waiting_queue::LockWaitEntry, DummyLockManager},
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
        txn::{Error as TxnError, ErrorInner as TxnErrorInner},
        types::PessimisticLockParameters,
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

    fn create_test_lock_wait_ctx(
        key: &Key,
        lock_wait_queues: &LockWaitQueues<impl LockManager>,
    ) -> (
        LockWaitToken,
        LockWaitContext<impl LockManager>,
        Receiver<StorageResult<StorageResult<PessimisticLockRes>>>,
    ) {
        let (cb, rx) = create_storage_cb();
        let token = LockWaitToken(Some(1));
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
        let lock_wait_queues = LockWaitQueues::new(DummyLockManager {});

        let (_, ctx, rx) = create_test_lock_wait_ctx(&key, &lock_wait_queues);
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
                lock_wait_token: token,
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

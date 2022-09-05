// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryInto,
    result::Result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use tikv_util::time::Instant;
use txn_types::TimeStamp;

use crate::storage::{
    lock_manager::{
        lock_waiting_queue::{PessimisticLockKeyCallback, SharedError},
        LockManager, LockWaitToken,
    },
    Error as StorageError, PessimisticLockRes, ProcessResult, StorageCallback,
};

pub struct LockWaitContextInner {
    pr: ProcessResult,
    cb: StorageCallback,
    // result_rx: std::sync::mpsc::Receiver<(
    //     usize,
    //     Result<PessimisticLockKeyResult, SharedError>,
    // )>,
    lock_wait_token: LockWaitToken,
}

pub struct LockWaitContextSharedState {
    ctx_inner: Mutex<Option<LockWaitContextInner>>,
    // remaining_keys: AtomicUsize,
    pub finished: AtomicBool,
}

impl LockWaitContextSharedState {
    pub fn is_finished(&self) -> bool {
        return self.finished.load(Ordering::Acquire);
    }
}

#[derive(Clone)]
struct LockWaitContext<L: LockManager> {
    shared_states: Arc<LockWaitContextSharedState>,
    // tx: std::sync::mpsc::Sender<(
    //     usize,
    //     Result<PessimisticLockKeyResult, SharedError>,
    // )>,
    lock_manager: L,
    allow_lock_with_conflict: bool,

    // Fields for logging:
    start_ts: TimeStamp,
    for_update_ts: TimeStamp,
}

impl<L: LockManager> LockWaitContext<L> {
    fn new(
        lock_manager: L,
        lock_wait_token: LockWaitToken,
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        first_batch_pr: ProcessResult,
        cb: StorageCallback,
        // size: usize,
        allow_lock_with_conflict: bool,
    ) -> Self {
        // let (tx, rx) = channel();
        let inner = LockWaitContextInner {
            pr: first_batch_pr,
            cb,
            // result_rx: rx,
            lock_wait_token,
        };
        Self {
            shared_states: Arc::new(LockWaitContextSharedState {
                ctx_inner: Mutex::new(Some(inner)),
                // remaining_keys: AtomicUsize::new(size),
                finished: AtomicBool::new(false),
            }),
            // tx,
            lock_manager,
            allow_lock_with_conflict,
            start_ts,
            for_update_ts,
        }
    }

    fn get_shared_states(&self) -> Arc<LockWaitContextSharedState> {
        self.shared_states.clone()
    }

    fn get_callback_for_first_write_batch(&self) -> StorageCallback {
        StorageCallback::Boolean(Box::new(|res| {
            res.unwrap();
        }))
    }

    fn get_callback_for_blocked_key(&self) -> PessimisticLockKeyCallback {
        let ctx = self.clone();
        if self.allow_lock_with_conflict {
            Box::new(move |res| {
                ctx.finish_request(res);
            })
        } else {
            Box::new(move |res| {
                ctx.finish_request(res);
            })
        }
    }

    fn get_callback_for_cancellation(&self) -> impl FnOnce(StorageError) {
        let ctx = self.clone();
        move |e| {
            ctx.finish_request(Err(e.into()));
        }
    }

    fn finish_request(&self, result: Result<PessimisticLockRes, SharedError>) {
        let ctx_inner = if let Some(inner) = self.shared_states.ctx_inner.lock().take() {
            inner
        } else {
            info!("shared state for partial pessimistic lock already taken, perhaps due to error";
                "start_ts" => self.start_ts,
                "for_update_ts" => self.for_update_ts
            );
            return;
        };

        self.shared_states.finished.store(true, Ordering::Release);

        // TODO: Uncomment this after merging the change to the lock manager.
        // self.lock_manager
        //     .remove_lock_wait(ctx_inner.lock_wait_token);

        // let results_in_pr = match ctx_inner.pr {
        //     ProcessResult::PessimisticLockRes {
        //         res: Ok(PessimisticLockResults(ref mut v)),
        //     } => v,
        //     _ => unreachable!(),
        // };

        if !self.allow_lock_with_conflict {
            // assert_eq!(results_in_pr.len(), 1);
            // assert!(matches!(
            //     results_in_pr[0],
            //     PessimisticLockKeyResult::Waiting(None)
            // ));
            // The result must be an owned error.
            let err = result.unwrap_err().try_into().unwrap();
            ctx_inner.cb.execute(ProcessResult::Failed { err });
            return;
        }

        // The following code is only valid after implementing the new lock-waiting
        // model.
        unreachable!();

        // while let Ok((index, msg)) = ctx_inner.result_rx.try_recv() {
        //     assert!(matches!(
        //         results_in_pr[index],
        //         PessimisticLockKeyResult::Waiting(None)
        //     ));
        //     match msg {
        //         Ok(lock_key_res) => results_in_pr[index] = lock_key_res,
        //         Err(e) => results_in_pr[index] =
        // PessimisticLockKeyResult::Failed(e), // ???     }
        // }
        //
        // if let Some(e) = external_error {
        //     let e = Arc::new(e);
        //     for r in results_in_pr {
        //         if fail_all_on_error || matches!(r,
        // PessimisticLockKeyResult::Waiting(_)) {             *r =
        // PessimisticLockKeyResult::Failed(e.clone());         }
        //     }
        // }
        // STORAGE_KEYWISE_PESSIMISTIC_LOCK_HANDLE_DURATION_HISTOGRAM_STATIC
        //     .finish_request
        //     .observe(start_time.saturating_elapsed_secs());
        //
        // ctx_inner.cb.execute(ctx_inner.pr);
    }
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::config::Config;
use super::deadlock::Scheduler as DetectorScheduler;
use super::metrics::*;
use crate::storage::lock_manager::{
    DiagnosticContext, KeyLockWaitInfo, LockDigest, LockWaitToken, WaitTimeout,
};
use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, TimeStamp};
use crate::storage::txn::{Error as TxnError, ErrorInner as TxnErrorInner};
use crate::storage::{
    Error as StorageError, ErrorInner as StorageErrorInner, ProcessResult, StorageCallback,
};
use collections::{HashMap, HashSet};
use engine_traits::KvEngine;
use raftstore::coprocessor::RoleChange;
use raftstore::coprocessor::{
    BoxRegionChangeObserver, BoxRoleObserver, Coprocessor, CoprocessorHost, ObserverContext,
    RegionChangeEvent, RegionChangeObserver, RoleObserver,
};
use tikv_util::worker::{FutureRunnable, FutureScheduler, Stopped};

use std::cell::RefCell;
use std::fmt::{self, Debug, Display, Formatter};
use std::num::NonZeroU64;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use futures::compat::Compat01As03;
use futures::compat::Future01CompatExt;
use futures::future::Future;
use futures::task::{Context, Poll};
use kvproto::deadlock::WaitForEntry;
use kvproto::metapb::RegionEpoch;
use prometheus::HistogramTimer;
use raft::StateRole;
use tikv_util::config::ReadableDuration;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tokio::task::spawn_local;

struct DelayInner {
    timer: Compat01As03<tokio_timer::Delay>,
    cancelled: bool,
}

/// `Delay` is a wrapper of `tokio_timer::Delay` which has a resolution of one millisecond.
/// It has some extra features than `tokio_timer::Delay` used by `WaiterManager`.
///
/// `Delay` performs no work and completes with `true` once the specified deadline has been reached.
/// If it has been cancelled, it will complete with `false` at arbitrary time.
// FIXME: Use `tokio_timer::DelayQueue` instead if https://github.com/tokio-rs/tokio/issues/1700 is fixed.
#[derive(Clone)]
struct Delay {
    inner: Rc<RefCell<DelayInner>>,
    deadline: Instant,
}

impl Delay {
    /// Create a new `Delay` instance that elapses at `deadline`.
    fn new(deadline: Instant) -> Self {
        let inner = DelayInner {
            timer: GLOBAL_TIMER_HANDLE.delay(deadline).compat(),
            cancelled: false,
        };
        Self {
            inner: Rc::new(RefCell::new(inner)),
            deadline,
        }
    }

    /// Resets the instance to an earlier deadline.
    fn reset(&self, deadline: Instant) {
        if deadline < self.deadline {
            self.inner.borrow_mut().timer.get_mut().reset(deadline);
        }
    }

    /// Cancels the instance. It will complete with `false` at arbitrary time.
    fn cancel(&self) {
        self.inner.borrow_mut().cancelled = true;
    }

    fn is_cancelled(&self) -> bool {
        self.inner.borrow().cancelled
    }
}

impl Future for Delay {
    // Whether the instance is triggered normally(true) or cancelled(false).
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        if self.is_cancelled() {
            return Poll::Ready(false);
        }
        Pin::new(&mut self.inner.borrow_mut().timer)
            .poll(cx)
            .map(|_| true)
    }
}

pub type Callback = Box<dyn FnOnce(Vec<WaitForEntry>) + Send>;

#[allow(clippy::large_enum_variant)]
pub enum Task {
    WaitFor {
        token: LockWaitToken,
        region_id: u64,
        region_epoch: RegionEpoch,
        term: NonZeroU64,
        // which txn waits for the lock
        start_ts: TimeStamp,
        wait_info: Vec<KeyLockWaitInfo>,
        timeout: WaitTimeout,
        cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
        diag_ctx: DiagnosticContext,
    },
    RemoveLockWait {
        token: LockWaitToken,
    },
    Dump {
        cb: Callback,
    },
    Deadlock {
        token: LockWaitToken,
        // Which txn causes deadlock
        start_ts: TimeStamp,
        key: Vec<u8>,
        lock: LockDigest,
        deadlock_key_hash: u64,
        wait_chain: Vec<WaitForEntry>,
    },
    RegionLeaderRetired {
        region_id: u64,
        expired_term: u64,
    },
    RegionEpochChanged {
        region_id: u64,
        region_epoch: RegionEpoch,
    },
    ChangeConfig {
        timeout: Option<ReadableDuration>,
        delay: Option<ReadableDuration>,
    },
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(ReadableDuration, ReadableDuration) + Send>),
}

/// Debug for task.
impl Debug for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Display for task.
impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::WaitFor {
                token,
                start_ts,
                wait_info,
                ..
            } => {
                write!(
                    f,
                    "txn:{} waiting for {}:{} and another {} locks, token {:?}",
                    start_ts,
                    wait_info[0].lock_digest.ts,
                    wait_info[0].lock_digest.hash,
                    wait_info.len() - 1,
                    token
                )
            }
            Task::RemoveLockWait { token } => {
                write!(f, "waking up txns waiting for token {:?}", token)
            }
            Task::Dump { .. } => write!(f, "dump"),
            Task::Deadlock { start_ts, .. } => write!(f, "txn:{} deadlock", start_ts),
            Task::RegionLeaderRetired {
                region_id,
                expired_term,
            } => write!(
                f,
                "region {} leader at term {} retired",
                region_id, expired_term
            ),
            Task::RegionEpochChanged {
                region_id,
                region_epoch,
            } => write!(f, "region {} epoch changed: {:?}", region_id, region_epoch),
            Task::ChangeConfig { timeout, delay } => write!(
                f,
                "change config to default_wait_for_lock_timeout: {:?}, wake_up_delay_duration: {:?}",
                timeout, delay
            ),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(_) => write!(f, "validate waiter manager config"),
        }
    }
}

/// If a pessimistic transaction meets a lock, it will wait for the lock
/// released in `WaiterManager`.
///
/// `Waiter` contains the context of the pessimistic transaction. Each `Waiter`
/// has a timeout. Transaction will be notified when the lock is released
/// or the corresponding waiter times out.
pub(crate) struct Waiter {
    region_id: u64,
    region_epoch: RegionEpoch,
    term: NonZeroU64,
    pub(crate) start_ts: TimeStamp,
    pub(crate) wait_info: Vec<KeyLockWaitInfo>,
    pub(crate) cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
    pub diag_ctx: DiagnosticContext,
    delay: Delay,
    _lifetime_timer: HistogramTimer,
}

impl Waiter {
    fn new(
        region_id: u64,
        region_epoch: RegionEpoch,
        term: NonZeroU64,
        start_ts: TimeStamp,
        wait_info: Vec<KeyLockWaitInfo>,
        cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
        deadline: Instant,
        diag_ctx: DiagnosticContext,
    ) -> Self {
        Self {
            region_id,
            region_epoch,
            term,
            start_ts,
            wait_info,
            cancel_callback,
            delay: Delay::new(deadline),
            diag_ctx,
            _lifetime_timer: WAITER_LIFETIME_HISTOGRAM.start_coarse_timer(),
        }
    }

    /// The `F` will be invoked if the `Waiter` times out normally.
    fn on_timeout<F: FnOnce()>(&self, f: F) -> impl Future<Output = ()> {
        let timer = self.delay.clone();
        async move {
            if timer.await {
                // Timer times out or error occurs.
                // It should call timeout handler to prevent starvation.
                f();
            }
            // The timer is cancelled. Don't call timeout handler.
        }
    }

    fn reset_timeout(&self, deadline: Instant) {
        self.delay.reset(deadline);
    }

    /// `Notify` consumes the `Waiter` to notify the corresponding transaction
    /// going on.
    fn cancel(self, error: Option<StorageError>) -> Vec<KeyLockWaitInfo> {
        // Cancel the delay timer to prevent removing the same `Waiter` earlier.
        self.delay.cancel();
        if let Some(error) = error {
            (self.cancel_callback)(error);
        }
        self.wait_info
    }

    fn cancel_for_finished(self) -> Vec<KeyLockWaitInfo> {
        self.cancel(None)
    }

    fn cancel_for_timeout(self) -> Vec<KeyLockWaitInfo> {
        let lock_info = self.wait_info[0].lock_info.clone();
        let error = MvccError::from(MvccErrorInner::KeyIsLocked(lock_info));
        self.cancel(Some(StorageError::from(TxnError::from(error))))
    }

    pub(super) fn cancel_no_timeout(
        mut wait_info: Vec<KeyLockWaitInfo>,
        cancel_callback: Box<dyn FnOnce(StorageError)>,
    ) {
        let lock_info = wait_info.swap_remove(0).lock_info;
        let error = MvccError::from(MvccErrorInner::KeyIsLocked(lock_info));
        cancel_callback(StorageError::from(TxnError::from(error)))
    }

    fn cancel_for_deadlock(
        self,
        lock_digest: LockDigest,
        key: Vec<u8>,
        deadlock_key_hash: u64,
        wait_chain: Vec<WaitForEntry>,
    ) -> Vec<KeyLockWaitInfo> {
        let e = MvccError::from(MvccErrorInner::Deadlock {
            start_ts: self.start_ts,
            lock_ts: lock_digest.ts,
            lock_key: key,
            deadlock_key_hash,
            wait_chain,
        });
        self.cancel(Some(StorageError::from(TxnError::from(e))))
    }

    // /// Changes the `ProcessResult` to `WriteConflict`.
    // /// It may be invoked more than once.
    // fn conflict_with(&mut self, lock_ts: TimeStamp, commit_ts: TimeStamp) {
    //     let (key, primary) = self.extract_key_info();
    //     let mvcc_err = MvccError::from(MvccErrorInner::WriteConflict {
    //         start_ts: self.start_ts,
    //         conflict_start_ts: lock_ts,
    //         conflict_commit_ts: commit_ts,
    //         key,
    //         primary,
    //     });
    //     self.pr = ProcessResult::Failed {
    //         err: StorageError::from(TxnError::from(mvcc_err)),
    //     };
    // }

    // /// Changes the `ProcessResult` to `Deadlock`.
    // fn deadlock_with(&mut self, deadlock_key_hash: u64, wait_chain: Vec<WaitForEntry>) {
    //     let (key, _) = self.extract_key_info();
    //     let mvcc_err = MvccError::from(MvccErrorInner::Deadlock {
    //         start_ts: self.start_ts,
    //         lock_ts: self.lock.ts,
    //         lock_key: key,
    //         deadlock_key_hash,
    //         wait_chain,
    //     });
    //     self.pr = ProcessResult::Failed {
    //         err: StorageError::from(TxnError::from(mvcc_err)),
    //     };
    // }

    // /// Extracts key and primary key from `ProcessResult`.
    // fn extract_key_info(&mut self) -> (Vec<u8>, Vec<u8>) {
    //     match &mut self.pr {
    //         ProcessResult::PessimisticLockRes { res } => match res {
    //             Err(StorageError(box StorageErrorInner::Txn(TxnError(
    //                 box TxnErrorInner::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked(info))),
    //             )))) => (info.take_key(), info.take_primary_lock()),
    //             _ => panic!("unexpected mvcc error"),
    //         },
    //         ProcessResult::Failed { err } => match err {
    //             StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
    //                 MvccError(box MvccErrorInner::WriteConflict {
    //                     ref mut key,
    //                     ref mut primary,
    //                     ..
    //                 }),
    //             )))) => (std::mem::take(key), std::mem::take(primary)),
    //             _ => panic!("unexpected mvcc error"),
    //         },
    //         _ => panic!("unexpected progress result"),
    //     }
    // }

    fn check_region_state(&self, region_state: &RegionState) -> CheckRegionStateResult {
        if self.term < region_state.term
            || self.region_epoch.version < region_state.region_epoch.version
            || self.region_epoch.conf_ver < region_state.region_epoch.conf_ver
        {
            return CheckRegionStateResult::WaiterExpired;
        } else if self.term > region_state.term
            || self.region_epoch.version > region_state.region_epoch.version
            || self.region_epoch.conf_ver > region_state.region_epoch.conf_ver
        {
            return CheckRegionStateResult::RegionStateExpired;
        }

        assert_eq!(self.term, region_state.term);
        assert_eq!(self.region_epoch.version, region_state.region_epoch.version);
        assert_eq!(
            self.region_epoch.conf_ver,
            region_state.region_epoch.conf_ver
        );

        CheckRegionStateResult::Ok
    }
}

// NOTE: Now we assume `Waiters` is not very long.
// Maybe needs to use `BinaryHeap` or sorted `VecDeque` instead.
// type Waiters = Vec<Waiter>;

#[derive(PartialEq)]
struct RegionState {
    region_epoch: RegionEpoch,
    term: NonZeroU64,
}

impl RegionState {
    fn new(region_epoch: RegionEpoch, term: NonZeroU64) -> Self {
        Self { region_epoch, term }
    }
}

enum CheckRegionStateResult {
    Ok,
    RegionStateExpired,
    WaiterExpired,
}

struct WaitTable {
    // Map lock hash and ts to waiters.
    // For compatibility.
    wait_table: HashMap<(u64, TimeStamp), LockWaitToken>,
    // Map region id to waiters belonging to this region, along with other meta of the region.
    region_waiters: HashMap<u64, (RegionState, HashSet<LockWaitToken>)>,
    waiter_pool: HashMap<LockWaitToken, Waiter>,
    waiter_count: Arc<AtomicUsize>,
    on_waiter_cancel_by_region_error:
        Option<Box<dyn Fn(LockWaitToken, Vec<KeyLockWaitInfo>) + Send>>,
}

impl WaitTable {
    fn new(waiter_count: Arc<AtomicUsize>) -> Self {
        Self {
            wait_table: HashMap::default(),
            region_waiters: HashMap::default(),
            waiter_pool: HashMap::default(),
            waiter_count,
            on_waiter_cancel_by_region_error: None,
        }
    }

    fn set_on_waiter_cancel_by_region_error(
        &mut self,
        cb: Option<Box<dyn Fn(LockWaitToken, Vec<KeyLockWaitInfo>) + Send>>,
    ) {
        self.on_waiter_cancel_by_region_error = cb;
    }

    #[cfg(test)]
    fn count(&self) -> usize {
        self.waiter_pool.len()
    }

    fn is_empty(&self) -> bool {
        self.waiter_pool.is_empty()
    }

    /// Returns the duplicated `Waiter` if there is.
    fn add_waiter(&mut self, token: LockWaitToken, waiter: Waiter) -> bool {
        // Map region id to waiters in the region.
        let mut region_waiters_entry =
            self.region_waiters
                .entry(waiter.region_id)
                .or_insert_with(|| {
                    (
                        RegionState::new(waiter.region_epoch.clone(), waiter.term),
                        HashSet::default(),
                    )
                });
        match waiter.check_region_state(&region_waiters_entry.0) {
            CheckRegionStateResult::RegionStateExpired => {
                self.cancel_region(waiter.region_id);
                region_waiters_entry =
                    self.region_waiters
                        .entry(waiter.region_id)
                        .or_insert_with(|| {
                            (
                                RegionState::new(waiter.region_epoch.clone(), waiter.term),
                                HashSet::default(),
                            )
                        });
            }
            CheckRegionStateResult::WaiterExpired => {
                self.waiter_count.fetch_sub(1, Ordering::SeqCst);
                waiter.cancel_for_timeout();
                return false;
            }
            CheckRegionStateResult::Ok => {}
        }

        for waiting_item in &waiter.wait_info {
            // Add to old wait_table for compatibility. Replace on duplicated.
            // let waiters = self.wait_table.entry(waiting_item.lock_digest.hash).or_insert_with(|| {
            //     // WAIT_TABLE_STATUS_GAUGE.locks.inc();
            //     Vec::default()
            // });
            // let waiter_pool = &mut self.waiter_pool;
            // let _old_idx = waiters
            //     .iter()
            //     .position(|w| waiter_pool.get(w).unwrap().start_ts == waiter.start_ts);
            // waiters.push(token);
            self.wait_table
                .insert((waiting_item.lock_digest.hash, waiter.start_ts), token);
        }

        assert!(region_waiters_entry.1.insert(token));
        assert!(self.waiter_pool.insert(token, waiter).is_none());

        // if let Some(old_idx) = old_idx {
        //     let _old = waiters.swap_remove(old_idx);
        //     self.waiter_count.fetch_sub(1, Ordering::SeqCst);
        //     // Some(old)
        // } else {
        //     // WAIT_TABLE_STATUS_GAUGE.txns.inc();
        //     // None
        // }
        // Here we don't increase waiter_count because it's already updated in LockManager::wait_for()

        true
    }

    fn cancel_region(&mut self, region_id: u64) {
        let (_, tokens) = match self.region_waiters.remove(&region_id) {
            Some(entry) => entry,
            None => return,
        };
        let count = tokens.len();
        for token in tokens {
            let waiter = self.waiter_pool.remove(&token).unwrap();
            for waiting_item in &waiter.wait_info {
                self.wait_table
                    .remove(&(waiting_item.lock_digest.hash, waiter.start_ts));
            }
            // TODO: Cancel with a region error.
            let wait_info = waiter.cancel_for_timeout();
            if let Some(cb) = &self.on_waiter_cancel_by_region_error {
                cb(token, wait_info)
            }
        }
        self.waiter_count.fetch_sub(count, Ordering::SeqCst);
    }

    // /// Removes all waiters waiting for the lock.
    // fn remove(&mut self, lock: LockDigest) {
    //     self.wait_table.remove(&lock.hash);
    //     WAIT_TABLE_STATUS_GAUGE.locks.dec();
    // }

    fn take_waiter(&mut self, token: LockWaitToken) -> Option<Waiter> {
        let waiter = self.waiter_pool.remove(&token)?;
        self.waiter_count.fetch_sub(1, Ordering::SeqCst);
        for waiting_item in &waiter.wait_info {
            self.wait_table
                .remove(&(waiting_item.lock_digest.hash, waiter.start_ts));
        }
        let region_waiters = self.region_waiters.get_mut(&waiter.region_id).unwrap();
        assert!(region_waiters.1.remove(&token));
        if region_waiters.1.is_empty() {
            self.region_waiters.remove(&waiter.region_id);
        }
        // WAIT_TABLE_STATUS_GAUGE.txns.dec();
        Some(waiter)
    }

    fn take_waiter_by_lock_digest(
        &mut self,
        lock: LockDigest,
        waiter_ts: TimeStamp,
    ) -> Option<Waiter> {
        let token = *self.wait_table.get(&(lock.hash, waiter_ts))?;
        self.take_waiter(token)
    }

    // /// Removes the `Waiter` with the smallest start ts and returns it with remaining waiters.
    // ///
    // /// NOTE: Due to the borrow checker, it doesn't remove the entry in the `WaitTable`
    // /// even if there is no remaining waiter.
    // fn remove_oldest_waiter(&mut self, lock: LockDigest) -> Option<(Waiter, &mut Waiters)> {
    //     let waiters = self.wait_table.get_mut(&lock.hash)?;
    //     let oldest_idx = waiters
    //         .iter()
    //         .enumerate()
    //         .min_by_key(|(_, w)| w.start_ts)
    //         .unwrap()
    //         .0;
    //     let oldest = waiters.swap_remove(oldest_idx);
    //     self.waiter_count.fetch_sub(1, Ordering::SeqCst);
    //     WAIT_TABLE_STATUS_GAUGE.txns.dec();
    //     Some((oldest, waiters))
    // }

    fn to_wait_for_entries(&self) -> Vec<WaitForEntry> {
        // self.wait_table
        //     .iter()
        //     .flat_map(|(_, waiters)| {
        //         waiters.iter().map(|waiter| {
        //             let mut wait_for_entry = WaitForEntry::default();
        //             wait_for_entry.set_txn(waiter.start_ts.into_inner());
        //             wait_for_entry.set_wait_for_txn(waiter.lock.ts.into_inner());
        //             wait_for_entry.set_key_hash(waiter.lock.hash);
        //             wait_for_entry.set_key(waiter.diag_ctx.key.clone());
        //             wait_for_entry
        //                 .set_resource_group_tag(waiter.diag_ctx.resource_group_tag.clone());
        //             wait_for_entry
        //         })
        //     })
        //     .collect()
        self.waiter_pool
            .iter()
            .flat_map(|(_, waiter)| {
                waiter.wait_info.iter().map(move |waiting_item| {
                    let mut wait_for_entry = WaitForEntry::default();
                    wait_for_entry.set_txn(waiter.start_ts.into_inner());
                    wait_for_entry.set_wait_for_txn(waiting_item.lock_digest.ts.into_inner());
                    wait_for_entry.set_key_hash(waiting_item.lock_digest.hash);
                    wait_for_entry.set_key(waiting_item.key.to_raw().unwrap());
                    wait_for_entry
                        .set_resource_group_tag(waiter.diag_ctx.resource_group_tag.clone());
                    wait_for_entry
                })
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct Scheduler(FutureScheduler<Task>);

impl Scheduler {
    pub fn new(scheduler: FutureScheduler<Task>) -> Self {
        Self(scheduler)
    }

    fn notify_scheduler(&self, task: Task) -> bool {
        if let Err(Stopped(task)) = self.0.schedule(task) {
            error!("failed to send task to waiter_manager"; "task" => %task);
            if let Task::WaitFor {
                cancel_callback, ..
            } = task
            {
                // TODO: Pass proper error for the scheduling error.
                cancel_callback(StorageError(Box::new(StorageErrorInner::SchedTooBusy)));
            }
            return false;
        }
        true
    }

    pub fn wait_for(
        &self,
        token: LockWaitToken,
        region_id: u64,
        region_epoch: RegionEpoch,
        term: NonZeroU64,
        start_ts: TimeStamp,
        wait_info: Vec<KeyLockWaitInfo>,
        timeout: WaitTimeout,
        cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
        diag_ctx: DiagnosticContext,
    ) {
        self.notify_scheduler(Task::WaitFor {
            token,
            region_id,
            region_epoch,
            term,
            start_ts,
            wait_info,
            timeout,
            cancel_callback,
            diag_ctx,
        });
    }

    pub fn remove_lock_wait(&self, token: LockWaitToken) {
        self.notify_scheduler(Task::RemoveLockWait { token });
    }

    pub fn dump_wait_table(&self, cb: Callback) -> bool {
        self.notify_scheduler(Task::Dump { cb })
    }

    pub fn deadlock(
        &self,
        token: LockWaitToken,
        txn_ts: TimeStamp,
        key: Vec<u8>,
        lock: LockDigest,
        deadlock_key_hash: u64,
        wait_chain: Vec<WaitForEntry>,
    ) {
        self.notify_scheduler(Task::Deadlock {
            token,
            start_ts: txn_ts,
            key,
            lock,
            deadlock_key_hash,
            wait_chain,
        });
    }

    pub fn change_config(
        &self,
        timeout: Option<ReadableDuration>,
        delay: Option<ReadableDuration>,
    ) {
        self.notify_scheduler(Task::ChangeConfig { timeout, delay });
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn validate(&self, f: Box<dyn FnOnce(ReadableDuration, ReadableDuration) + Send>) {
        self.notify_scheduler(Task::Validate(f));
    }
}

/// WaiterManager handles waiting and wake-up of pessimistic lock
pub struct WaiterManager {
    wait_table: Rc<RefCell<WaitTable>>,
    detector_scheduler: DetectorScheduler,
    /// It is the default and maximum timeout of waiter.
    default_wait_for_lock_timeout: ReadableDuration,
    /// If more than one waiters are waiting for the same lock, only the
    /// oldest one will be waked up immediately when the lock is released.
    /// Others will be waked up after `wake_up_delay_duration` to reduce
    /// contention and make the oldest one more likely acquires the lock.
    wake_up_delay_duration: ReadableDuration,
}

unsafe impl Send for WaiterManager {}

impl WaiterManager {
    pub fn new(
        waiter_count: Arc<AtomicUsize>,
        detector_scheduler: DetectorScheduler,
        cfg: &Config,
    ) -> Self {
        let mut wait_table = WaitTable::new(waiter_count);
        let detector_scheduler1 = detector_scheduler.clone();
        wait_table.set_on_waiter_cancel_by_region_error(Some(Box::new(move |token, wait_info| {
            detector_scheduler1.clean_up_wait_for(token, wait_info);
        })));

        Self {
            wait_table: Rc::new(RefCell::new(wait_table)),
            detector_scheduler,
            default_wait_for_lock_timeout: cfg.wait_for_lock_timeout,
            wake_up_delay_duration: cfg.wake_up_delay_duration,
        }
    }

    pub fn normalize_deadline(&self, timeout: WaitTimeout) -> Instant {
        Instant::now()
            + timeout.into_duration_with_ceiling(self.default_wait_for_lock_timeout.as_millis())
    }

    fn handle_wait_for(&mut self, token: LockWaitToken, waiter: Waiter) {
        let wait_table = self.wait_table.clone();
        let detector_scheduler = self.detector_scheduler.clone();
        // Remove the waiter from wait table when it times out.
        let f = waiter.on_timeout(move || {
            if let Some(waiter) = wait_table.borrow_mut().take_waiter(token) {
                let wait_info = waiter.cancel_for_timeout();
                detector_scheduler.clean_up_wait_for(token, wait_info);
            }
        });
        if self.wait_table.borrow_mut().add_waiter(token, waiter) {
            spawn_local(f);
        }
    }

    fn handle_remove_lock_wait(&mut self, token: LockWaitToken) {
        let mut wait_table = self.wait_table.borrow_mut();
        if wait_table.is_empty() {
            return;
        }
        // let duration: Duration = self.wake_up_delay_duration.into();
        // let _new_timeout = Instant::now() + duration;
        let waiter = if let Some(w) = wait_table.take_waiter(token) {
            w
        } else {
            return;
        };
        let wait_info = waiter.cancel_for_finished();
        self.detector_scheduler.clean_up_wait_for(token, wait_info);
        // for hash in hashes {
        //     let _lock = LockDigest { ts: lock_ts, hash };
        //     if let Some((mut oldest, others)) = wait_table.remove_oldest_waiter(lock) {
        //         // Notify the oldest one immediately.
        //         self.detector_scheduler
        //             .clean_up_wait_for(oldest.start_ts, oldest.lock);
        //         // oldest.conflict_with(lock_ts, commit_ts);
        //         // oldest.cancel();
        //         // Others will be waked up after `wake_up_delay_duration`.
        //         //
        //         // NOTE: Actually these waiters are waiting for an unknown transaction.
        //         // If there is a deadlock between them, it will be detected after timeout.
        //         if others.is_empty() {
        //             // Remove the empty entry here.
        //             wait_table.remove(lock);
        //         } else {
        //             others.iter_mut().for_each(|waiter| {
        //                 waiter.conflict_with(lock_ts, commit_ts);
        //                 waiter.reset_timeout(new_timeout);
        //             });
        //         }
        //     }
        // }
    }

    fn handle_dump(&self, cb: Callback) {
        cb(self.wait_table.borrow().to_wait_for_entries());
    }

    fn handle_deadlock(
        &mut self,
        token: LockWaitToken,
        waiter_ts: TimeStamp,
        key: Vec<u8>,
        lock: LockDigest,
        deadlock_key_hash: u64,
        wait_chain: Vec<WaitForEntry>,
    ) {
        let waiter = if token.0.is_some() {
            self.wait_table.borrow_mut().take_waiter(token)
        } else {
            self.wait_table
                .borrow_mut()
                .take_waiter_by_lock_digest(lock, waiter_ts)
        };
        if let Some(waiter) = waiter {
            // waiter.deadlock_with(deadlock_key_hash, wait_chain);
            // waiter.cancel();
            waiter.cancel_for_deadlock(lock, key, deadlock_key_hash, wait_chain);
        }
    }

    fn handle_region_leader_retire(&mut self, region_id: u64, _term: u64) {
        self.wait_table.borrow_mut().cancel_region(region_id);
    }

    fn handle_region_epoch_change(&mut self, region_id: u64, _epoch: RegionEpoch) {
        self.wait_table.borrow_mut().cancel_region(region_id);
    }

    fn handle_config_change(
        &mut self,
        timeout: Option<ReadableDuration>,
        delay: Option<ReadableDuration>,
    ) {
        if let Some(timeout) = timeout {
            self.default_wait_for_lock_timeout = timeout;
        }
        if let Some(delay) = delay {
            self.wake_up_delay_duration = delay;
        }
        info!(
            "Waiter manager config changed";
            "default_wait_for_lock_timeout" => self.default_wait_for_lock_timeout.to_string(),
            "wake_up_delay_duration" => self.wake_up_delay_duration.to_string()
        );
    }
}

impl FutureRunnable<Task> for WaiterManager {
    fn run(&mut self, task: Task) {
        match task {
            Task::WaitFor {
                token,
                region_id,
                region_epoch,
                term,
                start_ts,
                wait_info,
                timeout,
                cancel_callback,
                diag_ctx,
            } => {
                let waiter = Waiter::new(
                    region_id,
                    region_epoch,
                    term,
                    start_ts,
                    wait_info,
                    cancel_callback,
                    self.normalize_deadline(timeout),
                    diag_ctx,
                );
                self.handle_wait_for(token, waiter);
                TASK_COUNTER_METRICS.wait_for.inc();
            }
            Task::RemoveLockWait { token } => {
                self.handle_remove_lock_wait(token);
                TASK_COUNTER_METRICS.wake_up.inc();
            }
            Task::Dump { cb } => {
                self.handle_dump(cb);
                TASK_COUNTER_METRICS.dump.inc();
            }
            Task::Deadlock {
                token,
                start_ts,
                key,
                lock,
                deadlock_key_hash,
                wait_chain,
            } => {
                self.handle_deadlock(token, start_ts, key, lock, deadlock_key_hash, wait_chain);
            }
            Task::RegionLeaderRetired {
                region_id,
                expired_term,
            } => {
                self.handle_region_leader_retire(region_id, expired_term);
            }
            Task::RegionEpochChanged {
                region_id,
                region_epoch,
            } => {
                self.handle_region_epoch_change(region_id, region_epoch);
            }
            Task::ChangeConfig { timeout, delay } => self.handle_config_change(timeout, delay),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(f) => f(
                self.default_wait_for_lock_timeout,
                self.wake_up_delay_duration,
            ),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RegionLockWaitCancellationObserver {
    scheduler: Scheduler,
}

impl RegionLockWaitCancellationObserver {
    pub fn new(scheduler: Scheduler) -> Self {
        Self { scheduler }
    }

    pub fn register(self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        coprocessor_host
            .registry
            .register_role_observer(1, BoxRoleObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_region_change_observer(1, BoxRegionChangeObserver::new(self));
    }
}

impl Coprocessor for RegionLockWaitCancellationObserver {}

impl RoleObserver for RegionLockWaitCancellationObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, _role_change: &RoleChange) {
        self.scheduler.notify_scheduler(Task::RegionLeaderRetired {
            region_id: ctx.region().get_id(),
            expired_term: 0, // TODO: Pass term here.
        });
    }
}

impl RegionChangeObserver for RegionLockWaitCancellationObserver {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        _event: RegionChangeEvent,
        _role: StateRole,
    ) {
        // TODO: Filter out non-leader events.
        self.scheduler.notify_scheduler(Task::RegionEpochChanged {
            region_id: ctx.region().get_id(),
            region_epoch: ctx.region().get_region_epoch().clone(),
        });
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::PessimisticLockResults;
    use tikv_util::future::paired_future_callback;
    use tikv_util::worker::FutureWorker;

    use std::sync::mpsc;
    use std::time::Duration;

    use futures::executor::block_on;
    use futures::future::FutureExt;
    use kvproto::kvrpcpb::LockInfo;
    use rand::prelude::*;
    use tikv_util::config::ReadableDuration;
    use tikv_util::time::InstantExt;
    use txn_types::Key;

    impl Waiter {
        fn region_id(mut self, id: u64) -> Self {
            self.region_id = id;
            self
        }

        fn epoch(mut self, ver: u64, conf_ver: u64) -> Self {
            self.region_epoch.set_version(ver);
            self.region_epoch.set_conf_ver(conf_ver);
            self
        }

        fn term(mut self, term: u64) -> Self {
            self.term = NonZeroU64::new(term).unwrap();
            self
        }
    }

    fn dummy_waiter(start_ts: TimeStamp, lock_ts: TimeStamp, hash: u64) -> Waiter {
        Waiter {
            region_id: 1,
            region_epoch: Default::default(),
            term: NonZeroU64::new(1).unwrap(),
            start_ts,
            wait_info: vec![KeyLockWaitInfo {
                key: Key::from_raw(b""),
                lock_digest: LockDigest { ts: lock_ts, hash },
                lock_info: Default::default(),
            }],
            cancel_callback: Box::new(|_| ()),
            diag_ctx: DiagnosticContext::default(),
            delay: Delay::new(Instant::now()),
            _lifetime_timer: WAITER_LIFETIME_HISTOGRAM.start_coarse_timer(),
        }
    }

    pub(crate) fn assert_elapsed<F: FnOnce()>(f: F, min: u64, max: u64) {
        let now = Instant::now();
        f();
        let elapsed = now.saturating_elapsed();
        assert!(
            Duration::from_millis(min) <= elapsed && elapsed < Duration::from_millis(max),
            "elapsed: {:?}",
            elapsed
        );
    }

    #[test]
    fn test_delay() {
        let delay = Delay::new(Instant::now() + Duration::from_millis(100));
        assert_elapsed(
            || {
                block_on(delay.map(|not_cancelled| assert!(not_cancelled)));
            },
            50,
            200,
        );

        // Should reset timeout successfully with cloned delay.
        let delay = Delay::new(Instant::now() + Duration::from_millis(100));
        let delay_clone = delay.clone();
        delay_clone.reset(Instant::now() + Duration::from_millis(50));
        assert_elapsed(
            || {
                block_on(delay.map(|not_cancelled| assert!(not_cancelled)));
            },
            20,
            100,
        );

        // New deadline can't exceed the initial deadline.
        let delay = Delay::new(Instant::now() + Duration::from_millis(100));
        let delay_clone = delay.clone();
        delay_clone.reset(Instant::now() + Duration::from_millis(300));
        assert_elapsed(
            || {
                block_on(delay.map(|not_cancelled| assert!(not_cancelled)));
            },
            50,
            200,
        );

        // Cancel timer.
        let delay = Delay::new(Instant::now() + Duration::from_millis(100));
        let delay_clone = delay.clone();
        delay_clone.cancel();
        assert_elapsed(
            || {
                block_on(delay.map(|not_cancelled| assert!(!not_cancelled)));
            },
            0,
            200,
        );
    }

    // Make clippy happy.
    pub(crate) type WaiterCtx = (
        Waiter,
        LockInfo,
        futures::channel::oneshot::Receiver<StorageError>,
    );

    pub(crate) fn new_test_waiter(
        waiter_ts: TimeStamp,
        lock_ts: TimeStamp,
        lock_hash: u64,
    ) -> WaiterCtx {
        let raw_key = b"foo".to_vec();
        let primary = b"bar".to_vec();
        let mut info = LockInfo::default();
        info.set_key(raw_key.clone());
        info.set_lock_version(lock_ts.into_inner());
        info.set_primary_lock(primary);
        info.set_lock_ttl(3000);
        info.set_txn_size(16);
        let lock = LockDigest {
            ts: lock_ts,
            hash: lock_hash,
        };
        let (cb, f) = paired_future_callback();
        let waiter = Waiter::new(
            1,
            Default::default(),
            NonZeroU64::new(1).unwrap(),
            waiter_ts,
            vec![KeyLockWaitInfo {
                key: Key::from_raw(&raw_key),
                lock_digest: lock,
                lock_info: info.clone(),
            }],
            cb,
            Instant::now() + Duration::from_millis(3000),
            DiagnosticContext::default(),
        );
        (waiter, info, f)
    }

    // #[test]
    // fn test_waiter_extract_key_info() {
    //     let (mut waiter, mut lock_info, _) = new_test_waiter(10.into(), 20.into(), 20);
    //     assert_eq!(
    //         waiter.extract_key_info(),
    //         (lock_info.take_key(), lock_info.take_primary_lock())
    //     );
    //
    //     let (mut waiter, mut lock_info, _) = new_test_waiter(10.into(), 20.into(), 20);
    //     waiter.conflict_with(20.into(), 30.into());
    //     assert_eq!(
    //         waiter.extract_key_info(),
    //         (lock_info.take_key(), lock_info.take_primary_lock())
    //     );
    // }

    pub(crate) fn expect_key_is_locked(error: StorageError, lock_info: LockInfo) {
        match error {
            StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                MvccError(box MvccErrorInner::KeyIsLocked(res)),
            )))) => assert_eq!(res, lock_info),
            e => panic!("unexpected error: {:?}", e),
        }
    }

    pub(crate) fn expect_write_conflict(
        error: StorageError,
        waiter_ts: TimeStamp,
        mut lock_info: LockInfo,
        commit_ts: TimeStamp,
    ) {
        match error {
            StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                MvccError(box MvccErrorInner::WriteConflict {
                    start_ts,
                    conflict_start_ts,
                    conflict_commit_ts,
                    key,
                    primary,
                }),
            )))) => {
                assert_eq!(start_ts, waiter_ts);
                assert_eq!(conflict_start_ts, lock_info.get_lock_version().into());
                assert_eq!(conflict_commit_ts, commit_ts);
                assert_eq!(key, lock_info.take_key());
                assert_eq!(primary, lock_info.take_primary_lock());
            }
            e => panic!("unexpected error: {:?}", e),
        }
    }

    pub(crate) fn expect_deadlock(
        error: StorageError,
        waiter_ts: TimeStamp,
        mut lock_info: LockInfo,
        deadlock_hash: u64,
        expect_wait_chain: &[(u64, u64, &[u8], &[u8])], // (waiter_ts, wait_for_ts, key, resource_group_tag)
    ) {
        match error {
            StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                MvccError(box MvccErrorInner::Deadlock {
                    start_ts,
                    lock_ts,
                    lock_key,
                    deadlock_key_hash,
                    wait_chain,
                }),
            )))) => {
                assert_eq!(start_ts, waiter_ts);
                assert_eq!(lock_ts, lock_info.get_lock_version().into());
                assert_eq!(lock_key, lock_info.take_key());
                assert_eq!(deadlock_key_hash, deadlock_hash);
                assert_eq!(
                    wait_chain.len(),
                    expect_wait_chain.len(),
                    "incorrect wait chain {:?}",
                    wait_chain
                );
                for (i, (entry, expect_entry)) in
                    wait_chain.iter().zip(expect_wait_chain.iter()).enumerate()
                {
                    assert_eq!(
                        entry.get_txn(),
                        expect_entry.0,
                        "item {} in wait chain mismatch",
                        i
                    );
                    assert_eq!(
                        entry.get_wait_for_txn(),
                        expect_entry.1,
                        "item {} in wait chain mismatch",
                        i
                    );
                    assert_eq!(
                        entry.get_key(),
                        expect_entry.2,
                        "item {} in wait chain mismatch",
                        i
                    );
                    assert_eq!(
                        entry.get_resource_group_tag(),
                        expect_entry.3,
                        "item {} in wait chain mismatch",
                        i
                    );
                }
            }
            e => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_waiter_notify() {
        let (waiter, lock_info, f) = new_test_waiter(10.into(), 20.into(), 20);
        waiter.cancel_for_timeout();
        expect_key_is_locked(block_on(f).unwrap(), lock_info);

        // // A waiter can conflict with other transactions more than once.
        // for conflict_times in 1..=3 {
        //     let waiter_ts = TimeStamp::new(10);
        //     let mut lock_ts = TimeStamp::new(20);
        //     let (mut waiter, mut lock_info, f) = new_test_waiter(waiter_ts, lock_ts, 20);
        //     let mut conflict_commit_ts = TimeStamp::new(30);
        //     for _ in 0..conflict_times {
        //         waiter.conflict_with(*lock_ts.incr(), *conflict_commit_ts.incr());
        //         lock_info.set_lock_version(lock_ts.into_inner());
        //     }
        //     waiter.cancel_for_timeout();
        //     expect_write_conflict(
        //         block_on(f).unwrap(),
        //         waiter_ts,
        //         lock_info,
        //         conflict_commit_ts,
        //     );
        // }

        // Deadlock
        let waiter_ts = TimeStamp::new(10);
        let (mut waiter, lock_info, f) = new_test_waiter(waiter_ts, 20.into(), 20);
        // waiter.deadlock_with(111, vec![]);
        waiter.cancel_for_deadlock(
            LockDigest {
                ts: 20.into(),
                hash: 20,
            },
            b"foo".to_vec(),
            111,
            vec![],
        );
        expect_deadlock(block_on(f).unwrap(), waiter_ts, lock_info, 111, &[]);

        // // Conflict then deadlock.
        // let waiter_ts = TimeStamp::new(10);
        // let (mut waiter, lock_info, f) = new_test_waiter(waiter_ts, 20.into(), 20);
        // waiter.conflict_with(20.into(), 30.into());
        // waiter.deadlock_with(111, vec![]);
        // waiter.cancel();
        // expect_deadlock(block_on(f).unwrap(), waiter_ts, lock_info, 111, &[]);
    }

    #[test]
    fn test_waiter_on_timeout() {
        // The timeout handler should be invoked after timeout.
        let (waiter, ..) = new_test_waiter(10.into(), 20.into(), 20);
        waiter.reset_timeout(Instant::now() + Duration::from_millis(100));
        let (tx, rx) = mpsc::sync_channel(1);
        let f = waiter.on_timeout(move || tx.send(1).unwrap());
        assert_elapsed(|| block_on(f), 50, 200);
        rx.try_recv().unwrap();

        // The timeout handler shouldn't be invoked after waiter has been notified.
        let (waiter, ..) = new_test_waiter(10.into(), 20.into(), 20);
        waiter.reset_timeout(Instant::now() + Duration::from_millis(100));
        let (tx, rx) = mpsc::sync_channel(1);
        let f = waiter.on_timeout(move || tx.send(1).unwrap());
        waiter.cancel_for_timeout();
        assert_elapsed(|| block_on(f), 0, 200);
        rx.try_recv().unwrap_err();
    }

    #[test]
    fn test_wait_table_add_and_remove() {
        let mut wait_table = WaitTable::new(Arc::new(AtomicUsize::new(0)));
        let mut waiter_info = Vec::new();
        let mut rng = rand::thread_rng();
        for i in 0..20 {
            let waiter_ts = rng.gen::<u64>().into();
            let lock = LockDigest {
                ts: rng.gen::<u64>().into(),
                hash: rng.gen(),
            };
            // Avoid adding duplicated waiter.
            if wait_table.add_waiter(
                LockWaitToken(Some(i)),
                dummy_waiter(waiter_ts, lock.ts, lock.hash),
            ) {
                waiter_info.push((waiter_ts, lock));
            }
        }
        assert_eq!(wait_table.count(), waiter_info.len());

        for (waiter_ts, lock) in waiter_info {
            let waiter = wait_table
                .take_waiter_by_lock_digest(lock, waiter_ts)
                .unwrap();
            assert_eq!(waiter.start_ts, waiter_ts);
            assert_eq!(waiter.wait_info[0].lock_digest, lock);
        }
        assert_eq!(wait_table.count(), 0);
        assert!(wait_table.wait_table.is_empty());
        assert!(
            wait_table
                .take_waiter_by_lock_digest(
                    LockDigest {
                        ts: TimeStamp::zero(),
                        hash: 0
                    },
                    TimeStamp::zero(),
                )
                .is_none()
        );
    }

    // #[test]
    // fn test_wait_table_add_duplicated_waiter() {
    //     let mut wait_table = WaitTable::new(Arc::new(AtomicUsize::new(0)));
    //     let waiter_ts = 10.into();
    //     let lock = LockDigest {
    //         ts: 20.into(),
    //         hash: 20,
    //     };
    //     assert!(
    //         wait_table
    //             .add_waiter(dummy_waiter(waiter_ts, lock.ts, lock.hash))
    //             .is_none()
    //     );
    //     let waiter = wait_table
    //         .add_waiter(dummy_waiter(waiter_ts, lock.ts, lock.hash))
    //         .unwrap();
    //     assert_eq!(waiter.start_ts, waiter_ts);
    //     assert_eq!(waiter.lock, lock);
    // }
    //
    // #[test]
    // fn test_wait_table_remove_oldest_waiter() {
    //     let mut wait_table = WaitTable::new(Arc::new(AtomicUsize::new(0)));
    //     let lock = LockDigest {
    //         ts: 10.into(),
    //         hash: 10,
    //     };
    //     let waiter_count = 10;
    //     let mut waiters_ts: Vec<TimeStamp> = (0..waiter_count).map(TimeStamp::from).collect();
    //     waiters_ts.shuffle(&mut rand::thread_rng());
    //     for ts in waiters_ts.iter() {
    //         wait_table.add_waiter(dummy_waiter(*ts, lock.ts, lock.hash));
    //     }
    //     assert_eq!(wait_table.count(), waiters_ts.len());
    //     waiters_ts.sort();
    //     for (i, ts) in waiters_ts.into_iter().enumerate() {
    //         let (oldest, others) = wait_table.remove_oldest_waiter(lock).unwrap();
    //         assert_eq!(oldest.start_ts, ts);
    //         assert_eq!(others.len(), waiter_count as usize - i - 1);
    //     }
    //     // There is no waiter in the wait table but there is an entry in it.
    //     assert_eq!(wait_table.count(), 0);
    //     assert_eq!(wait_table.wait_table.len(), 1);
    //     wait_table.remove(lock);
    //     assert!(wait_table.wait_table.is_empty());
    // }

    #[test]
    fn test_wait_table_cancel_by_region() {
        let waiter_count = Arc::new(AtomicUsize::new(0));
        let mut wait_table = WaitTable::new(waiter_count.clone());

        // Cancel a region when there are nothing in it.
        wait_table.cancel_region(10);
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);

        assert!(wait_table.add_waiter(LockWaitToken(Some(1)), dummy_waiter(1.into(), 1.into(), 1)));
        assert!(wait_table.add_waiter(LockWaitToken(Some(2)), dummy_waiter(2.into(), 2.into(), 2)));
        assert!(wait_table.add_waiter(
            LockWaitToken(Some(3)),
            dummy_waiter(3.into(), 3.into(), 3).region_id(2)
        ));
        assert!(wait_table.add_waiter(
            LockWaitToken(Some(4)),
            dummy_waiter(4.into(), 4.into(), 4).region_id(2)
        ));
        assert!(wait_table.add_waiter(
            LockWaitToken(Some(5)),
            dummy_waiter(5.into(), 5.into(), 5).region_id(2)
        ));
        waiter_count.store(5, Ordering::SeqCst);
        assert_eq!(wait_table.region_waiters.len(), 2);

        // Clear one of the two regions.
        wait_table.cancel_region(2);
        assert_eq!(waiter_count.load(Ordering::SeqCst), 2);
        assert_eq!(wait_table.region_waiters.len(), 1);
        assert_eq!(wait_table.wait_table.len(), 2);
        assert_eq!(wait_table.waiter_pool.len(), 2);

        // Clear another region.
        wait_table.cancel_region(1);
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);
        assert_eq!(wait_table.region_waiters.len(), 0);
        assert_eq!(wait_table.wait_table.len(), 0);
        assert_eq!(wait_table.waiter_pool.len(), 0);

        // Epoch / term changing. Test on region 3, and items in region 1 should never be affected.
        waiter_count.fetch_add(1, Ordering::SeqCst);
        assert!(wait_table.add_waiter(
            LockWaitToken(Some(11)),
            dummy_waiter(11.into(), 11.into(), 11).region_id(1)
        ));
        waiter_count.fetch_add(1, Ordering::SeqCst);
        assert!(
            wait_table.add_waiter(
                LockWaitToken(Some(12)),
                dummy_waiter(12.into(), 12.into(), 12)
                    .region_id(3)
                    .epoch(1, 1)
                    .term(1)
            )
        );
        assert_eq!(waiter_count.load(Ordering::SeqCst), 2);
        // Adding a newer one.
        waiter_count.fetch_add(1, Ordering::SeqCst);
        assert!(
            wait_table.add_waiter(
                LockWaitToken(Some(13)),
                dummy_waiter(13.into(), 13.into(), 13)
                    .region_id(3)
                    .epoch(1, 1)
                    .term(2)
            )
        );
        assert_eq!(waiter_count.load(Ordering::SeqCst), 2);
        {
            let entry = wait_table.region_waiters.get(&3).unwrap();
            assert_eq!(entry.0.region_epoch.get_version(), 1);
            assert_eq!(entry.0.region_epoch.get_conf_ver(), 1);
            assert_eq!(entry.0.term.get(), 2);
            assert_eq!(entry.1.len(), 1);
            assert!(entry.1.contains(&LockWaitToken(Some(13))));
        }
        assert_eq!(wait_table.waiter_pool.len(), 2);
        assert!(
            wait_table
                .waiter_pool
                .contains_key(&LockWaitToken(Some(13)))
        );
        assert!(
            !wait_table
                .waiter_pool
                .contains_key(&LockWaitToken(Some(12)))
        );
        assert!(wait_table.wait_table.contains_key(&(13, 13.into())));
        assert!(!wait_table.wait_table.contains_key(&(12, 12.into())));
        // Adding a stale one.
        waiter_count.fetch_add(1, Ordering::SeqCst);
        assert!(
            !wait_table.add_waiter(
                LockWaitToken(Some(14)),
                dummy_waiter(14.into(), 14.into(), 14)
                    .region_id(3)
                    .epoch(1, 1)
                    .term(1)
            )
        );

        assert_eq!(waiter_count.load(Ordering::SeqCst), 2);
        {
            let entry = wait_table.region_waiters.get(&3).unwrap();
            assert_eq!(entry.0.region_epoch.get_version(), 1);
            assert_eq!(entry.0.region_epoch.get_conf_ver(), 1);
            assert_eq!(entry.0.term.get(), 2);
            assert_eq!(entry.1.len(), 1);
            assert!(entry.1.contains(&LockWaitToken(Some(13))));
        }
        assert_eq!(wait_table.waiter_pool.len(), 2);
        assert!(
            wait_table
                .waiter_pool
                .contains_key(&LockWaitToken(Some(13)))
        );
        assert!(
            !wait_table
                .waiter_pool
                .contains_key(&LockWaitToken(Some(12)))
        );
        assert!(wait_table.wait_table.contains_key(&(13, 13.into())));
        assert!(!wait_table.wait_table.contains_key(&(12, 12.into())));

        let waiter = wait_table.take_waiter(LockWaitToken(Some(13))).unwrap();
        assert_eq!(waiter.start_ts, 13.into());
    }

    #[test]
    fn test_wait_table_is_empty() {
        let waiter_count = Arc::new(AtomicUsize::new(0));
        let mut wait_table = WaitTable::new(Arc::clone(&waiter_count));

        let lock = LockDigest {
            ts: 2.into(),
            hash: 2,
        };

        wait_table.add_waiter(
            LockWaitToken(Some(1)),
            dummy_waiter(1.into(), lock.ts, lock.hash),
        );
        // Increase waiter_count manually and assert the previous value is zero
        assert_eq!(waiter_count.fetch_add(1, Ordering::SeqCst), 0);
        // // Adding a duplicated waiter shouldn't increase waiter count.
        // waiter_count.fetch_add(1, Ordering::SeqCst);
        // wait_table.add_waiter(dummy_waiter(1.into(), lock.ts, lock.hash));
        // assert_eq!(waiter_count.load(Ordering::SeqCst), 1);
        // Remove the waiter.
        wait_table
            .take_waiter_by_lock_digest(lock, 1.into())
            .unwrap();
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);
        // Removing a non-existed waiter shouldn't decrease waiter count.
        assert!(
            wait_table
                .take_waiter_by_lock_digest(lock, 1.into())
                .is_none()
        );
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);

        wait_table.add_waiter(
            LockWaitToken(Some(2)),
            dummy_waiter(1.into(), lock.ts, lock.hash),
        );
        wait_table.add_waiter(
            LockWaitToken(Some(3)),
            dummy_waiter(2.into(), lock.ts, lock.hash),
        );
        waiter_count.fetch_add(2, Ordering::SeqCst);
        wait_table.take_waiter(LockWaitToken(Some(3))).unwrap();
        assert_eq!(waiter_count.load(Ordering::SeqCst), 1);
        wait_table.take_waiter(LockWaitToken(Some(2))).unwrap();
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);
        // Removing a non-existed waiter shouldn't decrease waiter count.
        assert!(
            wait_table
                .take_waiter_by_lock_digest(lock, 1.into())
                .is_none()
        );
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_wait_table_to_wait_for_entries() {
        let mut wait_table = WaitTable::new(Arc::new(AtomicUsize::new(0)));
        assert!(wait_table.to_wait_for_entries().is_empty());

        for i in 1..5 {
            for j in 0..i {
                wait_table.add_waiter(
                    LockWaitToken(Some(i * 10 + j)),
                    dummy_waiter((i * 10 + j).into(), i.into(), j),
                );
            }
        }

        let mut wait_for_enties = wait_table.to_wait_for_entries();
        wait_for_enties.sort_by_key(|e| e.txn);
        wait_for_enties.reverse();
        for i in 1..5 {
            for j in 0..i {
                let e = wait_for_enties.pop().unwrap();
                assert_eq!(e.get_txn(), i * 10 + j);
                assert_eq!(e.get_wait_for_txn(), i);
                assert_eq!(e.get_key_hash(), j);
            }
        }
        assert!(wait_for_enties.is_empty());
    }

    fn start_waiter_manager(
        wait_for_lock_timeout: u64,
        wake_up_delay_duration: u64,
    ) -> (FutureWorker<Task>, Scheduler) {
        let detect_worker = FutureWorker::new("dummy-deadlock");
        let detector_scheduler = DetectorScheduler::new(detect_worker.scheduler());

        let cfg = Config {
            wait_for_lock_timeout: ReadableDuration::millis(wait_for_lock_timeout),
            wake_up_delay_duration: ReadableDuration::millis(wake_up_delay_duration),
            ..Default::default()
        };
        let mut waiter_mgr_worker = FutureWorker::new("test-waiter-manager");
        let waiter_mgr_runner =
            WaiterManager::new(Arc::new(AtomicUsize::new(0)), detector_scheduler, &cfg);
        let waiter_mgr_scheduler = Scheduler::new(waiter_mgr_worker.scheduler());
        waiter_mgr_worker.start(waiter_mgr_runner).unwrap();
        (waiter_mgr_worker, waiter_mgr_scheduler)
    }

    #[test]
    fn test_waiter_manager_timeout() {
        let (mut worker, scheduler) = start_waiter_manager(1000, 100);

        // Default timeout
        let (waiter, lock_info, f) = new_test_waiter(10.into(), 20.into(), 20);
        scheduler.wait_for(
            LockWaitToken(Some(1)),
            1,
            RegionEpoch::default(),
            NonZeroU64::new(1).unwrap(),
            waiter.start_ts,
            waiter.wait_info,
            WaitTimeout::Millis(1000),
            waiter.cancel_callback,
            DiagnosticContext::default(),
        );
        assert_elapsed(
            || expect_key_is_locked(block_on(f).unwrap(), lock_info),
            900,
            1200,
        );

        // Custom timeout
        let (waiter, lock_info, f) = new_test_waiter(20.into(), 30.into(), 30);
        scheduler.wait_for(
            LockWaitToken(Some(2)),
            1,
            RegionEpoch::default(),
            NonZeroU64::new(1).unwrap(),
            waiter.start_ts,
            waiter.wait_info,
            WaitTimeout::Millis(100),
            waiter.cancel_callback,
            DiagnosticContext::default(),
        );
        assert_elapsed(
            || expect_key_is_locked(block_on(f).unwrap(), lock_info),
            50,
            300,
        );

        // Timeout can't exceed wait_for_lock_timeout
        let (waiter, lock_info, f) = new_test_waiter(30.into(), 40.into(), 40);
        scheduler.wait_for(
            LockWaitToken(Some(3)),
            1,
            RegionEpoch::default(),
            NonZeroU64::new(1).unwrap(),
            waiter.start_ts,
            waiter.wait_info,
            WaitTimeout::Millis(3000),
            waiter.cancel_callback,
            DiagnosticContext::default(),
        );
        assert_elapsed(
            || expect_key_is_locked(block_on(f).unwrap(), lock_info),
            900,
            1200,
        );

        worker.stop().unwrap();
    }

    // #[test]
    // fn test_waiter_manager_wake_up() {
    //     let (wait_for_lock_timeout, wake_up_delay_duration) = (1000, 100);
    //     let (mut worker, scheduler) =
    //         start_waiter_manager(wait_for_lock_timeout, wake_up_delay_duration);
    //
    //     // Waiters waiting for different locks should be waked up immediately.
    //     let lock_ts = 10.into();
    //     let lock_hashes = vec![10, 11, 12];
    //     let waiters_ts = vec![20.into(), 30.into(), 40.into()];
    //     let mut waiters_info = vec![];
    //     for (&lock_hash, &waiter_ts) in lock_hashes.iter().zip(waiters_ts.iter()) {
    //         let (waiter, lock_info, f) = new_test_waiter(waiter_ts, lock_ts, lock_hash);
    //         scheduler.wait_for(
    //             waiter.start_ts,
    //             waiter.cb,
    //             waiter.pr,
    //             waiter.lock,
    //             WaitTimeout::Millis(wait_for_lock_timeout),
    //             DiagnosticContext::default(),
    //         );
    //         waiters_info.push((waiter_ts, lock_info, f));
    //     }
    //     let commit_ts = 15.into();
    //     scheduler.wake_up(lock_ts, lock_hashes, commit_ts);
    //     for (waiter_ts, lock_info, f) in waiters_info {
    //         assert_elapsed(
    //             || expect_write_conflict(block_on(f).unwrap(), waiter_ts, lock_info, commit_ts),
    //             0,
    //             200,
    //         );
    //     }
    //
    //     // Multiple waiters are waiting for one lock.
    //     let mut lock = LockDigest {
    //         ts: 10.into(),
    //         hash: 10,
    //     };
    //     let mut waiters_ts: Vec<TimeStamp> = (20..25).map(TimeStamp::from).collect();
    //     // Waiters are added in arbitrary order.
    //     waiters_ts.shuffle(&mut rand::thread_rng());
    //     let mut waiters_info = vec![];
    //     for waiter_ts in waiters_ts {
    //         let (waiter, lock_info, f) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
    //         scheduler.wait_for(
    //             waiter.start_ts,
    //             waiter.cb,
    //             waiter.pr,
    //             waiter.lock,
    //             WaitTimeout::Millis(wait_for_lock_timeout),
    //             DiagnosticContext::default(),
    //         );
    //         waiters_info.push((waiter_ts, lock_info, f));
    //     }
    //     waiters_info.sort_by_key(|(ts, ..)| *ts);
    //     let mut commit_ts = 30.into();
    //     // Each waiter should be waked up immediately in order.
    //     for (waiter_ts, mut lock_info, f) in waiters_info.drain(..waiters_info.len() - 1) {
    //         scheduler.wake_up(lock.ts, vec![lock.hash], commit_ts);
    //         lock_info.set_lock_version(lock.ts.into_inner());
    //         assert_elapsed(
    //             || expect_write_conflict(block_on(f).unwrap(), waiter_ts, lock_info, commit_ts),
    //             0,
    //             200,
    //         );
    //         // Now the lock is held by the waked up transaction.
    //         lock.ts = waiter_ts;
    //         commit_ts.incr();
    //     }
    //     // Last waiter isn't waked up by other transactions. It will be waked up after
    //     // wake_up_delay_duration.
    //     let (waiter_ts, mut lock_info, f) = waiters_info.pop().unwrap();
    //     // It conflicts with the last transaction.
    //     lock_info.set_lock_version(lock.ts.into_inner() - 1);
    //     assert_elapsed(
    //         || {
    //             expect_write_conflict(
    //                 block_on(f).unwrap(),
    //                 waiter_ts,
    //                 lock_info,
    //                 *commit_ts.decr(),
    //             )
    //         },
    //         wake_up_delay_duration - 50,
    //         wake_up_delay_duration + 200,
    //     );
    //
    //     // The max lifetime of waiter is its timeout.
    //     let lock = LockDigest {
    //         ts: 10.into(),
    //         hash: 10,
    //     };
    //     let (waiter1, lock_info1, f1) = new_test_waiter(20.into(), lock.ts, lock.hash);
    //     scheduler.wait_for(
    //         waiter1.start_ts,
    //         waiter1.cb,
    //         waiter1.pr,
    //         waiter1.lock,
    //         WaitTimeout::Millis(wait_for_lock_timeout),
    //         DiagnosticContext::default(),
    //     );
    //     let (waiter2, lock_info2, f2) = new_test_waiter(30.into(), lock.ts, lock.hash);
    //     // Waiter2's timeout is 50ms which is less than wake_up_delay_duration.
    //     scheduler.wait_for(
    //         waiter2.start_ts,
    //         waiter2.cb,
    //         waiter2.pr,
    //         waiter2.lock,
    //         WaitTimeout::Millis(50),
    //         DiagnosticContext::default(),
    //     );
    //     let commit_ts = 15.into();
    //     let (tx, rx) = mpsc::sync_channel(1);
    //     std::thread::spawn(move || {
    //         // Waiters2's lifetime can't exceed it timeout.
    //         assert_elapsed(
    //             || expect_write_conflict(block_on(f2).unwrap(), 30.into(), lock_info2, 15.into()),
    //             30,
    //             100,
    //         );
    //         tx.send(()).unwrap();
    //     });
    //     // It will increase waiter2's timeout to wake_up_delay_duration.
    //     scheduler.wake_up(lock.ts, vec![lock.hash], commit_ts);
    //     assert_elapsed(
    //         || expect_write_conflict(block_on(f1).unwrap(), 20.into(), lock_info1, commit_ts),
    //         0,
    //         200,
    //     );
    //     rx.recv().unwrap();
    //
    //     worker.stop().unwrap();
    // }

    #[test]
    fn test_waiter_manager_deadlock() {
        let (mut worker, scheduler) = start_waiter_manager(1000, 100);
        let (waiter_ts, lock) = (
            10.into(),
            LockDigest {
                ts: 20.into(),
                hash: 20,
            },
        );
        let (waiter, lock_info, f) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
        scheduler.wait_for(
            LockWaitToken(Some(1)),
            1,
            RegionEpoch::default(),
            NonZeroU64::new(1).unwrap(),
            waiter.start_ts,
            waiter.wait_info,
            WaitTimeout::Millis(1000),
            waiter.cancel_callback,
            DiagnosticContext::default(),
        );
        scheduler.deadlock(
            LockWaitToken(Some(1)),
            waiter_ts,
            b"foo".to_vec(),
            lock,
            30,
            vec![],
        );
        assert_elapsed(
            || expect_deadlock(block_on(f).unwrap(), waiter_ts, lock_info, 30, &[]),
            0,
            200,
        );
        worker.stop().unwrap();
    }

    // #[test]
    // fn test_waiter_manager_with_duplicated_waiters() {
    //     let (mut worker, scheduler) = start_waiter_manager(1000, 100);
    //     let (waiter_ts, lock) = (
    //         10.into(),
    //         LockDigest {
    //             ts: 20.into(),
    //             hash: 20,
    //         },
    //     );
    //     let (waiter1, lock_info1, f1) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
    //     scheduler.wait_for(
    //         LockWaitToken(Some(1)),
    //         1,
    //         RegionEpoch::default(),
    //         1.into(),
    //         waiter1.start_ts,
    //         waiter1.wait_info,
    //         WaitTimeout::Millis(1000),
    //         waiter1.cb,
    //         DiagnosticContext::default(),
    //     );
    //     let (waiter2, lock_info2, f2) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
    //     scheduler.wait_for(
    //         LockWaitToken(Some(2)),
    //         1,
    //         RegionEpoch::default(),
    //         1.into(),
    //         waiter2.start_ts,
    //         waiter2.wait_info,
    //         WaitTimeout::Millis(1000),
    //         waiter2.cb,
    //         DiagnosticContext::default(),
    //     );
    //     // Should notify duplicated waiter immediately.
    //     assert_elapsed(
    //         || expect_key_is_locked(block_on(f1).unwrap(), lock_info1),
    //         0,
    //         200,
    //     );
    //     // The new waiter will be wake up after timeout.
    //     assert_elapsed(
    //         || expect_key_is_locked(block_on(f2).unwrap(), lock_info2),
    //         900,
    //         1200,
    //     );
    //
    //     worker.stop().unwrap();
    // }

    // #[bench]
    // fn bench_wake_up_small_table_against_big_hashes(b: &mut test::Bencher) {
    //     let detect_worker = FutureWorker::new("dummy-deadlock");
    //     let detector_scheduler = DetectorScheduler::new(detect_worker.scheduler());
    //     let mut waiter_mgr = WaiterManager::new(
    //         Arc::new(AtomicUsize::new(0)),
    //         detector_scheduler,
    //         &Config::default(),
    //     );
    //     waiter_mgr.wait_table.borrow_mut().add_waiter(
    //         LockWaitToken(Some(1)),
    //         dummy_waiter(10.into(), 20.into(), 10000),
    //     );
    //     let hashes: Vec<u64> = (0..1000).collect();
    //     b.iter(|| {
    //         waiter_mgr.handle_remove_lock_wait(20.into(), hashes.clone(), 30.into());
    //     });
    // }
}

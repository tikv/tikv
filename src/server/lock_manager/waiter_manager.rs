// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::config::Config;
use super::deadlock::Scheduler as DetectorScheduler;
use super::metrics::*;
use crate::storage::lock_manager::Lock;
use crate::storage::mvcc::Error as MvccError;
use crate::storage::txn::{execute_callback, Error as TxnError, ProcessResult};
use crate::storage::{Error as StorageError, StorageCb};
use tikv_util::collections::HashMap;
use tikv_util::worker::{FutureRunnable, FutureScheduler, Stopped};

use std::cell::RefCell;
use std::fmt::{self, Debug, Display, Formatter};
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use futures::{Async, Future, Poll};
use kvproto::deadlock::WaitForEntry;
use prometheus::HistogramTimer;
use tokio_core::reactor::Handle;

struct DelayInner {
    timer: tokio_timer::Delay,
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
            timer: tokio_timer::Delay::new(deadline),
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
            self.inner.borrow_mut().timer.reset(deadline);
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
    type Item = bool;
    type Error = tokio_timer::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.is_cancelled() {
            return Ok(Async::Ready(false));
        }
        self.inner
            .borrow_mut()
            .timer
            .poll()
            .map(|ready| ready.map(|_| true))
    }
}

pub type Callback = Box<dyn FnOnce(Vec<WaitForEntry>) + Send>;

pub enum Task {
    WaitFor {
        // which txn waits for the lock
        start_ts: u64,
        cb: StorageCb,
        pr: ProcessResult,
        lock: Lock,
        timeout: u64,
    },
    WakeUp {
        // lock info
        lock_ts: u64,
        hashes: Vec<u64>,
        commit_ts: u64,
    },
    Dump {
        cb: Callback,
    },
    Deadlock {
        // Which txn causes deadlock
        start_ts: u64,
        lock: Lock,
        deadlock_key_hash: u64,
    },
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
            Task::WaitFor { start_ts, lock, .. } => {
                write!(f, "txn:{} waiting for {}:{}", start_ts, lock.ts, lock.hash)
            }
            Task::WakeUp { lock_ts, .. } => write!(f, "waking up txns waiting for {}", lock_ts),
            Task::Dump { .. } => write!(f, "dump"),
            Task::Deadlock { start_ts, .. } => write!(f, "txn:{} deadlock", start_ts),
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
    pub(crate) start_ts: u64,
    pub(crate) cb: StorageCb,
    /// The result of `Command::AcquirePessimisticLock`.
    ///
    /// It contains a `KeyIsLocked` error at the beginning. It will be changed
    /// to `WriteConflict` error if the lock is released or `Deadlock` error if
    /// it causes deadlock.
    pub(crate) pr: ProcessResult,
    pub(crate) lock: Lock,
    delay: Delay,
    _lifetime_timer: HistogramTimer,
}

impl Waiter {
    fn new(start_ts: u64, cb: StorageCb, pr: ProcessResult, lock: Lock, deadline: Instant) -> Self {
        Self {
            start_ts,
            cb,
            pr,
            lock,
            delay: Delay::new(deadline),
            _lifetime_timer: WAITER_LIFETIME_HISTOGRAM.start_coarse_timer(),
        }
    }

    /// The `F` will be invoked if the `Waiter` times out normally.
    fn on_timeout<F: FnOnce()>(&self, f: F) -> impl Future<Item = (), Error = ()> {
        let timer = self.delay.clone();
        timer
            .map_err(|e| warn!("waiter delay timer errored"; "err" => ?e))
            .then(move |res| {
                match res {
                    // The timer is cancelled. Don't call timeout handler.
                    Ok(false) => (),
                    // Timer times out or error occurs.
                    // It should call timeout handler to prevent starvation.
                    _ => f(),
                };
                Ok(())
            })
    }

    fn reset_timeout(&self, deadline: Instant) {
        self.delay.reset(deadline);
    }

    /// `Notify` consumes the `Waiter` to notify the corresponding transaction
    /// going on.
    fn notify(self) {
        // Cancel the delay timer to prevent removing the same `Waiter` earlier.
        self.delay.cancel();
        execute_callback(self.cb, self.pr);
    }

    /// Changes the `ProcessResult` to `WriteConflict`.
    /// It may be invoked more than once.
    fn conflict_with(&mut self, lock_ts: u64, commit_ts: u64) {
        let (key, primary) = self.extract_key_info();
        let mvcc_err = MvccError::WriteConflict {
            start_ts: self.start_ts,
            conflict_start_ts: lock_ts,
            conflict_commit_ts: commit_ts,
            key,
            primary,
        };
        self.pr = ProcessResult::Failed {
            err: StorageError::from(TxnError::from(mvcc_err)),
        };
    }

    /// Changes the `ProcessResult` to `Deadlock`.
    fn deadlock_with(&mut self, deadlock_key_hash: u64) {
        let (key, _) = self.extract_key_info();
        let mvcc_err = MvccError::Deadlock {
            start_ts: self.start_ts,
            lock_ts: self.lock.ts,
            lock_key: key,
            deadlock_key_hash,
        };
        self.pr = ProcessResult::Failed {
            err: StorageError::from(TxnError::from(mvcc_err)),
        };
    }

    /// Extracts key and primary key from `ProcessResult`.
    fn extract_key_info(&mut self) -> (Vec<u8>, Vec<u8>) {
        match &mut self.pr {
            ProcessResult::MultiRes { results } => match results.pop().expect("mustn't be empty") {
                Err(StorageError::Txn(TxnError::Mvcc(MvccError::KeyIsLocked(mut info)))) => {
                    (info.take_key(), info.take_primary_lock())
                }
                _ => panic!("unexpected mvcc error"),
            },
            ProcessResult::Failed { err } => match err {
                StorageError::Txn(TxnError::Mvcc(MvccError::WriteConflict {
                    ref mut key,
                    ref mut primary,
                    ..
                })) => (
                    std::mem::replace(key, Vec::new()),
                    std::mem::replace(primary, Vec::new()),
                ),
                _ => panic!("unexpected mvcc error"),
            },
            _ => panic!("unexpected progress result"),
        }
    }
}

// NOTE: Now we assume `Waiters` is not very long.
// Maybe needs to use `BinaryHeap` or sorted `VecDeque` instead.
type Waiters = Vec<Waiter>;

struct WaitTable {
    // Map lock hash to waiters.
    wait_table: HashMap<u64, Waiters>,
    waiter_count: Arc<AtomicUsize>,
}

impl WaitTable {
    fn new(waiter_count: Arc<AtomicUsize>) -> Self {
        Self {
            wait_table: HashMap::default(),
            waiter_count,
        }
    }

    #[cfg(test)]
    fn count(&self) -> usize {
        self.wait_table.iter().map(|(_, v)| v.len()).sum()
    }

    fn is_empty(&self) -> bool {
        self.wait_table.is_empty()
    }

    /// Returns the duplicated `Waiter` if there is.
    fn add_waiter(&mut self, waiter: Waiter) -> Option<Waiter> {
        let waiters = self.wait_table.entry(waiter.lock.hash).or_insert_with(|| {
            WAIT_TABLE_STATUS_GAUGE.locks.inc();
            Waiters::default()
        });
        let old_idx = waiters.iter().position(|w| w.start_ts == waiter.start_ts);
        waiters.push(waiter);
        if let Some(old_idx) = old_idx {
            let old = waiters.swap_remove(old_idx);
            self.waiter_count.fetch_sub(1, Ordering::SeqCst);
            Some(old)
        } else {
            WAIT_TABLE_STATUS_GAUGE.txns.inc();
            None
        }
        // Here we don't increase waiter_count because it's already updated in LockManager::wait_for()
    }

    /// Removes all waiters waiting for the lock.
    fn remove(&mut self, lock: Lock) {
        self.wait_table.remove(&lock.hash);
        WAIT_TABLE_STATUS_GAUGE.locks.dec();
    }

    fn remove_waiter(&mut self, lock: Lock, waiter_ts: u64) -> Option<Waiter> {
        let waiters = self.wait_table.get_mut(&lock.hash)?;
        let idx = waiters
            .iter()
            .position(|waiter| waiter.start_ts == waiter_ts)?;
        let waiter = waiters.swap_remove(idx);
        self.waiter_count.fetch_sub(1, Ordering::SeqCst);
        WAIT_TABLE_STATUS_GAUGE.txns.dec();
        if waiters.is_empty() {
            self.remove(lock);
        }
        Some(waiter)
    }

    /// Removes the `Waiter` with the smallest start ts and returns it with remaining waiters.
    ///
    /// NOTE: Due to the borrow checker, it doesn't remove the entry in the `WaitTable`
    /// even if there is no remaining waiter.
    fn remove_oldest_waiter(&mut self, lock: Lock) -> Option<(Waiter, &mut Waiters)> {
        let waiters = self.wait_table.get_mut(&lock.hash)?;
        let oldest_idx = waiters
            .iter()
            .enumerate()
            .min_by_key(|(_, w)| w.start_ts)
            .unwrap()
            .0;
        let oldest = waiters.swap_remove(oldest_idx);
        self.waiter_count.fetch_sub(1, Ordering::SeqCst);
        WAIT_TABLE_STATUS_GAUGE.txns.dec();
        Some((oldest, waiters))
    }

    fn to_wait_for_entries(&self) -> Vec<WaitForEntry> {
        self.wait_table
            .iter()
            .flat_map(|(_, waiters)| {
                waiters.iter().map(|waiter| {
                    let mut wait_for_entry = WaitForEntry::new();
                    wait_for_entry.set_txn(waiter.start_ts);
                    wait_for_entry.set_wait_for_txn(waiter.lock.ts);
                    wait_for_entry.set_key_hash(waiter.lock.hash);
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
            if let Task::WaitFor { cb, pr, .. } = task {
                execute_callback(cb, pr);
            }
            return false;
        }
        true
    }

    pub fn wait_for(
        &self,
        start_ts: u64,
        cb: StorageCb,
        pr: ProcessResult,
        lock: Lock,
        timeout: u64,
    ) {
        self.notify_scheduler(Task::WaitFor {
            start_ts,
            cb,
            pr,
            lock,
            timeout,
        });
    }

    pub fn wake_up(&self, lock_ts: u64, hashes: Vec<u64>, commit_ts: u64) {
        self.notify_scheduler(Task::WakeUp {
            lock_ts,
            hashes,
            commit_ts,
        });
    }

    pub fn dump_wait_table(&self, cb: Callback) -> bool {
        self.notify_scheduler(Task::Dump { cb })
    }

    pub fn deadlock(&self, txn_ts: u64, lock: Lock, deadlock_key_hash: u64) {
        self.notify_scheduler(Task::Deadlock {
            start_ts: txn_ts,
            lock,
            deadlock_key_hash,
        });
    }
}

/// WaiterManager handles waiting and wake-up of pessimistic lock
pub struct WaiterManager {
    wait_table: Rc<RefCell<WaitTable>>,
    detector_scheduler: DetectorScheduler,
    /// It is the default and maximum timeout of waiter.
    default_wait_for_lock_timeout: u64,
    /// If more than one waiters are waiting for the same lock, only the
    /// oldest one will be waked up immediately when the lock is released.
    /// Others will be waked up after `wake_up_delay_duration` to reduce
    /// contention and make the oldest one more likely acquires the lock.
    wake_up_delay_duration: u64,
}

unsafe impl Send for WaiterManager {}

impl WaiterManager {
    pub fn new(
        waiter_count: Arc<AtomicUsize>,
        detector_scheduler: DetectorScheduler,
        cfg: &Config,
    ) -> Self {
        Self {
            wait_table: Rc::new(RefCell::new(WaitTable::new(waiter_count))),
            detector_scheduler,
            default_wait_for_lock_timeout: cfg.wait_for_lock_timeout,
            wake_up_delay_duration: cfg.wake_up_delay_duration,
        }
    }

    pub fn normalize_deadline(&self, timeout: u64) -> Instant {
        let timeout = if timeout == 0 || timeout > self.default_wait_for_lock_timeout {
            self.default_wait_for_lock_timeout
        } else {
            timeout
        };
        Instant::now() + Duration::from_millis(timeout)
    }

    fn handle_wait_for(&mut self, handle: &Handle, waiter: Waiter) {
        let (waiter_ts, lock) = (waiter.start_ts, waiter.lock);
        let wait_table = self.wait_table.clone();
        let detector_scheduler = self.detector_scheduler.clone();
        // Remove the waiter from wait table when it times out.
        let f = waiter.on_timeout(move || {
            wait_table
                .borrow_mut()
                .remove_waiter(lock, waiter_ts)
                .and_then(|waiter| {
                    detector_scheduler.clean_up_wait_for(waiter.start_ts, waiter.lock);
                    waiter.notify();
                    Some(())
                });
        });
        self.wait_table
            .borrow_mut()
            .add_waiter(waiter)
            .and_then(|old| {
                old.notify();
                Some(())
            });
        handle.spawn(f);
    }

    fn handle_wake_up(&mut self, lock_ts: u64, hashes: Vec<u64>, commit_ts: u64) {
        let mut wait_table = self.wait_table.borrow_mut();
        if wait_table.is_empty() {
            return;
        }
        let new_timeout = Instant::now() + Duration::from_millis(self.wake_up_delay_duration);
        for hash in hashes {
            let lock = Lock { ts: lock_ts, hash };
            if let Some((mut oldest, others)) = wait_table.remove_oldest_waiter(lock) {
                // Notify the oldest one immediately.
                self.detector_scheduler
                    .clean_up_wait_for(oldest.start_ts, oldest.lock);
                oldest.conflict_with(lock_ts, commit_ts);
                oldest.notify();
                // Others will be waked up after `wake_up_delay_duration`.
                //
                // NOTE: Actually these waiters are waiting for an unknown transaction.
                // If there is a deadlock between them, it will be detected after timeout.
                if others.is_empty() {
                    // Remove the empty entry here.
                    wait_table.remove(lock);
                } else {
                    others.iter_mut().for_each(|waiter| {
                        waiter.conflict_with(lock_ts, commit_ts);
                        waiter.reset_timeout(new_timeout);
                    });
                }
            }
        }
    }

    fn handle_dump(&self, cb: Callback) {
        cb(self.wait_table.borrow().to_wait_for_entries());
    }

    fn handle_deadlock(&mut self, waiter_ts: u64, lock: Lock, deadlock_key_hash: u64) {
        self.wait_table
            .borrow_mut()
            .remove_waiter(lock, waiter_ts)
            .and_then(|mut waiter| {
                waiter.deadlock_with(deadlock_key_hash);
                waiter.notify();
                Some(())
            });
    }
}

impl FutureRunnable<Task> for WaiterManager {
    fn run(&mut self, task: Task, handle: &Handle) {
        match task {
            Task::WaitFor {
                start_ts,
                cb,
                pr,
                lock,
                timeout,
            } => {
                let waiter = Waiter::new(start_ts, cb, pr, lock, self.normalize_deadline(timeout));
                self.handle_wait_for(handle, waiter);
                TASK_COUNTER_VEC.wait_for.inc();
            }
            Task::WakeUp {
                lock_ts,
                hashes,
                commit_ts,
            } => {
                self.handle_wake_up(lock_ts, hashes, commit_ts);
                TASK_COUNTER_VEC.wake_up.inc();
            }
            Task::Dump { cb } => {
                self.handle_dump(cb);
                TASK_COUNTER_VEC.dump.inc();
            }
            Task::Deadlock {
                start_ts,
                lock,
                deadlock_key_hash,
            } => {
                self.handle_deadlock(start_ts, lock, deadlock_key_hash);
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use tikv_util::future::paired_future_callback;
    use tikv_util::worker::FutureWorker;

    use std::sync::mpsc;

    use kvproto::kvrpcpb::LockInfo;
    use rand::prelude::*;
    use tokio_core::reactor::Core;

    fn dummy_waiter(start_ts: u64, lock_ts: u64, hash: u64) -> Waiter {
        Waiter {
            start_ts,
            cb: StorageCb::Boolean(Box::new(|_| ())),
            pr: ProcessResult::Res,
            lock: Lock { ts: lock_ts, hash },
            delay: Delay::new(Instant::now()),
            _lifetime_timer: WAITER_LIFETIME_HISTOGRAM.start_coarse_timer(),
        }
    }

    pub(crate) fn assert_elapsed<F: FnOnce()>(f: F, min: u64, max: u64) {
        let now = Instant::now();
        f();
        let elapsed = now.elapsed();
        assert!(
            Duration::from_millis(min) <= elapsed && elapsed < Duration::from_millis(max),
            "elapsed: {:?}",
            elapsed
        );
    }

    #[test]
    fn test_delay() {
        // tokio_timer can only run with tokio executor.
        let mut core = Core::new().unwrap();
        let delay = Delay::new(Instant::now() + Duration::from_millis(100));
        assert_elapsed(
            || {
                core.run(delay.map(|not_cancelled| assert!(not_cancelled)))
                    .unwrap()
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
                core.run(delay.map(|not_cancelled| assert!(not_cancelled)))
                    .unwrap()
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
                core.run(delay.map(|not_cancelled| assert!(not_cancelled)))
                    .unwrap()
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
                core.run(delay.map(|not_cancelled| assert!(!not_cancelled)))
                    .unwrap()
            },
            0,
            200,
        );
    }

    // Make clippy happy.
    pub(crate) type WaiterCtx = (
        Waiter,
        LockInfo,
        futures::sync::oneshot::Receiver<Result<Vec<Result<(), StorageError>>, StorageError>>,
    );

    pub(crate) fn new_test_waiter(waiter_ts: u64, lock_ts: u64, lock_hash: u64) -> WaiterCtx {
        let raw_key = b"foo".to_vec();
        let primary = b"bar".to_vec();
        let mut info = LockInfo::default();
        info.set_key(raw_key.clone());
        info.set_lock_version(lock_ts);
        info.set_primary_lock(primary.clone());
        info.set_lock_ttl(3000);
        info.set_txn_size(16);
        let pr = ProcessResult::MultiRes {
            results: vec![Err(StorageError::from(TxnError::from(
                MvccError::KeyIsLocked(info.clone()),
            )))],
        };
        let lock = Lock {
            ts: lock_ts,
            hash: lock_hash,
        };
        let (cb, f) = paired_future_callback();
        let waiter = Waiter::new(
            waiter_ts,
            StorageCb::Booleans(cb),
            pr,
            lock,
            Instant::now() + Duration::from_millis(3000),
        );
        (waiter, info, f)
    }

    #[test]
    fn test_waiter_extract_key_info() {
        let (mut waiter, mut lock_info, _) = new_test_waiter(10, 20, 20);
        assert_eq!(
            waiter.extract_key_info(),
            (lock_info.take_key(), lock_info.take_primary_lock())
        );

        let (mut waiter, mut lock_info, _) = new_test_waiter(10, 20, 20);
        waiter.conflict_with(20, 30);
        assert_eq!(
            waiter.extract_key_info(),
            (lock_info.take_key(), lock_info.take_primary_lock())
        );
    }

    pub(crate) fn expect_key_is_locked<T: Debug>(
        res: Result<T, StorageError>,
        lock_info: LockInfo,
    ) {
        match res {
            Err(StorageError::Txn(TxnError::Mvcc(MvccError::KeyIsLocked(res)))) => {
                assert_eq!(res, lock_info)
            }
            e => panic!("unexpected error: {:?}", e),
        }
    }

    pub(crate) fn expect_write_conflict<T: Debug>(
        res: Result<T, StorageError>,
        waiter_ts: u64,
        mut lock_info: LockInfo,
        commit_ts: u64,
    ) {
        match res {
            Err(StorageError::Txn(TxnError::Mvcc(MvccError::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key,
                primary,
            }))) => {
                assert_eq!(start_ts, waiter_ts);
                assert_eq!(conflict_start_ts, lock_info.get_lock_version());
                assert_eq!(conflict_commit_ts, commit_ts);
                assert_eq!(key, lock_info.take_key());
                assert_eq!(primary, lock_info.take_primary_lock());
            }
            e => panic!("unexpected error: {:?}", e),
        }
    }

    pub(crate) fn expect_deadlock<T: Debug>(
        res: Result<T, StorageError>,
        waiter_ts: u64,
        mut lock_info: LockInfo,
        deadlock_hash: u64,
    ) {
        match res {
            Err(StorageError::Txn(TxnError::Mvcc(MvccError::Deadlock {
                start_ts,
                lock_ts,
                lock_key,
                deadlock_key_hash,
            }))) => {
                assert_eq!(start_ts, waiter_ts);
                assert_eq!(lock_ts, lock_info.get_lock_version());
                assert_eq!(lock_key, lock_info.take_key());
                assert_eq!(deadlock_key_hash, deadlock_hash);
            }
            e => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_waiter_notify() {
        let (waiter, lock_info, f) = new_test_waiter(10, 20, 20);
        waiter.notify();
        expect_key_is_locked(f.wait().unwrap().unwrap().pop().unwrap(), lock_info);

        // A waiter can conflict with other transactions more than once.
        for conflict_times in 1..=3 {
            let waiter_ts = 10;
            let mut lock_ts = 20;
            let (mut waiter, mut lock_info, f) = new_test_waiter(waiter_ts, lock_ts, 20);
            let mut conflict_commit_ts = 30;
            for _ in 0..conflict_times {
                lock_ts += 1;
                conflict_commit_ts += 1;
                waiter.conflict_with(lock_ts, conflict_commit_ts);
                lock_info.set_lock_version(lock_ts);
            }
            waiter.notify();
            expect_write_conflict(f.wait().unwrap(), waiter_ts, lock_info, conflict_commit_ts);
        }

        // Deadlock
        let waiter_ts = 10;
        let (mut waiter, lock_info, f) = new_test_waiter(waiter_ts, 20, 20);
        waiter.deadlock_with(111);
        waiter.notify();
        expect_deadlock(f.wait().unwrap(), waiter_ts, lock_info, 111);

        // Conflict then deadlock.
        let waiter_ts = 10;
        let (mut waiter, lock_info, f) = new_test_waiter(waiter_ts, 20, 20);
        waiter.conflict_with(20, 30);
        waiter.deadlock_with(111);
        waiter.notify();
        expect_deadlock(f.wait().unwrap(), waiter_ts, lock_info, 111);
    }

    #[test]
    fn test_waiter_on_timeout() {
        let mut core = Core::new().unwrap();

        // The timeout handler should be invoked after timeout.
        let (waiter, _, _) = new_test_waiter(10, 20, 20);
        waiter.reset_timeout(Instant::now() + Duration::from_millis(100));
        let (tx, rx) = mpsc::sync_channel(1);
        let f = waiter.on_timeout(move || tx.send(1).unwrap());
        assert_elapsed(|| core.run(f).unwrap(), 50, 200);
        rx.try_recv().unwrap();

        // The timeout handler shouldn't be invoked after waiter has been notified.
        let (waiter, _, _) = new_test_waiter(10, 20, 20);
        waiter.reset_timeout(Instant::now() + Duration::from_millis(100));
        let (tx, rx) = mpsc::sync_channel(1);
        let f = waiter.on_timeout(move || tx.send(1).unwrap());
        waiter.notify();
        assert_elapsed(|| core.run(f).unwrap(), 0, 200);
        rx.try_recv().unwrap_err();
    }

    #[test]
    fn test_wait_table_add_and_remove() {
        let mut wait_table = WaitTable::new(Arc::new(AtomicUsize::new(0)));
        let mut waiter_info = Vec::new();
        let mut rng = rand::thread_rng();
        for _ in 0..20 {
            let waiter_ts = rng.gen::<u64>();
            let lock = Lock {
                ts: rng.gen::<u64>(),
                hash: rng.gen(),
            };
            // Avoid adding duplicated waiter.
            if wait_table
                .add_waiter(dummy_waiter(waiter_ts, lock.ts, lock.hash))
                .is_none()
            {
                waiter_info.push((waiter_ts, lock));
            }
        }
        assert_eq!(wait_table.count(), waiter_info.len());

        for (waiter_ts, lock) in waiter_info {
            let waiter = wait_table.remove_waiter(lock, waiter_ts).unwrap();
            assert_eq!(waiter.start_ts, waiter_ts);
            assert_eq!(waiter.lock, lock);
        }
        assert_eq!(wait_table.count(), 0);
        assert!(wait_table.wait_table.is_empty());
        assert!(wait_table
            .remove_waiter(Lock { ts: 0, hash: 0 }, 0,)
            .is_none());
    }

    #[test]
    fn test_wait_table_add_duplicated_waiter() {
        let mut wait_table = WaitTable::new(Arc::new(AtomicUsize::new(0)));
        let waiter_ts = 10;
        let lock = Lock { ts: 20, hash: 20 };
        assert!(wait_table
            .add_waiter(dummy_waiter(waiter_ts, lock.ts, lock.hash))
            .is_none());
        let waiter = wait_table
            .add_waiter(dummy_waiter(waiter_ts, lock.ts, lock.hash))
            .unwrap();
        assert_eq!(waiter.start_ts, waiter_ts);
        assert_eq!(waiter.lock, lock);
    }

    #[test]
    fn test_wait_table_remove_oldest_waiter() {
        let mut wait_table = WaitTable::new(Arc::new(AtomicUsize::new(0)));
        let lock = Lock { ts: 10, hash: 10 };
        let waiter_count = 10;
        let mut waiters_ts: Vec<u64> = (0..waiter_count).collect();
        waiters_ts.shuffle(&mut rand::thread_rng());
        for ts in waiters_ts.iter() {
            wait_table.add_waiter(dummy_waiter(*ts, lock.ts, lock.hash));
        }
        assert_eq!(wait_table.count(), waiters_ts.len());
        waiters_ts.sort();
        for (i, ts) in waiters_ts.into_iter().enumerate() {
            let (oldest, others) = wait_table.remove_oldest_waiter(lock).unwrap();
            assert_eq!(oldest.start_ts, ts);
            assert_eq!(others.len(), waiter_count as usize - i - 1);
        }
        // There is no waiter in the wait table but there is an entry in it.
        assert_eq!(wait_table.count(), 0);
        assert_eq!(wait_table.wait_table.len(), 1);
        wait_table.remove(lock);
        assert!(wait_table.wait_table.is_empty());
    }

    #[test]
    fn test_wait_table_is_empty() {
        let waiter_count = Arc::new(AtomicUsize::new(0));
        let mut wait_table = WaitTable::new(Arc::clone(&waiter_count));

        let lock = Lock { ts: 2, hash: 2 };
        wait_table.add_waiter(dummy_waiter(1, lock.ts, lock.hash));
        // Increase waiter_count manually and assert the previous value is zero
        assert_eq!(waiter_count.fetch_add(1, Ordering::SeqCst), 0);
        // Adding a duplicated waiter shouldn't increase waiter count.
        waiter_count.fetch_add(1, Ordering::SeqCst);
        wait_table.add_waiter(dummy_waiter(1, lock.ts, lock.hash));
        assert_eq!(waiter_count.load(Ordering::SeqCst), 1);
        // Remove the waiter.
        wait_table.remove_waiter(lock, 1).unwrap();
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);
        // Removing a non-existed waiter shouldn't decrease waiter count.
        assert!(wait_table.remove_waiter(lock, 1).is_none());
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);

        wait_table.add_waiter(dummy_waiter(1, lock.ts, lock.hash));
        wait_table.add_waiter(dummy_waiter(2, lock.ts, lock.hash));
        waiter_count.fetch_add(2, Ordering::SeqCst);
        wait_table.remove_oldest_waiter(lock).unwrap();
        assert_eq!(waiter_count.load(Ordering::SeqCst), 1);
        wait_table.remove_oldest_waiter(lock).unwrap();
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);
        wait_table.remove(lock);
        // Removing a non-existed waiter shouldn't decrease waiter count.
        assert!(wait_table.remove_oldest_waiter(lock).is_none());
        assert_eq!(waiter_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_wait_table_to_wait_for_entries() {
        let mut wait_table = WaitTable::new(Arc::new(AtomicUsize::new(0)));
        assert!(wait_table.to_wait_for_entries().is_empty());

        for i in 1..5 {
            for j in 0..i {
                wait_table.add_waiter(dummy_waiter(i * 10 + j, i, j));
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

        let mut cfg = Config::default();
        cfg.wait_for_lock_timeout = wait_for_lock_timeout;
        cfg.wake_up_delay_duration = wake_up_delay_duration;
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
        let (waiter, lock_info, f) = new_test_waiter(10, 20, 20);
        scheduler.wait_for(waiter.start_ts, waiter.cb, waiter.pr, waiter.lock, 1000);
        assert_elapsed(
            || expect_key_is_locked(f.wait().unwrap().unwrap().pop().unwrap(), lock_info),
            900,
            1200,
        );

        // Custom timeout
        let (waiter, lock_info, f) = new_test_waiter(20, 30, 30);
        scheduler.wait_for(waiter.start_ts, waiter.cb, waiter.pr, waiter.lock, 100);
        assert_elapsed(
            || expect_key_is_locked(f.wait().unwrap().unwrap().pop().unwrap(), lock_info),
            50,
            300,
        );

        // Timeout can't exceed wait_for_lock_timeout
        let (waiter, lock_info, f) = new_test_waiter(30, 40, 40);
        scheduler.wait_for(waiter.start_ts, waiter.cb, waiter.pr, waiter.lock, 3000);
        assert_elapsed(
            || expect_key_is_locked(f.wait().unwrap().unwrap().pop().unwrap(), lock_info),
            900,
            1200,
        );

        worker.stop().unwrap();
    }

    #[test]
    fn test_waiter_manager_wake_up() {
        let (wait_for_lock_timeout, wake_up_delay_duration) = (1000, 100);
        let (mut worker, scheduler) =
            start_waiter_manager(wait_for_lock_timeout, wake_up_delay_duration);

        // Waiters waiting for different locks should be waked up immediately.
        let lock_ts = 10;
        let lock_hashes = vec![10, 11, 12];
        let waiters_ts = vec![20, 30, 40];
        let mut waiters_info = vec![];
        for (&lock_hash, &waiter_ts) in lock_hashes.iter().zip(waiters_ts.iter()) {
            let (waiter, lock_info, f) = new_test_waiter(waiter_ts, lock_ts, lock_hash);
            scheduler.wait_for(
                waiter.start_ts,
                waiter.cb,
                waiter.pr,
                waiter.lock,
                wait_for_lock_timeout,
            );
            waiters_info.push((waiter_ts, lock_info, f));
        }
        let commit_ts = 15;
        scheduler.wake_up(lock_ts, lock_hashes, commit_ts);
        for (waiter_ts, lock_info, f) in waiters_info {
            assert_elapsed(
                || expect_write_conflict(f.wait().unwrap(), waiter_ts, lock_info, commit_ts),
                0,
                200,
            );
        }

        // Multiple waiters are waiting for one lock.
        let mut lock = Lock { ts: 10, hash: 10 };
        let mut waiters_ts: Vec<u64> = (20..25).collect();
        // Waiters are added in arbitrary order.
        waiters_ts.shuffle(&mut rand::thread_rng());
        let mut waiters_info = vec![];
        for waiter_ts in waiters_ts {
            let (waiter, lock_info, f) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
            scheduler.wait_for(
                waiter.start_ts,
                waiter.cb,
                waiter.pr,
                waiter.lock,
                wait_for_lock_timeout,
            );
            waiters_info.push((waiter_ts, lock_info, f));
        }
        waiters_info.sort_by_key(|(ts, _, _)| *ts);
        let mut commit_ts = 30;
        // Each waiter should be waked up immediately in order.
        for (waiter_ts, mut lock_info, f) in waiters_info.drain(..waiters_info.len() - 1) {
            scheduler.wake_up(lock.ts, vec![lock.hash], commit_ts);
            lock_info.set_lock_version(lock.ts);
            assert_elapsed(
                || expect_write_conflict(f.wait().unwrap(), waiter_ts, lock_info, commit_ts),
                0,
                200,
            );
            // Now the lock is held by the waked up transaction.
            lock.ts = waiter_ts;
            commit_ts += 1;
        }
        // Last waiter isn't waked up by other transactions. It will be waked up after
        // wake_up_delay_duration.
        let (waiter_ts, mut lock_info, f) = waiters_info.pop().unwrap();
        // It conflicts with the last transaction.
        lock_info.set_lock_version(lock.ts - 1);
        assert_elapsed(
            || expect_write_conflict(f.wait().unwrap(), waiter_ts, lock_info, commit_ts - 1),
            wake_up_delay_duration - 50,
            wake_up_delay_duration + 200,
        );

        // The max lifetime of waiter is its timeout.
        let lock = Lock { ts: 10, hash: 10 };
        let (waiter1, lock_info1, f1) = new_test_waiter(20, lock.ts, lock.hash);
        scheduler.wait_for(
            waiter1.start_ts,
            waiter1.cb,
            waiter1.pr,
            waiter1.lock,
            wait_for_lock_timeout,
        );
        let (waiter2, lock_info2, f2) = new_test_waiter(30, lock.ts, lock.hash);
        // Waiter2's timeout is 50ms which is less than wake_up_delay_duration.
        scheduler.wait_for(waiter2.start_ts, waiter2.cb, waiter2.pr, waiter2.lock, 50);
        let commit_ts = 15;
        let (tx, rx) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            // Waiters2's lifetime can't exceed it timeout.
            assert_elapsed(
                || expect_write_conflict(f2.wait().unwrap(), 30, lock_info2, 15),
                30,
                100,
            );
            tx.send(()).unwrap();
        });
        // It will increase waiter2's timeout to wake_up_delay_duration.
        scheduler.wake_up(lock.ts, vec![lock.hash], commit_ts);
        assert_elapsed(
            || expect_write_conflict(f1.wait().unwrap(), 20, lock_info1, commit_ts),
            0,
            200,
        );
        rx.recv().unwrap();

        worker.stop().unwrap();
    }

    #[test]
    fn test_waiter_manager_deadlock() {
        let (mut worker, scheduler) = start_waiter_manager(1000, 100);
        let (waiter_ts, lock) = (10, Lock { ts: 20, hash: 20 });
        let (waiter, lock_info, f) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
        scheduler.wait_for(waiter.start_ts, waiter.cb, waiter.pr, waiter.lock, 1000);
        scheduler.deadlock(waiter_ts, lock, 30);
        assert_elapsed(
            || expect_deadlock(f.wait().unwrap(), waiter_ts, lock_info, 30),
            0,
            200,
        );
        worker.stop().unwrap();
    }

    #[test]
    fn test_waiter_manager_with_duplicated_waiters() {
        let (mut worker, scheduler) = start_waiter_manager(1000, 100);
        let (waiter_ts, lock) = (10, Lock { ts: 20, hash: 20 });
        let (waiter1, lock_info1, f1) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
        scheduler.wait_for(waiter1.start_ts, waiter1.cb, waiter1.pr, waiter1.lock, 1000);
        let (waiter2, lock_info2, f2) = new_test_waiter(waiter_ts, lock.ts, lock.hash);
        scheduler.wait_for(waiter2.start_ts, waiter2.cb, waiter2.pr, waiter2.lock, 1000);
        // Should notify duplicated waiter immediately.
        assert_elapsed(
            || expect_key_is_locked(f1.wait().unwrap().unwrap().pop().unwrap(), lock_info1),
            0,
            200,
        );
        // The new waiter will be wake up after timeout.
        assert_elapsed(
            || expect_key_is_locked(f2.wait().unwrap().unwrap().pop().unwrap(), lock_info2),
            900,
            1200,
        );

        worker.stop().unwrap();
    }

    #[bench]
    fn bench_wake_up_small_table_against_big_hashes(b: &mut test::Bencher) {
        let detect_worker = FutureWorker::new("dummy-deadlock");
        let detector_scheduler = DetectorScheduler::new(detect_worker.scheduler());
        let mut waiter_mgr = WaiterManager::new(
            Arc::new(AtomicUsize::new(0)),
            detector_scheduler,
            &Config::default(),
        );
        waiter_mgr
            .wait_table
            .borrow_mut()
            .add_waiter(dummy_waiter(10, 20, 10000));
        let hashes: Vec<u64> = (0..1000).collect();
        b.iter(|| {
            test::black_box(|| waiter_mgr.handle_wake_up(20, hashes.clone(), 30));
        });
    }
}

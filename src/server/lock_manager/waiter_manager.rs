// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    fmt::{self, Debug, Display, Formatter},
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use collections::HashMap;
use futures::{
    compat::{Compat01As03, Future01CompatExt},
    future::Future,
    task::{Context, Poll},
};
use kvproto::{deadlock::WaitForEntry, metapb::RegionEpoch};
use tikv_util::{
    config::ReadableDuration,
    time::{duration_to_sec, InstantExt},
    timer::GLOBAL_TIMER_HANDLE,
    worker::{FutureRunnable, FutureScheduler, Stopped},
};
use tokio::task::spawn_local;
use tracker::GLOBAL_TRACKERS;
use txn_types::Key;

use super::{config::Config, deadlock::Scheduler as DetectorScheduler, metrics::*};
use crate::storage::{
    lock_manager::{
        DiagnosticContext, KeyLockWaitInfo, LockDigest, LockWaitToken, UpdateWaitForEvent,
        WaitTimeout,
    },
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, TimeStamp},
    txn::Error as TxnError,
    Error as StorageError, ErrorInner as StorageErrorInner,
};

struct DelayInner {
    timer: Compat01As03<tokio_timer::Delay>,
    cancelled: bool,
}

/// `Delay` is a wrapper of `tokio_timer::Delay` which has a resolution of one
/// millisecond. It has some extra features than `tokio_timer::Delay` used by
/// `WaiterManager`.
///
/// `Delay` performs no work and completes with `true` once the specified
/// deadline has been reached. If it has been cancelled, it will complete with
/// `false` at arbitrary time.
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
    SetKeyWakeUpDelayCallback {
        cb: Box<dyn Fn(&Key, TimeStamp, TimeStamp, Instant) + Send>,
    },
    WaitFor {
        token: LockWaitToken,
        region_id: u64,
        region_epoch: RegionEpoch,
        term: u64,
        // which txn waits for the lock
        start_ts: TimeStamp,
        wait_info: KeyLockWaitInfo,
        timeout: WaitTimeout,
        cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
        diag_ctx: DiagnosticContext,
        start_waiting_time: Instant,
    },
    RemoveLockWait {
        token: LockWaitToken,
    },
    UpdateWaitFor {
        events: Vec<UpdateWaitForEvent>,
    },
    Dump {
        cb: Callback,
    },
    Deadlock {
        // Which txn causes deadlock
        start_ts: TimeStamp,
        key: Vec<u8>,
        lock: LockDigest,
        deadlock_key_hash: u64,
        wait_chain: Vec<WaitForEntry>,
    },
    ChangeConfig {
        timeout: Option<ReadableDuration>,
    },
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(ReadableDuration) + Send>),
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
            Task::SetKeyWakeUpDelayCallback { .. } => {
                write!(f, "setting key wake up delay callback")
            }
            Task::WaitFor {
                token,
                start_ts,
                wait_info,
                ..
            } => {
                write!(
                    f,
                    "txn:{} waiting for {}:{}, token {:?}",
                    start_ts, wait_info.lock_digest.ts, wait_info.lock_digest.hash, token
                )
            }
            Task::RemoveLockWait { token } => {
                write!(f, "waking up txns waiting for token {:?}", token)
            }
            Task::UpdateWaitFor { events } => {
                write!(f, "updating wait info {:?}", events)
            }
            Task::Dump { .. } => write!(f, "dump"),
            Task::Deadlock { start_ts, .. } => write!(f, "txn:{} deadlock", start_ts),
            Task::ChangeConfig { timeout } => write!(
                f,
                "change config to default_wait_for_lock_timeout: {:?}",
                timeout
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
    // These field will be needed for supporting region-level waking up when region errors
    // happens.
    // region_id: u64,
    // region_epoch: RegionEpoch,
    // term: u64,
    pub(crate) start_ts: TimeStamp,
    pub(crate) wait_info: KeyLockWaitInfo,
    pub(crate) cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
    pub diag_ctx: DiagnosticContext,
    delay: Delay,
    start_waiting_time: Instant,
}

impl Waiter {
    fn new(
        _region_id: u64,
        _region_epoch: RegionEpoch,
        _term: u64,
        start_ts: TimeStamp,
        wait_info: KeyLockWaitInfo,
        cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
        deadline: Instant,
        diag_ctx: DiagnosticContext,
        start_waiting_time: Instant,
    ) -> Self {
        Self {
            start_ts,
            wait_info,
            cancel_callback,
            delay: Delay::new(deadline),
            diag_ctx,
            start_waiting_time,
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

    /// Consumes the `Waiter` to notify the corresponding transaction `going on.
    fn cancel(self, error: Option<StorageError>) -> KeyLockWaitInfo {
        let elapsed = self.start_waiting_time.saturating_elapsed();
        GLOBAL_TRACKERS.with_tracker(self.diag_ctx.tracker, |tracker| {
            tracker.metrics.pessimistic_lock_wait_nanos = elapsed.as_nanos() as u64;
        });
        WAITER_LIFETIME_HISTOGRAM.observe(duration_to_sec(elapsed));
        // Cancel the delay timer to prevent removing the same `Waiter` earlier.
        self.delay.cancel();
        if let Some(error) = error {
            (self.cancel_callback)(error);
        }
        self.wait_info
    }

    fn cancel_for_finished(self) -> KeyLockWaitInfo {
        self.cancel(None)
    }

    fn cancel_for_timeout(self, _skip_resolving_lock: bool) -> KeyLockWaitInfo {
        let lock_info = self.wait_info.lock_info.clone();
        // lock_info.set_skip_resolving_lock(skip_resolving_lock);
        let error = MvccError::from(MvccErrorInner::KeyIsLocked(lock_info));
        self.cancel(Some(StorageError::from(TxnError::from(error))))
    }

    pub(super) fn cancel_no_timeout(
        wait_info: KeyLockWaitInfo,
        cancel_callback: Box<dyn FnOnce(StorageError)>,
    ) {
        let lock_info = wait_info.lock_info;
        let error = MvccError::from(MvccErrorInner::KeyIsLocked(lock_info));
        cancel_callback(StorageError::from(TxnError::from(error)))
    }

    fn cancel_for_deadlock(
        self,
        lock_digest: LockDigest,
        key: Vec<u8>,
        deadlock_key_hash: u64,
        wait_chain: Vec<WaitForEntry>,
    ) -> KeyLockWaitInfo {
        let e = MvccError::from(MvccErrorInner::Deadlock {
            start_ts: self.start_ts,
            lock_ts: lock_digest.ts,
            lock_key: key,
            deadlock_key_hash,
            wait_chain,
        });
        self.cancel(Some(StorageError::from(TxnError::from(e))))
    }
}

struct WaitTable {
    // Map lock hash and ts to waiters.
    // For compatibility.
    wait_table: HashMap<(u64, TimeStamp), LockWaitToken>,
    waiter_pool: HashMap<LockWaitToken, Waiter>,
    waiter_count: Arc<AtomicUsize>,

    wake_up_key_delay_callback: Option<Box<dyn Fn(&Key, TimeStamp, TimeStamp, Instant) + Send>>,
}

impl WaitTable {
    fn new(waiter_count: Arc<AtomicUsize>) -> Self {
        Self {
            wait_table: HashMap::default(),
            waiter_pool: HashMap::default(),
            waiter_count,
            wake_up_key_delay_callback: None,
        }
    }

    fn set_wake_up_key_delay_callback(
        &mut self,
        cb: Option<Box<dyn Fn(&Key, TimeStamp, TimeStamp, Instant) + Send>>,
    ) {
        self.wake_up_key_delay_callback = cb;
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
        self.wait_table
            .insert((waiter.wait_info.lock_digest.hash, waiter.start_ts), token);

        assert!(self.waiter_pool.insert(token, waiter).is_none());

        true
    }

    fn take_waiter(&mut self, token: LockWaitToken) -> Option<Waiter> {
        let waiter = self.waiter_pool.remove(&token)?;
        self.waiter_count.fetch_sub(1, Ordering::SeqCst);
        self.wait_table
            .remove(&(waiter.wait_info.lock_digest.hash, waiter.start_ts));
        // WAIT_TABLE_STATUS_GAUGE.txns.dec();
        Some(waiter)
    }

    fn update_waiter(&mut self, update_event: &UpdateWaitForEvent) -> Option<KeyLockWaitInfo> {
        let waiter = self.waiter_pool.get_mut(&update_event.token)?;

        assert_eq!(waiter.wait_info.key, update_event.wait_info.key);

        if waiter.wait_info.lock_digest.ts == update_event.wait_info.lock_digest.ts {
            // Unchanged.
            return None;
        }

        let result = std::mem::replace(&mut waiter.wait_info, update_event.wait_info.clone());
        waiter.diag_ctx = update_event.diag_ctx.clone();

        return Some(result);
    }

    fn take_waiter_by_lock_digest(
        &mut self,
        lock: LockDigest,
        waiter_ts: TimeStamp,
    ) -> Option<Waiter> {
        let token = *self.wait_table.get(&(lock.hash, waiter_ts))?;
        self.take_waiter(token)
    }

    fn to_wait_for_entries(&self) -> Vec<WaitForEntry> {
        self.waiter_pool
            .iter()
            .map(|(_, waiter)| {
                let mut wait_for_entry = WaitForEntry::default();
                wait_for_entry.set_txn(waiter.start_ts.into_inner());
                wait_for_entry.set_wait_for_txn(waiter.wait_info.lock_digest.ts.into_inner());
                wait_for_entry.set_key_hash(waiter.wait_info.lock_digest.hash);
                wait_for_entry.set_key(waiter.wait_info.key.to_raw().unwrap());
                wait_for_entry.set_resource_group_tag(waiter.diag_ctx.resource_group_tag.clone());
                wait_for_entry
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
        term: u64,
        start_ts: TimeStamp,
        wait_info: KeyLockWaitInfo,
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
            start_waiting_time: Instant::now(),
        });
    }

    pub fn set_key_wake_up_delay_callback(
        &self,
        cb: Box<dyn Fn(&Key, TimeStamp, TimeStamp, Instant) + Send>,
    ) {
        self.notify_scheduler(Task::SetKeyWakeUpDelayCallback { cb });
    }

    pub fn remove_lock_wait(&self, token: LockWaitToken) {
        self.notify_scheduler(Task::RemoveLockWait { token });
    }

    pub fn update_wait_for(&self, events: Vec<UpdateWaitForEvent>) {
        self.notify_scheduler(Task::UpdateWaitFor { events });
    }

    pub fn dump_wait_table(&self, cb: Callback) -> bool {
        self.notify_scheduler(Task::Dump { cb })
    }

    pub fn deadlock(
        &self,
        txn_ts: TimeStamp,
        key: Vec<u8>,
        lock: LockDigest,
        deadlock_key_hash: u64,
        wait_chain: Vec<WaitForEntry>,
    ) {
        self.notify_scheduler(Task::Deadlock {
            start_ts: txn_ts,
            key,
            lock,
            deadlock_key_hash,
            wait_chain,
        });
    }

    pub fn change_config(&self, timeout: Option<ReadableDuration>) {
        self.notify_scheduler(Task::ChangeConfig { timeout });
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn validate(&self, f: Box<dyn FnOnce(ReadableDuration) + Send>) {
        self.notify_scheduler(Task::Validate(f));
    }
}

/// WaiterManager handles waiting and wake-up of pessimistic lock
pub struct WaiterManager {
    wait_table: Rc<RefCell<WaitTable>>,
    detector_scheduler: DetectorScheduler,
    /// It is the default and maximum timeout of waiter.
    default_wait_for_lock_timeout: ReadableDuration,
    // /// If more than one waiters are waiting for the same lock, only the
    // /// oldest one will be waked up immediately when the lock is released.
    // /// Others will be waked up after `wake_up_delay_duration` to reduce
    // /// contention and make the oldest one more likely acquires the lock.
    // wake_up_delay_duration: ReadableDuration,
}

unsafe impl Send for WaiterManager {}

impl WaiterManager {
    pub fn new(
        waiter_count: Arc<AtomicUsize>,
        detector_scheduler: DetectorScheduler,
        cfg: &Config,
    ) -> Self {
        let wait_table = WaitTable::new(waiter_count);

        Self {
            wait_table: Rc::new(RefCell::new(wait_table)),
            detector_scheduler,
            default_wait_for_lock_timeout: cfg.wait_for_lock_timeout,
            // wake_up_delay_duration: cfg.wake_up_delay_duration,
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
            let mut wait_table = wait_table.borrow_mut();
            if let Some(waiter) = wait_table.take_waiter(token) {
                let start_ts = waiter.start_ts;
                let wait_info = waiter.cancel_for_timeout(false);
                detector_scheduler.clean_up_wait_for(start_ts, wait_info);
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
        let start_ts = waiter.start_ts;
        let wait_info = waiter.cancel_for_finished();
        self.detector_scheduler
            .clean_up_wait_for(start_ts, wait_info);
    }

    fn handle_update_wait_for(&mut self, events: Vec<UpdateWaitForEvent>) {
        let mut wait_table = self.wait_table.borrow_mut();
        for event in events {
            let previous_wait_info = wait_table.update_waiter(&event);

            if event.is_first_lock {
                continue;
            }

            if let Some(previous_wait_info) = previous_wait_info {
                self.detector_scheduler
                    .clean_up_wait_for(event.start_ts, previous_wait_info);
                self.detector_scheduler
                    .detect(event.start_ts, event.wait_info, event.diag_ctx);
            }
        }
    }

    fn handle_dump(&self, cb: Callback) {
        cb(self.wait_table.borrow().to_wait_for_entries());
    }

    fn handle_deadlock(
        &mut self,
        waiter_ts: TimeStamp,
        key: Vec<u8>,
        lock: LockDigest,
        deadlock_key_hash: u64,
        wait_chain: Vec<WaitForEntry>,
    ) {
        let waiter = self
            .wait_table
            .borrow_mut()
            .take_waiter_by_lock_digest(lock, waiter_ts);
        if let Some(waiter) = waiter {
            waiter.cancel_for_deadlock(lock, key, deadlock_key_hash, wait_chain);
        }
    }

    fn handle_config_change(&mut self, timeout: Option<ReadableDuration>) {
        if let Some(timeout) = timeout {
            self.default_wait_for_lock_timeout = timeout;
        }
        info!(
            "Waiter manager config changed";
            "default_wait_for_lock_timeout" => self.default_wait_for_lock_timeout.to_string(),
        );
    }
}

impl FutureRunnable<Task> for WaiterManager {
    fn run(&mut self, task: Task) {
        match task {
            Task::SetKeyWakeUpDelayCallback { cb } => {
                self.wait_table
                    .borrow_mut()
                    .set_wake_up_key_delay_callback(Some(cb));
            }
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
                start_waiting_time,
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
                    start_waiting_time,
                );
                self.handle_wait_for(token, waiter);
                TASK_COUNTER_METRICS.wait_for.inc();
            }

            Task::RemoveLockWait { token } => {
                self.handle_remove_lock_wait(token);
                TASK_COUNTER_METRICS.wake_up.inc();
            }
            Task::UpdateWaitFor { events } => {
                self.handle_update_wait_for(events);
                TASK_COUNTER_METRICS.update_wait_for.inc();
            }
            Task::Dump { cb } => {
                self.handle_dump(cb);
                TASK_COUNTER_METRICS.dump.inc();
            }
            Task::Deadlock {
                start_ts,
                key,
                lock,
                deadlock_key_hash,
                wait_chain,
            } => {
                self.handle_deadlock(start_ts, key, lock, deadlock_key_hash, wait_chain);
            }
            Task::ChangeConfig { timeout } => self.handle_config_change(timeout),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(f) => f(
                self.default_wait_for_lock_timeout,
                // self.wake_up_delay_duration,
            ),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{sync::mpsc, time::Duration};

    use futures::{executor::block_on, future::FutureExt};
    use kvproto::kvrpcpb::LockInfo;
    use rand::prelude::*;
    use tikv_util::{
        config::ReadableDuration, future::paired_future_callback, time::InstantExt,
        worker::FutureWorker,
    };
    use txn_types::Key;

    use super::*;
    use crate::storage::txn::ErrorInner as TxnErrorInner;

    fn dummy_waiter(start_ts: TimeStamp, lock_ts: TimeStamp, hash: u64) -> Waiter {
        Waiter {
            start_ts,
            wait_info: KeyLockWaitInfo {
                key: Key::from_raw(b""),
                lock_digest: LockDigest { ts: lock_ts, hash },
                lock_info: Default::default(),
            },
            cancel_callback: Box::new(|_| ()),
            diag_ctx: DiagnosticContext::default(),
            delay: Delay::new(Instant::now()),
            start_waiting_time: Instant::now(),
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
            1,
            waiter_ts,
            KeyLockWaitInfo {
                key: Key::from_raw(&raw_key),
                lock_digest: lock,
                lock_info: info.clone(),
            },
            cb,
            Instant::now() + Duration::from_millis(3000),
            DiagnosticContext::default(),
            Instant::now(),
        );
        (waiter, info, f)
    }

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
        expect_wait_chain: &[(u64, u64, &[u8], &[u8])], /* (waiter_ts, wait_for_ts, key,
                                                         * resource_group_tag) */
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
        waiter.cancel_for_timeout(false);
        expect_key_is_locked(block_on(f).unwrap(), lock_info);

        // // A waiter can conflict with other transactions more than once.
        // for conflict_times in 1..=3 {
        //     let waiter_ts = TimeStamp::new(10);
        //     let mut lock_ts = TimeStamp::new(20);
        //     let (mut waiter, mut lock_info, f) = new_test_waiter(waiter_ts, lock_ts,
        // 20);     let mut conflict_commit_ts = TimeStamp::new(30);
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
        let (waiter, lock_info, f) = new_test_waiter(waiter_ts, 20.into(), 20);
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
        // let (mut waiter, lock_info, f) = new_test_waiter(waiter_ts,
        // 20.into(), 20); waiter.conflict_with(20.into(), 30.into());
        // waiter.deadlock_with(111, vec![]);
        // waiter.cancel();
        // expect_deadlock(block_on(f).unwrap(), waiter_ts, lock_info, 111,
        // &[]);
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
        waiter.cancel_for_timeout(false);
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
            assert_eq!(waiter.wait_info.lock_digest, lock);
        }
        assert_eq!(wait_table.count(), 0);
        assert!(wait_table.wait_table.is_empty());
        assert!(
            wait_table
                .take_waiter_by_lock_digest(
                    LockDigest {
                        ts: TimeStamp::zero(),
                        hash: 0,
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
    //     let mut waiters_ts: Vec<TimeStamp> =
    // (0..waiter_count).map(TimeStamp::from).collect();     waiters_ts.
    // shuffle(&mut rand::thread_rng());     for ts in waiters_ts.iter() {
    //         wait_table.add_waiter(dummy_waiter(*ts, lock.ts, lock.hash));
    //     }
    //     assert_eq!(wait_table.count(), waiters_ts.len());
    //     waiters_ts.sort();
    //     for (i, ts) in waiters_ts.into_iter().enumerate() {
    //         let (oldest, others) =
    // wait_table.remove_oldest_waiter(lock).unwrap();         assert_eq!
    // (oldest.start_ts, ts);         assert_eq!(others.len(), waiter_count as
    // usize - i - 1);     }
    //     // There is no waiter in the wait table but there is an entry in it.
    //     assert_eq!(wait_table.count(), 0);
    //     assert_eq!(wait_table.wait_table.len(), 1);
    //     wait_table.remove(lock);
    //     assert!(wait_table.wait_table.is_empty());
    // }

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
            1,
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
            1,
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
            1,
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
    //     for (&lock_hash, &waiter_ts) in lock_hashes.iter().zip(waiters_ts.iter())
    // {         let (waiter, lock_info, f) = new_test_waiter(waiter_ts,
    // lock_ts, lock_hash);         scheduler.wait_for(
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
    //             || expect_write_conflict(block_on(f).unwrap(), waiter_ts,
    // lock_info, commit_ts),             0,
    //             200,
    //         );
    //     }
    //
    //     // Multiple waiters are waiting for one lock.
    //     let mut lock = LockDigest {
    //         ts: 10.into(),
    //         hash: 10,
    //     };
    //     let mut waiters_ts: Vec<TimeStamp> =
    // (20..25).map(TimeStamp::from).collect();     // Waiters are added in
    // arbitrary order.     waiters_ts.shuffle(&mut rand::thread_rng());
    //     let mut waiters_info = vec![];
    //     for waiter_ts in waiters_ts {
    //         let (waiter, lock_info, f) = new_test_waiter(waiter_ts, lock.ts,
    // lock.hash);         scheduler.wait_for(
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
    //     for (waiter_ts, mut lock_info, f) in
    // waiters_info.drain(..waiters_info.len() - 1) {         scheduler.
    // wake_up(lock.ts, vec![lock.hash], commit_ts);         lock_info.
    // set_lock_version(lock.ts.into_inner());         assert_elapsed(
    //             || expect_write_conflict(block_on(f).unwrap(), waiter_ts,
    // lock_info, commit_ts),             0,
    //             200,
    //         );
    //         // Now the lock is held by the waked up transaction.
    //         lock.ts = waiter_ts;
    //         commit_ts.incr();
    //     }
    //     // Last waiter isn't waked up by other transactions. It will be waked up
    // after     // wake_up_delay_duration.
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
    //     let (waiter1, lock_info1, f1) = new_test_waiter(20.into(), lock.ts,
    // lock.hash);     scheduler.wait_for(
    //         waiter1.start_ts,
    //         waiter1.cb,
    //         waiter1.pr,
    //         waiter1.lock,
    //         WaitTimeout::Millis(wait_for_lock_timeout),
    //         DiagnosticContext::default(),
    //     );
    //     let (waiter2, lock_info2, f2) = new_test_waiter(30.into(), lock.ts,
    // lock.hash);     // Waiter2's timeout is 50ms which is less than
    // wake_up_delay_duration.     scheduler.wait_for(
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
    //             || expect_write_conflict(block_on(f2).unwrap(), 30.into(),
    // lock_info2, 15.into()),             30,
    //             100,
    //         );
    //         tx.send(()).unwrap();
    //     });
    //     // It will increase waiter2's timeout to wake_up_delay_duration.
    //     scheduler.wake_up(lock.ts, vec![lock.hash], commit_ts);
    //     assert_elapsed(
    //         || expect_write_conflict(block_on(f1).unwrap(), 20.into(),
    // lock_info1, commit_ts),         0,
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
            1,
            waiter.start_ts,
            waiter.wait_info,
            WaitTimeout::Millis(1000),
            waiter.cancel_callback,
            DiagnosticContext::default(),
        );
        scheduler.deadlock(waiter_ts, b"foo".to_vec(), lock, 30, vec![]);
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
    //     let (waiter1, lock_info1, f1) = new_test_waiter(waiter_ts, lock.ts,
    // lock.hash);     scheduler.wait_for(
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
    //     let (waiter2, lock_info2, f2) = new_test_waiter(waiter_ts, lock.ts,
    // lock.hash);     scheduler.wait_for(
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
    //     let detector_scheduler =
    // DetectorScheduler::new(detect_worker.scheduler());     let mut
    // waiter_mgr = WaiterManager::new(         Arc::new(AtomicUsize::
    // new(0)),         detector_scheduler,
    //         &Config::default(),
    //     );
    //     waiter_mgr.wait_table.borrow_mut().add_waiter(
    //         LockWaitToken(Some(1)),
    //         dummy_waiter(10.into(), 20.into(), 10000),
    //     );
    //     let hashes: Vec<u64> = (0..1000).collect();
    //     b.iter(|| {
    //         waiter_mgr.handle_remove_lock_wait(20.into(), hashes.clone(),
    // 30.into());     });
    // }
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    fmt::{self, Debug, Display, Formatter},
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
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
    time::{InstantExt, duration_to_sec},
    timer::GLOBAL_TIMER_HANDLE,
    worker::{FutureRunnable, FutureScheduler, Stopped},
};
use tokio::task::spawn_local;
use tracker::GLOBAL_TRACKERS;

use super::{config::Config, deadlock::Scheduler as DetectorScheduler, metrics::*};
use crate::storage::{
    Error as StorageError, ErrorInner as StorageErrorInner,
    lock_manager::{
        CancellationCallback, DiagnosticContext, KeyLockWaitInfo, LockDigest, LockWaitToken,
        UpdateWaitForEvent, WaitTimeout,
    },
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, TimeStamp},
    txn::Error as TxnError,
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
    WaitFor {
        token: LockWaitToken,
        region_id: u64,
        region_epoch: RegionEpoch,
        term: u64,
        // which txn waits for the lock
        start_ts: TimeStamp,
        wait_info: KeyLockWaitInfo,
        timeout: WaitTimeout,
        cancel_callback: CancellationCallback,
        diag_ctx: DiagnosticContext,
        start_waiting_time: Instant,
        // Whether this wait registered a detector edge (false => Detect was/will be sent).
        is_first_lock: bool,
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
            Task::WaitFor {
                token,
                start_ts,
                wait_info,
                is_first_lock,
                ..
            } => {
                write!(
                    f,
                    "txn:{} waiting for {}:{}, token {:?}, first_lock {}",
                    start_ts,
                    wait_info.lock_digest.ts,
                    wait_info.lock_digest.hash,
                    token,
                    is_first_lock
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
    pub(crate) cancel_callback: CancellationCallback,
    pub diag_ctx: DiagnosticContext,
    delay: Delay,
    start_waiting_time: Instant,
    last_updated_time: Option<Instant>,
    /// The wait-for edge last sent to DetectTable (or queued to send).
    /// `None` if this waiter never registered (first lock) or the edge was
    /// already cleaned up after an owner change.
    registered_edge: Option<KeyLockWaitInfo>,
}

impl Waiter {
    fn new(
        _region_id: u64,
        _region_epoch: RegionEpoch,
        _term: u64,
        start_ts: TimeStamp,
        wait_info: KeyLockWaitInfo,
        cancel_callback: CancellationCallback,
        deadline: Instant,
        diag_ctx: DiagnosticContext,
        start_waiting_time: Instant,
        is_first_lock: bool,
    ) -> Self {
        let registered_edge = if is_first_lock {
            None
        } else {
            Some(wait_info.clone())
        };
        Self {
            start_ts,
            wait_info,
            cancel_callback,
            delay: Delay::new(deadline),
            diag_ctx,
            start_waiting_time,
            last_updated_time: None,
            registered_edge,
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

    #[allow(dead_code)]
    fn reset_timeout(&self, deadline: Instant) {
        self.delay.reset(deadline);
    }

    /// Consumes the `Waiter` to notify the corresponding transaction going on.
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

    fn cancel_for_timeout(self) -> KeyLockWaitInfo {
        let mut lock_info = self.wait_info.lock_info.clone();
        lock_info.set_duration_to_last_update_ms(
            self.last_updated_time
                // round up, so that duration in (0, 1ms] won't be treated as 0.
                .map(|t| (t.elapsed().as_millis() as u64).max(1))
                .unwrap_or_default(),
        );
        let error = MvccError::from(MvccErrorInner::KeyIsLocked(lock_info));
        self.cancel(Some(StorageError::from(TxnError::from(error))))
    }

    pub(super) fn cancel_no_timeout(
        wait_info: KeyLockWaitInfo,
        cancel_callback: CancellationCallback,
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
}

impl WaitTable {
    fn new(waiter_count: Arc<AtomicUsize>) -> Self {
        Self {
            wait_table: HashMap::default(),
            waiter_pool: HashMap::default(),
            waiter_count,
        }
    }

    #[cfg(test)]
    fn count(&self) -> usize {
        self.waiter_pool.len()
    }

    fn is_empty(&self) -> bool {
        self.waiter_pool.is_empty()
    }

    /// Adds a waiter identified by given token. The caller must guarantee that
    /// the `token` is unique and doesn't exist in waiter manager currently.
    fn add_waiter(&mut self, token: LockWaitToken, waiter: Waiter) {
        self.wait_table
            .insert((waiter.wait_info.lock_digest.hash, waiter.start_ts), token);
        assert!(self.waiter_pool.insert(token, waiter).is_none());
    }

    fn take_waiter(&mut self, token: LockWaitToken) -> Option<Waiter> {
        let waiter = self.waiter_pool.remove(&token)?;
        self.waiter_count.fetch_sub(1, Ordering::SeqCst);
        self.wait_table
            .remove(&(waiter.wait_info.lock_digest.hash, waiter.start_ts));
        Some(waiter)
    }

    fn take_registered_edge(&mut self, token: LockWaitToken) -> Option<KeyLockWaitInfo> {
        self.waiter_pool.get_mut(&token)?.registered_edge.take()
    }

    fn update_waiter(
        &mut self,
        update_event: &UpdateWaitForEvent,
        now: Instant,
    ) -> Option<(KeyLockWaitInfo, DiagnosticContext)> {
        let waiter = self.waiter_pool.get_mut(&update_event.token)?;
        waiter.last_updated_time = Some(now);

        assert_eq!(waiter.wait_info.key, update_event.wait_info.key);

        if waiter.wait_info.lock_digest.ts == update_event.wait_info.lock_digest.ts {
            // Unchanged.
            return None;
        }

        let result = std::mem::replace(&mut waiter.wait_info, update_event.wait_info.clone());

        Some((result, waiter.diag_ctx.clone()))
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
            .values()
            .map(|waiter| {
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
        cancel_callback: CancellationCallback,
        diag_ctx: DiagnosticContext,
        is_first_lock: bool,
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
            is_first_lock,
        });
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

/// WaiterManager handles lock waiting, cancels waiters when needed (due to
/// timeout or deadlock detected), and provide lock waiting information for
/// diagnosing.
pub struct WaiterManager {
    wait_table: Rc<RefCell<WaitTable>>,
    detector_scheduler: DetectorScheduler,
    /// It is the default and maximum timeout of waiter.
    default_wait_for_lock_timeout: ReadableDuration,
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
                let edge = waiter.registered_edge.clone();
                let _wait_info = waiter.cancel_for_timeout();
                if let Some(edge) = edge {
                    detector_scheduler.clean_up_wait_for(start_ts, edge);
                }
            }
        });
        self.wait_table.borrow_mut().add_waiter(token, waiter);
        spawn_local(f);
    }

    fn handle_remove_lock_wait(&mut self, token: LockWaitToken) {
        let mut wait_table = self.wait_table.borrow_mut();
        if wait_table.is_empty() {
            return;
        }
        let waiter = if let Some(w) = wait_table.take_waiter(token) {
            w
        } else {
            return;
        };
        let start_ts = waiter.start_ts;
        let edge = waiter.registered_edge.clone();
        let _wait_info = waiter.cancel_for_finished();
        if let Some(edge) = edge {
            self.detector_scheduler.clean_up_wait_for(start_ts, edge);
        }
    }

    fn handle_update_wait_for(&mut self, events: Vec<UpdateWaitForEvent>) {
        let mut wait_table = self.wait_table.borrow_mut();
        let now = Instant::now();
        for event in events {
            let previous_wait_info = wait_table.update_waiter(&event, now);

            if event.is_first_lock {
                continue;
            }

            if let Some((previous_wait_info, _diag_ctx)) = previous_wait_info {
                if previous_wait_info.allow_lock_with_conflict {
                    // First owner change: clean up the registered edge and do
                    // not Detect the new owner. Later changes are no-ops.
                    // A cycle that needs the new edge is invisible until this
                    // wait ends (capped by wait-for-lock-timeout) and retries.
                    if let Some(edge) = wait_table.take_registered_edge(event.token) {
                        self.detector_scheduler
                            .clean_up_wait_for(event.start_ts, edge);
                        TASK_COUNTER_METRICS.drop_registered_edge.inc();
                    }
                }
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
<<<<<<< HEAD
=======
            if waiter.wait_info.lock_info.is_shared_lock() {
                // When deadlock detected on a shared lock, some wait-for entries might have
                // been registered to the detect table. So do clean up here to avoid those
                // entries causing false-positive deadlock errors.
                if let Some(edge) = waiter.registered_edge.clone() {
                    self.detector_scheduler.clean_up_wait_for(waiter_ts, edge);
                }
            }
>>>>>>> 1ef0a1961c (lock_manager: skip detect on fair waiter owner change (#20002))
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
                is_first_lock,
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
                    is_first_lock,
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
    use std::{
        sync::{Mutex, mpsc},
        thread::sleep,
        time::Duration,
    };

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
                allow_lock_with_conflict: false,
            },
            cancel_callback: Box::new(|_| ()),
            diag_ctx: DiagnosticContext::default(),
            delay: Delay::new(Instant::now()),
            start_waiting_time: Instant::now(),
            last_updated_time: None,
            registered_edge: None,
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
        new_test_waiter_impl(waiter_ts, lock_ts, None, Some(lock_hash))
    }

    pub(crate) fn new_test_waiter_with_key(
        waiter_ts: TimeStamp,
        lock_ts: TimeStamp,
        key: &[u8],
    ) -> WaiterCtx {
        new_test_waiter_impl(waiter_ts, lock_ts, Some(key), None)
    }

    fn new_test_waiter_impl(
        waiter_ts: TimeStamp,
        lock_ts: TimeStamp,
        key: Option<&[u8]>,
        lock_hash: Option<u64>,
    ) -> WaiterCtx {
        let raw_key = key.unwrap_or(b"foo").to_vec();
        let lock_hash = lock_hash.unwrap_or_else(|| Key::from_raw(&raw_key).gen_hash());
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
                allow_lock_with_conflict: false,
            },
            cb,
            Instant::now() + Duration::from_millis(3000),
            DiagnosticContext::default(),
            Instant::now(),
            false,
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
        waiter.cancel_for_timeout();
        expect_key_is_locked(block_on(f).unwrap(), lock_info);

        // Deadlock
        let waiter_ts = TimeStamp::new(10);
        let (waiter, lock_info, f) = new_test_waiter(waiter_ts, 20.into(), 20);
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
            wait_table.add_waiter(
                LockWaitToken(Some(i)),
                dummy_waiter(waiter_ts, lock.ts, lock.hash),
            );
            waiter_info.push((waiter_ts, lock));
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
            false,
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
            false,
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
            false,
        );
        assert_elapsed(
            || expect_key_is_locked(block_on(f).unwrap(), lock_info),
            900,
            1200,
        );

        worker.stop().unwrap();
    }

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
            false,
        );
        scheduler.deadlock(waiter_ts, b"foo".to_vec(), lock, 30, vec![]);
        assert_elapsed(
            || expect_deadlock(block_on(f).unwrap(), waiter_ts, lock_info, 30, &[]),
            0,
            200,
        );
        worker.stop().unwrap();
    }

    #[test]
    fn test_duration_to_last_update() {
        let (mut worker, scheduler) = start_waiter_manager(1000, 100);
        let key = Key::from_raw(b"foo");
        let (waiter_ts, lock) = (
            10.into(),
            LockDigest {
                ts: 20.into(),
                hash: key.gen_hash(),
            },
        );
        // waiter1 is updated when waiting, while waiter2(f2) is not.
        let (waiter1, ..) = new_test_waiter_with_key(waiter_ts, lock.ts, &key.to_raw().unwrap());
        let (waiter2, _, f2) = new_test_waiter_with_key(100.into(), 100.into(), "foo".as_bytes());
        scheduler.wait_for(
            LockWaitToken(Some(1)),
            1,
            RegionEpoch::default(),
            1,
            waiter1.start_ts,
            waiter1.wait_info,
            WaitTimeout::Millis(1000),
            waiter1.cancel_callback,
            DiagnosticContext::default(),
            false,
        );
        scheduler.wait_for(
            LockWaitToken(Some(2)),
            1,
            RegionEpoch::default(),
            1,
            waiter2.start_ts,
            waiter2.wait_info,
            WaitTimeout::Millis(1000),
            waiter2.cancel_callback,
            DiagnosticContext::default(),
            false,
        );

        // then update waiter
        sleep(Duration::from_millis(500));
        let event = UpdateWaitForEvent {
            token: LockWaitToken(Some(1)),
            start_ts: waiter1.start_ts,
            is_first_lock: false,
            wait_info: KeyLockWaitInfo {
                key: key.clone(),
                lock_digest: Default::default(),
                lock_info: LockInfo {
                    key: key.to_raw().unwrap(),
                    ..Default::default()
                },
                allow_lock_with_conflict: false,
            },
        };
        scheduler.update_wait_for(vec![event]);

        assert_elapsed(
            || match block_on(f2).unwrap() {
                StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
                    MvccError(box MvccErrorInner::KeyIsLocked(res)),
                )))) => {
                    assert_eq!(res.duration_to_last_update_ms, 0);
                }
                e => panic!("unexpected error: {:?}", e),
            },
            400,
            600,
        );

        worker.stop().unwrap();
    }

    /// Counts Detect / CleanUpWaitFor tasks scheduled to the deadlock detector.
    /// Used to reproduce tikv#19846 (unbounded 2N fan-out per owner change)
    /// and to check that owner-change updates no longer grow with waiter count.
    struct CountingDetector {
        detect: Arc<AtomicUsize>,
        cleanup_wait_for: Arc<AtomicUsize>,
        detect_lock_ts: Arc<Mutex<Vec<u64>>>,
        cleanup_lock_ts: Arc<Mutex<Vec<u64>>>,
    }

    impl FutureRunnable<crate::server::lock_manager::deadlock::Task> for CountingDetector {
        fn run(&mut self, task: crate::server::lock_manager::deadlock::Task) {
            use crate::server::lock_manager::deadlock::{DetectType, Task as DetectorTask};
            if let DetectorTask::Detect { tp, wait_info, .. } = task {
                let ts = wait_info
                    .as_ref()
                    .map(|w| w.lock_digest.ts.into_inner())
                    .unwrap_or(0);
                match tp {
                    DetectType::Detect => {
                        self.detect.fetch_add(1, Ordering::SeqCst);
                        self.detect_lock_ts.lock().unwrap().push(ts);
                    }
                    DetectType::CleanUpWaitFor => {
                        self.cleanup_wait_for.fetch_add(1, Ordering::SeqCst);
                        self.cleanup_lock_ts.lock().unwrap().push(ts);
                    }
                    DetectType::CleanUp => {}
                }
            }
        }
    }

    struct DetectorCounters {
        detect: Arc<AtomicUsize>,
        cleanup_wait_for: Arc<AtomicUsize>,
        #[allow(dead_code)]
        detect_lock_ts: Arc<Mutex<Vec<u64>>>,
        cleanup_lock_ts: Arc<Mutex<Vec<u64>>>,
        detect_worker: FutureWorker<crate::server::lock_manager::deadlock::Task>,
    }

    fn start_waiter_manager_counting(
        wait_for_lock_timeout: u64,
    ) -> (FutureWorker<Task>, Scheduler, DetectorCounters) {
        let detect = Arc::new(AtomicUsize::new(0));
        let cleanup_wait_for = Arc::new(AtomicUsize::new(0));
        let detect_lock_ts = Arc::new(Mutex::new(Vec::new()));
        let cleanup_lock_ts = Arc::new(Mutex::new(Vec::new()));
        let mut detect_worker = FutureWorker::new("counting-deadlock");
        let detector_scheduler = DetectorScheduler::new(detect_worker.scheduler());
        detect_worker
            .start(CountingDetector {
                detect: detect.clone(),
                cleanup_wait_for: cleanup_wait_for.clone(),
                detect_lock_ts: detect_lock_ts.clone(),
                cleanup_lock_ts: cleanup_lock_ts.clone(),
            })
            .unwrap();

        let cfg = Config {
            wait_for_lock_timeout: ReadableDuration::millis(wait_for_lock_timeout),
            ..Default::default()
        };
        let mut waiter_mgr_worker = FutureWorker::new("test-waiter-manager");
        let waiter_mgr_runner =
            WaiterManager::new(Arc::new(AtomicUsize::new(0)), detector_scheduler, &cfg);
        let waiter_mgr_scheduler = Scheduler::new(waiter_mgr_worker.scheduler());
        waiter_mgr_worker.start(waiter_mgr_runner).unwrap();
        (
            waiter_mgr_worker,
            waiter_mgr_scheduler,
            DetectorCounters {
                detect,
                cleanup_wait_for,
                detect_lock_ts,
                cleanup_lock_ts,
                detect_worker,
            },
        )
    }

    fn wait_for_counts(counters: &DetectorCounters, expect_detect: usize, expect_cleanup: usize) {
        for _ in 0..100 {
            if counters.detect.load(Ordering::SeqCst) == expect_detect
                && counters.cleanup_wait_for.load(Ordering::SeqCst) == expect_cleanup
            {
                return;
            }
            sleep(Duration::from_millis(10));
        }
        panic!(
            "timeout waiting for detect={}, cleanup={}; got detect={}, cleanup={}",
            expect_detect,
            expect_cleanup,
            counters.detect.load(Ordering::SeqCst),
            counters.cleanup_wait_for.load(Ordering::SeqCst)
        );
    }

    fn flush_waiter_mgr(scheduler: &Scheduler) {
        let (tx, rx) = mpsc::channel();
        scheduler.validate(Box::new(move |_| {
            tx.send(()).unwrap();
        }));
        rx.recv().unwrap();
    }

    fn new_fair_test_waiter(waiter_ts: TimeStamp, lock_ts: TimeStamp, key: &[u8]) -> WaiterCtx {
        let (mut waiter, info, f) = new_test_waiter_with_key(waiter_ts, lock_ts, key);
        waiter.wait_info.allow_lock_with_conflict = true;
        (waiter, info, f)
    }

    fn owner_update_event(
        token: u64,
        start_ts: TimeStamp,
        key: &Key,
        new_lock_ts: TimeStamp,
        fair: bool,
        is_first_lock: bool,
    ) -> UpdateWaitForEvent {
        let raw = key.to_raw().unwrap();
        UpdateWaitForEvent {
            token: LockWaitToken(Some(token)),
            start_ts,
            is_first_lock,
            wait_info: KeyLockWaitInfo {
                key: key.clone(),
                lock_digest: LockDigest {
                    ts: new_lock_ts,
                    hash: key.gen_hash(),
                },
                lock_info: LockInfo {
                    key: raw,
                    lock_version: new_lock_ts.into_inner(),
                    ..Default::default()
                },
                allow_lock_with_conflict: fair,
            },
        }
    }

    /// N fair waiters × many owner changes produces N Cleanups of the
    /// original edge and zero Detects from update_wait_for. Message count
    /// does not grow with owner-change count (tikv#19846).
    #[test]
    fn test_fair_waiter_detector_rpc_bounded_across_owner_changes() {
        let (mut worker, scheduler, mut counters) = start_waiter_manager_counting(10_000);
        let raw_key = b"hot";
        let key = Key::from_raw(raw_key);
        let n = 20usize;
        let initial_lock_ts = 100u64;
        let owner_changes = 5usize;

        for i in 0..n {
            let waiter_ts = TimeStamp::new(1000 + i as u64);
            let (waiter, ..) = new_fair_test_waiter(waiter_ts, initial_lock_ts.into(), raw_key);
            scheduler.wait_for(
                LockWaitToken(Some(i as u64 + 1)),
                1,
                RegionEpoch::default(),
                1,
                waiter.start_ts,
                waiter.wait_info,
                WaitTimeout::Millis(10_000),
                waiter.cancel_callback,
                DiagnosticContext::default(),
                false,
            );
        }
        flush_waiter_mgr(&scheduler);
        // WaiterManager itself does not Detect on wait_for; LockManager does.
        wait_for_counts(&counters, 0, 0);

        for step in 1..=owner_changes {
            let new_ts = TimeStamp::new(initial_lock_ts + step as u64);
            let events: Vec<_> = (0..n)
                .map(|i| {
                    owner_update_event(
                        i as u64 + 1,
                        TimeStamp::new(1000 + i as u64),
                        &key,
                        new_ts,
                        true,
                        false,
                    )
                })
                .collect();
            scheduler.update_wait_for(events);
            flush_waiter_mgr(&scheduler);
            // First owner change: one Cleanup of the registered edge per waiter.
            // Later changes: no more detector RPC.
            wait_for_counts(&counters, 0, n);
        }

        assert_eq!(counters.detect.load(Ordering::SeqCst), 0);
        assert_eq!(counters.cleanup_wait_for.load(Ordering::SeqCst), n);
        let cleaned = counters.cleanup_lock_ts.lock().unwrap().clone();
        assert_eq!(cleaned.len(), n);
        assert!(
            cleaned.iter().all(|ts| *ts == initial_lock_ts),
            "cleanup must use the registered edge, got {:?}",
            cleaned
        );

        // Edge already cleaned on the first owner change: leaving must not
        // emit another Cleanup of the updated owner.
        for i in 0..n {
            scheduler.remove_lock_wait(LockWaitToken(Some(i as u64 + 1)));
        }
        flush_waiter_mgr(&scheduler);
        wait_for_counts(&counters, 0, n);

        worker.stop().unwrap();
        counters.detect_worker.stop().unwrap();
    }

    #[test]
    fn test_non_fair_waiter_skips_detector_on_owner_change() {
        let (mut worker, scheduler, mut counters) = start_waiter_manager_counting(10_000);
        let raw_key = b"hot";
        let key = Key::from_raw(raw_key);
        let (waiter, ..) = new_test_waiter_with_key(10.into(), 100.into(), raw_key);
        scheduler.wait_for(
            LockWaitToken(Some(1)),
            1,
            RegionEpoch::default(),
            1,
            waiter.start_ts,
            waiter.wait_info,
            WaitTimeout::Millis(10_000),
            waiter.cancel_callback,
            DiagnosticContext::default(),
            false,
        );
        flush_waiter_mgr(&scheduler);
        scheduler.update_wait_for(vec![owner_update_event(
            1,
            10.into(),
            &key,
            200.into(),
            false,
            false,
        )]);
        flush_waiter_mgr(&scheduler);
        wait_for_counts(&counters, 0, 0);
        // Leave must Cleanup the registered owner (100), not the updated 200.
        scheduler.remove_lock_wait(LockWaitToken(Some(1)));
        flush_waiter_mgr(&scheduler);
        wait_for_counts(&counters, 0, 1);
        assert_eq!(counters.cleanup_lock_ts.lock().unwrap().as_slice(), &[100]);
        worker.stop().unwrap();
        counters.detect_worker.stop().unwrap();
    }

    #[test]
    fn test_fair_waiter_cleanup_registered_edge_if_owner_never_changed() {
        let (mut worker, scheduler, mut counters) = start_waiter_manager_counting(10_000);
        let raw_key = b"hot";
        let (waiter, ..) = new_fair_test_waiter(10.into(), 100.into(), raw_key);
        scheduler.wait_for(
            LockWaitToken(Some(1)),
            1,
            RegionEpoch::default(),
            1,
            waiter.start_ts,
            waiter.wait_info,
            WaitTimeout::Millis(10_000),
            waiter.cancel_callback,
            DiagnosticContext::default(),
            false,
        );
        flush_waiter_mgr(&scheduler);
        scheduler.remove_lock_wait(LockWaitToken(Some(1)));
        flush_waiter_mgr(&scheduler);
        wait_for_counts(&counters, 0, 1);
        assert_eq!(counters.cleanup_lock_ts.lock().unwrap().as_slice(), &[100]);
        worker.stop().unwrap();
        counters.detect_worker.stop().unwrap();
    }

    #[test]
    fn test_first_lock_does_not_register_or_drop_edge() {
        let (mut worker, scheduler, mut counters) = start_waiter_manager_counting(10_000);
        let raw_key = b"hot";
        let key = Key::from_raw(raw_key);
        let (waiter, ..) = new_fair_test_waiter(10.into(), 100.into(), raw_key);
        scheduler.wait_for(
            LockWaitToken(Some(1)),
            1,
            RegionEpoch::default(),
            1,
            waiter.start_ts,
            waiter.wait_info,
            WaitTimeout::Millis(10_000),
            waiter.cancel_callback,
            DiagnosticContext::default(),
            true,
        );
        flush_waiter_mgr(&scheduler);
        scheduler.update_wait_for(vec![owner_update_event(
            1,
            10.into(),
            &key,
            200.into(),
            true,
            true,
        )]);
        flush_waiter_mgr(&scheduler);
        scheduler.remove_lock_wait(LockWaitToken(Some(1)));
        flush_waiter_mgr(&scheduler);
        wait_for_counts(&counters, 0, 0);
        worker.stop().unwrap();
        counters.detect_worker.stop().unwrap();
    }
}

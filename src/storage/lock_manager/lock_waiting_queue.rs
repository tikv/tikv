// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This mod contains the [`LockWaitQueues`] for managing waiting and waking up
//! of `AcquirePessimisticLock` requests in lock-contention scenarios, and other
//! related accessories, including:
//!
//! - [`SharedError`]: A wrapper type to [`crate::storage::Error`] to allow the
//!   error being shared in multiple places
//! - Related type aliases
//! - [`LockWaitEntry`]: which is used to represent lock-waiting requests in the
//!   queue
//! - [`Box<LockWaitEntry>`]: The comparable wrapper of [`LockWaitEntry`] which
//!   defines the priority ordering among lock-waiting requests
//!
//! Each key may have its own lock-waiting queue, which is a priority queue that
//! orders the entries with the order defined by
//! [`Box<LockWaitEntry>`].
//!
//! There are be two kinds of `AcquirePessimisticLock` requests:
//!
//! * Requests in legacy mode: indicated by `allow_lock_with_conflict = false`.
//!   A legacy request is woken up, it should return a `WriteConflict`
//!   immediately to the client to tell the client to retry. Then, the remaining
//!   lock-waiting entries should be woken up after delaying for
//!   `wake-up-delay-duration` which is a configurable value.
//! * Resumable requests: indicated by `allow_lock_with_conflict = true`.  This
//!   kind of requests are allowed to lock even if there is write conflict, When
//!   it's woken up after waiting for another lock, it can then resume execution
//!   and try to acquire the lock again. No delayed waking up is necessary.
//!   **Note that though the `LockWaitQueues` is designed to accept it, this
//!   kind of requests are currently not implemented yet.**
//!
//! ## Details about delayed waking up
//!
//! The delayed waking-up is needed after waking up a request in legacy mode.
//! The reasons are:
//!
//! * The head of the queue (let's denote its belonging transaction by `T1`) is
//!   woken up after the current lock being released, then the request will
//!   return a `WriteConflict` error immediately, and the key is left unlocked.
//!   It's possible that `T1` won't lock the key again. However, the other
//!   waiting requests need releasing-lock event to be woken up. In this case,
//!   we should not let them wait forever until timeout.
//! * When many transactions are blocked on the same key, and a transaction is
//!   granted the lock after another releasing the lock, the transaction that's
//!   blocking other transactions is changed. After cancelling the other
//!   transactions and let them retry the `AcquirePessimisticLock` request, they
//!   will be able to re-detect deadlocks with the latest information.
//!
//! To achieve this, after delaying for `wake-up-delay-duration` since the
//! latest waking-up event on the key, a call to
//! [`LockWaitQueues::delayed_notify_all`] will be made. However, since the
//! [`LockWaitQueues`] do not have its own thread pool, the user may receive a
//! future after calling some of the functions, and the user will be responsible
//! for executing the future in a suitable place.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::{self, mapref::entry::Entry as DashMapEntry};
use futures_util::compat::Future01CompatExt;
use keyed_priority_queue::KeyedPriorityQueue;
use kvproto::kvrpcpb;
use smallvec::SmallVec;
use sync_wrapper::SyncWrapper;
use tikv_util::{time::InstantExt, timer::GLOBAL_TIMER_HANDLE};
use txn_types::{Key, TimeStamp};

use crate::storage::{
    Error as StorageError, ErrorInner as StorageErrorInner,
    lock_manager::{
        KeyLockWaitInfo, LockDigest, LockManager, LockWaitToken, UpdateWaitForEvent,
        lock_wait_context::{LockWaitContextSharedState, PessimisticLockKeyCallback},
    },
    metrics::*,
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, SharedLockUpgradeHint},
    txn::{Error as TxnError, ErrorInner as TxnErrorInner},
    types::PessimisticLockParameters,
};

/// Represents an `AcquirePessimisticLock` request that's waiting for a lock,
/// and contains the request's parameters.
pub struct LockWaitEntry {
    pub key: Key,
    pub lock_hash: u64,
    pub parameters: PessimisticLockParameters,
    // `parameters` provides parameter for a request, but `should_not_exist` is specified key-wise.
    // Put it in a separated field.
    pub should_not_exist: bool,
    /// Whether the waiting request is acquiring a shared lock.
    pub is_shared_lock: bool,
    /// Whether this waiting request is a same-key shared-lock upgrade.
    pub is_lock_upgrade: bool,
    pub lock_wait_token: LockWaitToken,
    pub req_states: Arc<LockWaitContextSharedState>,
    pub legacy_wake_up_index: Option<usize>,
    pub key_cb: Option<SyncWrapper<PessimisticLockKeyCallback>>,
}

impl PartialEq<Self> for LockWaitEntry {
    fn eq(&self, other: &Self) -> bool {
        self.parameters.start_ts == other.parameters.start_ts
    }
}

impl Eq for LockWaitEntry {}

impl PartialOrd<Self> for LockWaitEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Reverse it since the priority queue is a max heap and we want to pop the
        // minimal.
        other
            .parameters
            .start_ts
            .partial_cmp(&self.parameters.start_ts)
    }
}

impl Ord for LockWaitEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse it since the priority queue is a max heap and we want to pop the
        // minimal.
        other.parameters.start_ts.cmp(&self.parameters.start_ts)
    }
}

#[derive(Clone, Copy)]
struct PendingUpgrader {
    start_ts: TimeStamp,
    token: LockWaitToken,
}

pub struct KeyLockWaitState {
    #[allow(dead_code)]
    current_lock: kvrpcpb::LockInfo,

    /// The counter of wake up events of legacy pessimistic lock requests
    /// (`allow_lock_with_conflict == false`). When an lock wait entry is
    /// pushed to the queue, it records the current counter. The purpose
    /// is to mark the entries that needs to be woken up after delaying.
    ///
    /// Here is an example showing how it works (note that requests in
    /// the example are all in legacy mode):
    ///
    /// Let's denote a lock-wait entry by `(start_ts,
    /// legacy_wake_up_index)`. Consider there are three requests with
    /// start_ts 20, 30, 40 respectively, and they are pushed to the
    /// queue when the `KeyLockWaitState::legacy_wake_up_index` is 0. Then the
    /// `KeyLockWaitState` is:
    ///
    /// ```text
    /// legacy_wake_up_index: 0, queue: [(20, 0), (30, 0), (40, 0)]
    /// ```
    ///
    /// Then the lock on the key is released. We pops the first entry in the
    /// queue to wake it up, and then schedule a call to
    /// [`LockWaitQueues::delayed_notify_all`] after delaying for
    /// `wake_up_delay_duration`. The current state becomes:
    ///
    /// ```text
    /// legacy_wake_up_index: 1, queue: [(30, 0), (40, 0)]
    /// ````
    ///
    /// Here, if some other request arrives, one of them may successfully
    /// acquire the lock and others are pushed to the queue. the state
    /// becomes:
    ///
    /// ```text
    /// legacy_wake_up_index: 1, queue: [(30, 0), (40, 0), (50, 1), (60, 1)]
    /// ```
    ///
    /// Then `wake_up_delay_duration` is elapsed since the previous waking up.
    /// Here, we expect that the lock wait entries 30 and 40 can be woken
    /// up, since they exists when the previous waking up occurs. But we
    /// don't want to wake up later-arrived entries (50 and 60) since it
    /// introduces useless pessimistic retries to transaction 50 and 60 when
    /// they don't need to. The solution is, only wake up the entries that
    /// has `entry.legacy_wake_up_index <
    /// key_lock_wait_state.legacy_wake_up_index`. Therefore, we only wakes up
    /// entries 30 and 40 who has `legacy_wake_up_index < 1`, while 50
    /// and 60 will be left untouched.
    ///
    /// When waking up resumable requests, the mechanism above won't take
    /// effect. If a legacy request is woken up and triggered the mechanism,
    /// and there is a resumable request in the queue, `delayed_notify_all`
    /// will stop at the first resumable request it meets, pop it out, and
    /// return it from a [`DelayedNotifyAllFuture`]. See
    /// [`LockWaitQueues::pop_for_waking_up`].
    legacy_wake_up_index: usize,
    queue: KeyedPriorityQueue<
        LockWaitToken,
        Box<LockWaitEntry>,
        std::hash::BuildHasherDefault<fxhash::FxHasher>,
    >,

    /// The start_ts of the most recent waking up event.
    last_conflict_start_ts: TimeStamp,
    /// The commit_ts of the most recent waking up event.
    last_conflict_commit_ts: TimeStamp,

    /// `(id, start_time, delay_duration)`
    delayed_notify_all_state: Option<(u64, Instant, Arc<AtomicU64>)>,
    pending_upgrader: Option<PendingUpgrader>,
}

impl KeyLockWaitState {
    fn new() -> Self {
        Self {
            current_lock: kvrpcpb::LockInfo::default(),
            legacy_wake_up_index: 0,
            queue: KeyedPriorityQueue::default(),
            last_conflict_start_ts: TimeStamp::zero(),
            last_conflict_commit_ts: TimeStamp::zero(),
            delayed_notify_all_state: None,
            pending_upgrader: None,
        }
    }
}

pub type DelayedNotifyAllFuture = Pin<Box<dyn Future<Output = Option<Box<LockWaitEntry>>> + Send>>;

enum PendingUpgraderWakeupKind {
    Retry,
    SharedLockLost,
}

pub struct LockWaitQueueInner<L: LockManager> {
    queue_map: dashmap::DashMap<Key, KeyLockWaitState>,
    id_allocated: AtomicU64,
    entries_count: AtomicUsize,
    lock_mgr: L,
}

#[derive(Clone)]
pub struct LockWaitQueues<L: LockManager> {
    inner: Arc<LockWaitQueueInner<L>>,
}

impl<L: LockManager> LockWaitQueues<L> {
    pub fn new(lock_mgr: L) -> Self {
        Self {
            inner: Arc::new(LockWaitQueueInner {
                queue_map: dashmap::DashMap::new(),
                id_allocated: AtomicU64::new(1),
                entries_count: AtomicUsize::new(0),
                lock_mgr,
            }),
        }
    }

    /// Enqueues a lock wait entry. The key is indicated by the `key` field of
    /// the `lock_wait_entry`. The caller also needs to provide the
    /// information of the current-holding lock.
    ///
    /// Returns whether the entry is actually inserted into the queue.
    pub fn push_lock_wait(
        &self,
        mut lock_wait_entry: Box<LockWaitEntry>,
        current_lock: kvrpcpb::LockInfo,
    ) -> bool {
        let mut new_key = false;

        let map_entry = self.inner.queue_map.entry(lock_wait_entry.key.clone());

        // If it's not the first time the request is put into the queue, the request
        // might be canceled from outside when the entry is temporarily absent
        // in the queue. In this case, the cancellation operation is not done.
        // Do it here. For details about this corner case, see document of
        // `LockWaitContext::is_canceled` field.
        if lock_wait_entry.req_states.is_canceled() {
            self.on_push_canceled_entry(lock_wait_entry, map_entry);
            return false;
        }

        let mut key_state = map_entry.or_insert_with(|| {
            new_key = true;
            KeyLockWaitState::new()
        });
        if !current_lock.key.is_empty() {
            key_state.current_lock = current_lock;
        }

        if lock_wait_entry.legacy_wake_up_index.is_none() {
            lock_wait_entry.legacy_wake_up_index = Some(key_state.value().legacy_wake_up_index);
        }

        // Shared-lock upgrade waits have a stricter invariant than ordinary waiters:
        // each key may have at most one pending upgrader. A later upgrader is rejected
        // immediately as DuplicateInFlight / SecondUpgrader instead of entering the
        // ordinary wait queue; otherwise it would keep its shared lock while queued
        // behind the current upgrader and create an unresolvable deadlock.
        if lock_wait_entry.is_lock_upgrade {
            let pending_upgrader = key_state.value().pending_upgrader;
            match pending_upgrader {
                None => {
                    key_state.pending_upgrader = Some(PendingUpgrader {
                        start_ts: lock_wait_entry.parameters.start_ts,
                        token: lock_wait_entry.lock_wait_token,
                    });
                }
                Some(pending_upgrader) => {
                    // Release the queue-map guard before invoking external callback.
                    drop(key_state);

                    let cb = lock_wait_entry.key_cb.unwrap().into_inner();
                    let reason = if pending_upgrader.start_ts == lock_wait_entry.parameters.start_ts
                    {
                        LOCK_UPGRADE_CONFLICT_COUNTER_VEC.duplicate_in_flight.inc();
                        kvrpcpb::LockUpgradeConflictReason::DuplicateInFlight
                    } else {
                        LOCK_UPGRADE_CONFLICT_COUNTER_VEC.second_upgrader.inc();
                        kvrpcpb::LockUpgradeConflictReason::SecondUpgrader
                    };
                    let err = StorageError::from(TxnError::from(MvccError::from(
                        MvccErrorInner::LockUpgradeConflict {
                            start_ts: lock_wait_entry.parameters.start_ts,
                            owner_start_ts: pending_upgrader.start_ts,
                            key: lock_wait_entry.key.clone().into_raw().unwrap(),
                            reason,
                        },
                    )));
                    cb(Err(err.into()), true);
                    return false;
                }
            }
        }

        key_state
            .value_mut()
            .queue
            .push(lock_wait_entry.lock_wait_token, lock_wait_entry);
        self.inner.entries_count.fetch_add(1, Ordering::SeqCst);

        let len = key_state.value_mut().queue.len();
        drop(key_state);
        LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.waiters.inc();
        LOCK_WAIT_QUEUE_LENGTH_HISTOGRAM.observe(len as f64);
        if new_key {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.keys.inc()
        }
        true
    }

    fn clear_pending_upgrader_if_matches(
        key_state: &mut KeyLockWaitState,
        lock_wait_token: LockWaitToken,
    ) {
        if key_state
            .pending_upgrader
            .as_ref()
            .is_some_and(|pending| pending.token == lock_wait_token)
        {
            key_state.pending_upgrader = None;
        }
    }

    fn finish_pending_upgrader(
        lock_wait_entry: Box<LockWaitEntry>,
        wakeup_kind: PendingUpgraderWakeupKind,
    ) {
        let cb = lock_wait_entry.key_cb.unwrap().into_inner();
        let err = match wakeup_kind {
            PendingUpgraderWakeupKind::Retry => StorageError::from(TxnError::from(
                MvccError::from(MvccErrorInner::WriteConflict {
                    start_ts: lock_wait_entry.parameters.start_ts,
                    conflict_start_ts: TimeStamp::zero(),
                    conflict_commit_ts: TimeStamp::zero(),
                    key: lock_wait_entry.key.into_raw().unwrap(),
                    primary: lock_wait_entry.parameters.primary,
                    reason: kvrpcpb::WriteConflictReason::PessimisticRetry,
                }),
            )),
            PendingUpgraderWakeupKind::SharedLockLost => StorageError::from(TxnError::from(
                MvccError::from(MvccErrorInner::SharedLockLost {
                    start_ts: lock_wait_entry.parameters.start_ts,
                    key: lock_wait_entry.key.into_raw().unwrap(),
                }),
            )),
        };
        cb(Err(err.into()), false);
    }

    fn on_push_canceled_entry(
        &self,
        lock_wait_entry: Box<LockWaitEntry>,
        key_state: DashMapEntry<'_, Key, KeyLockWaitState, impl std::hash::BuildHasher>,
    ) {
        fail_point!("lock_wait_queue_before_canceled_error");
        let latest_lock_info = match &key_state {
            DashMapEntry::Occupied(entry) => {
                let info = &entry.get().current_lock;
                (!info.key.is_empty()).then(|| info.clone())
            }
            DashMapEntry::Vacant(_) => None,
        };
        // Cancellation must acquire this shard lock before sending the error.
        // Release both occupied and vacant guards before waiting for it.
        drop(key_state);
        let mut err = lock_wait_entry.req_states.get_external_error();

        if let StorageError(box StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(
            MvccError(box MvccErrorInner::KeyIsLocked(lock_info)),
        )))) = &mut err
        {
            // Update the error with the lock info observed before releasing the shard.
            if let Some(latest_lock_info) = latest_lock_info {
                *lock_info = latest_lock_info;
            }
        }

        let cb = lock_wait_entry.key_cb.unwrap().into_inner();
        cb(Err(err.into()), true);
    }

    /// Dequeues the head of the lock waiting queue of the specified key,
    /// assuming the popped entry will be woken up.
    ///
    /// If it's waking up a legacy request and the queue is not empty, a future
    /// will be returned and the caller will be responsible for executing it.
    /// The future waits until `wake_up_delay_duration` is elapsed since the
    /// most recent waking-up, and then wakes up all lock waiting entries that
    /// exists at the time when the latest waking-up happens. The future will
    /// return a `LockWaitEntry` if a resumable entry is popped out from the
    /// queue while executing, and in this case the caller will be responsible
    /// to wake it up. When the head of the queue is a shared lock wait, all
    /// consecutive shared lock waits are returned together in the vector.
    #[allow(clippy::vec_box)]
    pub fn pop_for_waking_up(
        &self,
        key: &Key,
        conflicting_start_ts: TimeStamp,
        conflicting_commit_ts: TimeStamp,
        wake_up_delay_duration_ms: u64,
    ) -> (Vec<Box<LockWaitEntry>>, Option<DelayedNotifyAllFuture>) {
        self.pop_for_waking_up_impl(
            key,
            conflicting_start_ts,
            conflicting_commit_ts,
            Some(wake_up_delay_duration_ms),
        )
    }

    #[allow(clippy::vec_box)]
    fn pop_for_waking_up_impl(
        &self,
        key: &Key,
        conflicting_start_ts: TimeStamp,
        conflicting_commit_ts: TimeStamp,
        wake_up_delay_duration_ms: Option<u64>,
    ) -> (Vec<Box<LockWaitEntry>>, Option<DelayedNotifyAllFuture>) {
        let mut popped_entries: Vec<Box<LockWaitEntry>> = Vec::new();
        let mut popped_future: Option<DelayedNotifyAllFuture> = None;
        // For statistics.
        let mut removed_waiters = 0usize;

        // We don't want other threads insert any more entries between finding the
        // queue is empty and removing the queue from the map. Wrap the logic
        // within a call to `remove_if_mut` to avoid releasing lock during the
        // procedure.
        let removed_key = self.inner.queue_map.remove_if_mut(key, |_, v| {
            v.last_conflict_start_ts = conflicting_start_ts;
            v.last_conflict_commit_ts = conflicting_commit_ts;

            if let Some((_, lock_wait_entry)) = v.queue.pop() {
                removed_waiters += 1;
                Self::clear_pending_upgrader_if_matches(v, lock_wait_entry.lock_wait_token);

                let mut group_entries = Vec::with_capacity(4);
                let mut schedule_delayed_wake_up = false;

                if !lock_wait_entry.parameters.allow_lock_with_conflict {
                    v.legacy_wake_up_index += 1;
                    schedule_delayed_wake_up = true;
                }

                let shared_group = lock_wait_entry.is_shared_lock;
                group_entries.push(lock_wait_entry);

                if shared_group {
                    while let Some((_, front)) = v.queue.peek() {
                        if !front.is_shared_lock {
                            break;
                        }
                        let (_, shared_entry) = v.queue.pop().unwrap();
                        removed_waiters += 1;
                        Self::clear_pending_upgrader_if_matches(v, shared_entry.lock_wait_token);
                        if !shared_entry.parameters.allow_lock_with_conflict {
                            v.legacy_wake_up_index += 1;
                            schedule_delayed_wake_up = true;
                        }
                        group_entries.push(shared_entry);
                    }
                }

                let notify_all_future = if schedule_delayed_wake_up {
                    match wake_up_delay_duration_ms {
                        Some(delay) if !v.queue.is_empty() => {
                            self.handle_delayed_wake_up(v, key, delay)
                        }
                        _ => None,
                    }
                } else {
                    None
                };

                popped_future = notify_all_future;
                popped_entries = group_entries;
            }

            self.inner
                .entries_count
                .fetch_sub(removed_waiters, Ordering::SeqCst);

            // Remove the queue if it's emptied.
            v.queue.is_empty()
        });

        if removed_waiters != 0 {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC
                .waiters
                .sub(removed_waiters as i64);
        }
        if removed_key.is_some() {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.keys.dec();
        }

        (popped_entries, popped_future)
    }

    /// Schedule delayed waking up on the specified key.
    ///
    /// Returns a future if it's needed to spawn a new async task to do the
    /// delayed waking up. The caller should be responsible for executing
    /// it.
    fn handle_delayed_wake_up(
        &self,
        key_lock_wait_state: &mut KeyLockWaitState,
        key: &Key,
        wake_up_delay_duration_ms: u64,
    ) -> Option<DelayedNotifyAllFuture> {
        if let Some((_, start_time, delay_duration)) =
            &mut key_lock_wait_state.delayed_notify_all_state
        {
            // There's already an async task spawned for handling delayed waking up on this
            // key. Update its state to extend its delaying duration (until now
            // + wake_up_delay_duration).
            let new_delay_duration =
                (start_time.saturating_elapsed().as_millis() as u64) + wake_up_delay_duration_ms;
            delay_duration.store(new_delay_duration, Ordering::Release);
            None
        } else {
            // It's needed to spawn a new async task for performing delayed waking up on
            // this key. Return a future to let the caller execute it in a
            // proper thread pool.
            let notify_id = self.allocate_internal_id();
            let start_time = Instant::now();
            let delay_ms = Arc::new(AtomicU64::new(wake_up_delay_duration_ms));

            key_lock_wait_state.delayed_notify_all_state =
                Some((notify_id, start_time, delay_ms.clone()));
            Some(Box::pin(self.clone().async_delayed_notify_all(
                key.clone(),
                start_time,
                delay_ms,
                notify_id,
            )))
        }
    }

    fn allocate_internal_id(&self) -> u64 {
        self.inner.id_allocated.fetch_add(1, Ordering::SeqCst)
    }

    async fn async_delayed_notify_all(
        self,
        key: Key,
        start_time: Instant,
        delay_ms: Arc<AtomicU64>,
        notify_id: u64,
    ) -> Option<Box<LockWaitEntry>> {
        let mut prev_delay_ms = 0;
        // The delay duration may be extended by later waking-up events, by updating the
        // value of `delay_ms`. So we loop until we find that the elapsed
        // duration is larger than `delay_ms`.
        loop {
            let current_delay_ms = delay_ms.load(Ordering::Acquire);
            if current_delay_ms == 0 {
                // Cancelled.
                return None;
            }

            if current_delay_ms <= prev_delay_ms
                || (start_time.saturating_elapsed().as_millis() as u64) >= current_delay_ms
            {
                // Timed out.
                break;
            }

            let deadline = start_time + Duration::from_millis(current_delay_ms);

            GLOBAL_TIMER_HANDLE.delay(deadline).compat().await.unwrap();

            prev_delay_ms = current_delay_ms;
        }

        fail_point!("lock_waiting_queue_before_delayed_notify_all");

        self.delayed_notify_all(&key, notify_id)
    }

    fn delayed_notify_all(&self, key: &Key, notify_id: u64) -> Option<Box<LockWaitEntry>> {
        let mut popped_lock_wait_entries = SmallVec::<[_; 4]>::new();

        let mut woken_up_resumable_entry = None;
        let mut conflicting_start_ts = TimeStamp::zero();
        let mut conflicting_commit_ts = TimeStamp::zero();

        let mut removed_waiters = 0usize;

        // We don't want other threads insert any more entries between finding the
        // queue is empty and removing the queue from the map. Wrap the logic
        // within a call to `remove_if_mut` to avoid releasing lock during the
        // procedure.
        let removed_key = self.inner.queue_map.remove_if_mut(key, |_, v| {
            // The KeyLockWaitState of the key might have been removed from the map and then
            // recreated. Skip.
            if v.delayed_notify_all_state
                .as_ref()
                .is_none_or(|(id, ..)| *id != notify_id)
            {
                return false;
            }

            // Clear the state which indicates the scheduled `delayed_notify_all` has
            // finished.
            v.delayed_notify_all_state = None;

            conflicting_start_ts = v.last_conflict_start_ts;
            conflicting_commit_ts = v.last_conflict_commit_ts;

            let legacy_wake_up_index = v.legacy_wake_up_index;

            while let Some((_, front)) = v.queue.peek() {
                if front
                    .legacy_wake_up_index
                    .is_some_and(|idx| idx >= legacy_wake_up_index)
                {
                    // This entry is added after the legacy-wakeup that issued the current
                    // delayed_notify_all operation. Keep it and other remaining items in the queue.
                    break;
                }
                let (_, lock_wait_entry) = v.queue.pop().unwrap();
                Self::clear_pending_upgrader_if_matches(v, lock_wait_entry.lock_wait_token);
                removed_waiters += 1;
                if lock_wait_entry.parameters.allow_lock_with_conflict {
                    woken_up_resumable_entry = Some(lock_wait_entry);
                    break;
                }
                popped_lock_wait_entries.push(lock_wait_entry);
            }

            self.inner
                .entries_count
                .fetch_sub(removed_waiters, Ordering::SeqCst);

            // If the queue is empty, remove it from the map.
            v.queue.is_empty()
        });

        if removed_waiters != 0 {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC
                .waiters
                .sub(removed_waiters as i64);
        }
        if removed_key.is_some() {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.keys.dec();
        }

        // Call callbacks to cancel these entries here.
        // TODO: Perhaps we'd better make it concurrent with scheduling the new command
        // (if `woken_up_resumable_entry` is some) if there are too many.
        for lock_wait_entry in popped_lock_wait_entries {
            let lock_wait_entry = *lock_wait_entry;
            let cb = lock_wait_entry.key_cb.unwrap().into_inner();
            let e = StorageError::from(TxnError::from(MvccError::from(
                MvccErrorInner::WriteConflict {
                    start_ts: lock_wait_entry.parameters.start_ts,
                    conflict_start_ts: conflicting_start_ts,
                    conflict_commit_ts: conflicting_commit_ts,
                    key: lock_wait_entry.key.into_raw().unwrap(),
                    primary: lock_wait_entry.parameters.primary,
                    reason: kvrpcpb::WriteConflictReason::PessimisticRetry,
                },
            )));
            cb(Err(e.into()), false);
        }

        // Return the item to be woken up in resumable way.
        woken_up_resumable_entry
    }

    /// Finds a specific LockWaitEntry by key and token, and removes it from the
    /// queue. No extra operation will be performed on the removed entry.
    /// The caller is responsible for finishing or cancelling the request to
    /// let it return the response to the client.
    pub fn remove_by_token(
        &self,
        key: &Key,
        lock_wait_token: LockWaitToken,
    ) -> Option<Box<LockWaitEntry>> {
        let mut result = None;

        // We don't want other threads insert any more entries between finding the
        // queue is empty and removing the queue from the map. Wrap the logic
        // within a call to `remove_if_mut` to avoid releasing lock during the
        // procedure.
        let removed_key = self.inner.queue_map.remove_if_mut(key, |_, v| {
            if let Some(res) = v.queue.remove(&lock_wait_token) {
                self.inner.entries_count.fetch_sub(1, Ordering::SeqCst);
                LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.waiters.dec();
                Self::clear_pending_upgrader_if_matches(v, lock_wait_token);
                result = Some(res);
            }
            v.queue.is_empty()
        });

        if removed_key.is_some() {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.keys.dec();
        }

        result
    }

    pub fn has_pending_upgrader(&self, key: &Key) -> bool {
        self.inner
            .queue_map
            .get(key)
            .is_some_and(|state| state.pending_upgrader.is_some())
    }

    pub fn update_lock_wait(&self, lock_info: Vec<kvrpcpb::LockInfo>) {
        let mut update_wait_for_events = vec![];
        for lock_info in lock_info {
            let key = Key::from_raw(lock_info.get_key());
            if !lock_info.get_shared_lock_infos().is_empty() {
                let stale = self
                    .inner
                    .queue_map
                    .get(&key)
                    .and_then(|state| state.pending_upgrader)
                    .filter(|pending| {
                        !lock_info
                            .get_shared_lock_infos()
                            .iter()
                            .any(|holder| holder.lock_version == pending.start_ts.into_inner())
                    });
                if let Some(entry) =
                    stale.and_then(|pending| self.remove_by_token(&key, pending.token))
                {
                    Self::finish_pending_upgrader(entry, PendingUpgraderWakeupKind::SharedLockLost);
                }
            }
            if let Some(mut key_state) = self.inner.queue_map.get_mut(&key) {
                key_state.current_lock = lock_info;
                update_wait_for_events.reserve(key_state.queue.len());
                for (&token, entry) in key_state.queue.iter() {
                    let event = UpdateWaitForEvent {
                        token,
                        start_ts: entry.parameters.start_ts,
                        is_first_lock: entry.parameters.is_first_lock,
                        wait_info: KeyLockWaitInfo {
                            key: key.clone(),
                            lock_digest: LockDigest {
                                ts: key_state.current_lock.lock_version.into(),
                                hash: entry.lock_hash,
                            },
                            lock_info: key_state.current_lock.clone(),
                            allow_lock_with_conflict: entry.parameters.allow_lock_with_conflict,
                        },
                    };
                    update_wait_for_events.push(event);
                }
            }
        }
        if !update_wait_for_events.is_empty() {
            self.inner.lock_mgr.update_wait_for(update_wait_for_events);
        }
    }

    /// Completes the current pending upgrader for each one-holder boundary
    /// hint.
    ///
    /// The caller must hold the latch for every hinted key until its write
    /// finishes.
    pub fn consume_shared_lock_upgrade_hints(&self, hints: Vec<SharedLockUpgradeHint>) {
        for hint in hints {
            let pending = self
                .inner
                .queue_map
                .get(&hint.key)
                .and_then(|key_state| key_state.pending_upgrader);
            let Some(pending) = pending else {
                continue;
            };
            let wakeup_kind = if pending.start_ts == hint.sole_holder_start_ts {
                PendingUpgraderWakeupKind::Retry
            } else {
                PendingUpgraderWakeupKind::SharedLockLost
            };

            if let Some(entry) = self.remove_by_token(&hint.key, pending.token) {
                Self::finish_pending_upgrader(entry, wakeup_kind);
            }
        }
    }

    /// Gets the count of entries currently waiting in queues.
    ///
    /// Mind that the contents of the queues may be changed concurrently.
    pub fn entry_count(&self) -> usize {
        self.inner.entries_count.load(Ordering::SeqCst)
    }

    /// Checks whether there's nothing at all waiting in queue.
    ///
    /// Mind that the contents of the queues may be changed concurrently.
    pub fn is_empty(&self) -> bool {
        self.entry_count() == 0
    }

    #[allow(dead_code)]
    pub(super) fn get_lock_mgr(&self) -> &L {
        &self.inner.lock_mgr
    }

    #[cfg(test)]
    pub fn must_not_contain_key(&self, key: &[u8]) {
        assert!(self.inner.queue_map.get(&Key::from_raw(key)).is_none());
    }

    #[cfg(test)]
    pub fn must_have_next_entry(&self, key: &[u8], start_ts: impl Into<TimeStamp>) {
        assert_eq!(
            self.inner
                .queue_map
                .get(&Key::from_raw(key))
                .unwrap()
                .queue
                .peek()
                .unwrap()
                .1
                .parameters
                .start_ts,
            start_ts.into()
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Mutex,
            mpsc::{Receiver, RecvTimeoutError, channel},
        },
        thread,
        time::Duration,
    };

    use super::*;
    use crate::storage::{
        ErrorInner as StorageErrorInner, PessimisticLockKeyResult, PessimisticLockResults,
        StorageCallback,
        errors::SharedError,
        lock_manager::{MockLockManager, WaitTimeout, lock_wait_context::LockWaitContext},
        txn::ErrorInner as TxnErrorInner,
    };

    const LOCK_UPGRADE_CONFLICT_METRIC: &str = "tikv_lock_upgrade_conflict_total";
    static LOCK_UPGRADE_CONFLICT_METRIC_TEST_MUTEX: Mutex<()> = Mutex::new(());

    fn lock_upgrade_conflict_count(reason: &str) -> f64 {
        for family in prometheus::gather() {
            if family.get_name() != LOCK_UPGRADE_CONFLICT_METRIC {
                continue;
            }
            for metric in family.get_metric() {
                let has_reason = metric
                    .get_label()
                    .iter()
                    .any(|label| label.get_name() == "reason" && label.get_value() == reason);
                if has_reason {
                    return metric.get_counter().get_value();
                }
            }
        }
        0.0
    }

    struct TestLockWaitEntryHandle {
        token: LockWaitToken,
        wake_up_rx: Receiver<Result<PessimisticLockKeyResult, SharedError>>,
        cancel_cb: Box<dyn FnOnce()>,
    }

    struct TestUpgradeWaitRequestHandle {
        request_rx:
            Receiver<crate::storage::Result<crate::storage::Result<PessimisticLockResults>>>,
        cancel_cb: Box<dyn FnOnce()>,
    }

    fn upgrade_hint(key: &[u8], sole_holder_start_ts: u64) -> SharedLockUpgradeHint {
        SharedLockUpgradeHint {
            key: Key::from_raw(key),
            sole_holder_start_ts: sole_holder_start_ts.into(),
        }
    }

    impl TestLockWaitEntryHandle {
        fn wait_for_result_timeout(
            &self,
            timeout: Duration,
        ) -> Option<Result<PessimisticLockKeyResult, SharedError>> {
            match self.wake_up_rx.recv_timeout(timeout) {
                Ok(res) => Some(res),
                Err(RecvTimeoutError::Timeout) => None,
                Err(e) => panic!(
                    "unexpected error when receiving result of a LockWaitEntry: {:?}",
                    e
                ),
            }
        }

        fn wait_for_result(self) -> Result<PessimisticLockKeyResult, SharedError> {
            self.wake_up_rx
                .recv_timeout(Duration::from_secs(10))
                .unwrap()
        }

        fn cancel(self) {
            (self.cancel_cb)();
        }
    }

    // Additionally add some helper functions to the LockWaitQueues for simplifying
    // test code.
    impl<L: LockManager> LockWaitQueues<L> {
        pub fn make_lock_info_pb(&self, key: &[u8], ts: impl Into<TimeStamp>) -> kvrpcpb::LockInfo {
            let ts = ts.into();
            let mut lock_info = kvrpcpb::LockInfo::default();
            lock_info.set_lock_version(ts.into_inner());
            lock_info.set_lock_for_update_ts(ts.into_inner());
            lock_info.set_key(key.to_owned());
            lock_info.set_primary_lock(key.to_owned());
            lock_info
        }

        fn make_mock_lock_wait_entry(
            &self,
            key: &[u8],
            start_ts: impl Into<TimeStamp>,
            lock_info_pb: kvrpcpb::LockInfo,
        ) -> (Box<LockWaitEntry>, TestLockWaitEntryHandle) {
            let start_ts = start_ts.into();
            let token = self.inner.lock_mgr.allocate_token();
            let dummy_request_cb = StorageCallback::PessimisticLock(Box::new(|_| ()));
            let dummy_ctx = LockWaitContext::new(
                Key::from_raw(key),
                self.clone(),
                token,
                dummy_request_cb,
                false,
            );

            let parameters = PessimisticLockParameters {
                pb_ctx: Default::default(),
                primary: key.to_owned(),
                start_ts,
                lock_ttl: 1000,
                for_update_ts: start_ts,
                wait_timeout: Some(WaitTimeout::Default),
                return_values: false,
                min_commit_ts: 0.into(),
                check_existence: false,
                is_first_lock: false,
                lock_only_if_exists: false,
                allow_lock_with_conflict: false,
                is_shared_lock_upgrade: false,
            };

            let key = Key::from_raw(key);
            let lock_hash = key.gen_hash();
            let (tx, rx) = channel();
            let lock_wait_entry = Box::new(LockWaitEntry {
                key,
                lock_hash,
                parameters,
                should_not_exist: false,
                is_shared_lock: false,
                is_lock_upgrade: false,
                lock_wait_token: token,
                req_states: dummy_ctx.get_shared_states().clone(),
                legacy_wake_up_index: None,
                key_cb: Some(SyncWrapper::new(Box::new(move |res, _| {
                    tx.send(res).unwrap()
                }))),
            });

            let cancel_callback = dummy_ctx.get_callback_for_cancellation();
            let cancel = move || {
                cancel_callback(StorageError::from(TxnError::from(MvccError::from(
                    MvccErrorInner::KeyIsLocked(lock_info_pb),
                ))))
            };

            (
                lock_wait_entry,
                TestLockWaitEntryHandle {
                    token,
                    wake_up_rx: rx,
                    cancel_cb: Box::new(cancel),
                },
            )
        }

        fn mock_lock_wait(
            &self,
            key: &[u8],
            start_ts: impl Into<TimeStamp>,
            encountered_lock_ts: impl Into<TimeStamp>,
            resumable: bool,
        ) -> TestLockWaitEntryHandle {
            self.mock_lock_wait_with_shared(key, start_ts, encountered_lock_ts, resumable, false)
        }

        fn mock_lock_wait_with_shared(
            &self,
            key: &[u8],
            start_ts: impl Into<TimeStamp>,
            encountered_lock_ts: impl Into<TimeStamp>,
            resumable: bool,
            shared: bool,
        ) -> TestLockWaitEntryHandle {
            let mut lock_info_pb = self.make_lock_info_pb(key, encountered_lock_ts);
            if shared {
                lock_info_pb.set_lock_type(kvrpcpb::Op::SharedPessimisticLock);
            }
            let (mut entry, handle) =
                self.make_mock_lock_wait_entry(key, start_ts, lock_info_pb.clone());
            entry.parameters.allow_lock_with_conflict = resumable;
            entry.is_shared_lock = shared;
            self.push_lock_wait(entry, lock_info_pb);
            handle
        }

        fn mock_upgrade_wait(
            &self,
            key: &[u8],
            start_ts: impl Into<TimeStamp>,
            encountered_lock_ts: impl Into<TimeStamp>,
        ) -> TestLockWaitEntryHandle {
            let start_ts = start_ts.into();
            let mut lock_info_pb = self.make_lock_info_pb(key, encountered_lock_ts);
            lock_info_pb.set_lock_type(kvrpcpb::Op::SharedLock);

            let mut holder = self.make_lock_info_pb(key, start_ts);
            holder.set_lock_type(kvrpcpb::Op::PessimisticLock);
            lock_info_pb.set_shared_lock_infos(vec![holder].into());

            let (mut entry, handle) =
                self.make_mock_lock_wait_entry(key, start_ts, lock_info_pb.clone());
            entry.is_lock_upgrade = true;
            self.push_lock_wait(entry, lock_info_pb);
            handle
        }

        fn mock_upgrade_wait_with_request_callback(
            &self,
            key: &[u8],
            start_ts: impl Into<TimeStamp>,
            encountered_lock_ts: impl Into<TimeStamp>,
        ) -> TestUpgradeWaitRequestHandle {
            let start_ts = start_ts.into();
            let mut lock_info_pb = self.make_lock_info_pb(key, encountered_lock_ts);
            lock_info_pb.set_lock_type(kvrpcpb::Op::SharedLock);

            let mut holder = self.make_lock_info_pb(key, start_ts);
            holder.set_lock_type(kvrpcpb::Op::PessimisticLock);
            lock_info_pb.set_shared_lock_infos(vec![holder].into());

            let token = self.inner.lock_mgr.allocate_token();
            let (request_tx, request_rx) = channel();
            let request_cb = StorageCallback::PessimisticLock(Box::new(move |res| {
                request_tx.send(res).unwrap()
            }));
            let wait_ctx =
                LockWaitContext::new(Key::from_raw(key), self.clone(), token, request_cb, false);
            let cancel_cb = wait_ctx.get_callback_for_cancellation();
            let cancel_lock_info = lock_info_pb.clone();
            let cancel = move || {
                cancel_cb(StorageError::from(TxnError::from(MvccError::from(
                    MvccErrorInner::KeyIsLocked(cancel_lock_info),
                ))))
            };
            let key = Key::from_raw(key);
            let lock_wait_entry = Box::new(LockWaitEntry {
                key: key.clone(),
                lock_hash: key.gen_hash(),
                parameters: PessimisticLockParameters {
                    pb_ctx: Default::default(),
                    primary: key.to_raw().unwrap(),
                    start_ts,
                    lock_ttl: 1000,
                    for_update_ts: start_ts,
                    wait_timeout: Some(WaitTimeout::Default),
                    return_values: false,
                    min_commit_ts: 0.into(),
                    check_existence: false,
                    is_first_lock: false,
                    lock_only_if_exists: false,
                    allow_lock_with_conflict: false,
                    is_shared_lock_upgrade: false,
                },
                should_not_exist: false,
                is_shared_lock: false,
                is_lock_upgrade: true,
                lock_wait_token: token,
                req_states: wait_ctx.get_shared_states().clone(),
                legacy_wake_up_index: None,
                key_cb: Some(SyncWrapper::new(wait_ctx.get_callback_for_blocked_key())),
            });
            assert!(self.push_lock_wait(lock_wait_entry, lock_info_pb));

            TestUpgradeWaitRequestHandle {
                request_rx,
                cancel_cb: Box::new(cancel),
            }
        }

        /// Pop an entry from the queue of the specified key, but do not create
        /// the future for delayed wake up. Used in tests that do not
        /// care about the delayed wake up.
        fn must_pop(
            &self,
            key: &[u8],
            conflicting_start_ts: impl Into<TimeStamp>,
            conflicting_commit_ts: impl Into<TimeStamp>,
        ) -> Box<LockWaitEntry> {
            let (mut entries, future) = self.pop_for_waking_up_impl(
                &Key::from_raw(key),
                conflicting_start_ts.into(),
                conflicting_commit_ts.into(),
                None,
            );
            assert_eq!(entries.len(), 1);
            assert!(future.is_none());
            entries.pop().unwrap()
        }

        fn must_pop_none(
            &self,
            key: &[u8],
            conflicting_start_ts: impl Into<TimeStamp>,
            conflicting_commit_ts: impl Into<TimeStamp>,
        ) {
            let (entries, future) = self.pop_for_waking_up_impl(
                &Key::from_raw(key),
                conflicting_start_ts.into(),
                conflicting_commit_ts.into(),
                Some(1),
            );
            assert!(entries.is_empty());
            assert!(future.is_none());
        }

        fn must_pop_with_delayed_notify(
            &self,
            key: &[u8],
            conflicting_start_ts: impl Into<TimeStamp>,
            conflicting_commit_ts: impl Into<TimeStamp>,
        ) -> (Box<LockWaitEntry>, DelayedNotifyAllFuture) {
            let (mut entries, future) = self.pop_for_waking_up_impl(
                &Key::from_raw(key),
                conflicting_start_ts.into(),
                conflicting_commit_ts.into(),
                Some(50),
            );
            assert_eq!(entries.len(), 1);
            (entries.pop().unwrap(), future.unwrap())
        }

        fn must_pop_with_no_delayed_notify(
            &self,
            key: &[u8],
            conflicting_start_ts: impl Into<TimeStamp>,
            conflicting_commit_ts: impl Into<TimeStamp>,
        ) -> Box<LockWaitEntry> {
            let (mut entries, future) = self.pop_for_waking_up_impl(
                &Key::from_raw(key),
                conflicting_start_ts.into(),
                conflicting_commit_ts.into(),
                Some(50),
            );
            assert_eq!(entries.len(), 1);
            assert!(future.is_none());
            entries.pop().unwrap()
        }

        fn get_delayed_notify_id(&self, key: &[u8]) -> Option<u64> {
            self.inner
                .queue_map
                .get(&Key::from_raw(key))
                .unwrap()
                .delayed_notify_all_state
                .as_ref()
                .map(|(id, ..)| *id)
        }

        fn get_queue_length_of_key(&self, key: &[u8]) -> usize {
            self.inner
                .queue_map
                .get(&Key::from_raw(key))
                .map_or(0, |v| v.queue.len())
        }

        fn pending_upgrader(&self, key: &[u8]) -> Option<PendingUpgrader> {
            self.inner
                .queue_map
                .get(&Key::from_raw(key))
                .and_then(|v| v.pending_upgrader)
        }
    }

    impl LockWaitEntry {
        fn check_key(&self, expected_key: &[u8]) -> &Self {
            assert_eq!(self.key, Key::from_raw(expected_key));
            self
        }

        fn check_shared(&self, expected: bool) -> &Self {
            assert_eq!(self.is_shared_lock, expected);
            self
        }

        fn check_start_ts(&self, expected_start_ts: impl Into<TimeStamp>) -> &Self {
            assert_eq!(self.parameters.start_ts, expected_start_ts.into());
            self
        }
    }

    fn expect_write_conflict(
        err: &StorageError,
        expect_conflict_start_ts: impl Into<TimeStamp>,
        expect_conflict_commit_ts: impl Into<TimeStamp>,
    ) {
        match &*err.0 {
            StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::WriteConflict {
                    conflict_start_ts,
                    conflict_commit_ts,
                    ..
                },
            )))) => {
                assert_eq!(*conflict_start_ts, expect_conflict_start_ts.into());
                assert_eq!(*conflict_commit_ts, expect_conflict_commit_ts.into());
            }
            e => panic!("unexpected error: {:?}", e),
        }
    }

    fn expect_lock_upgrade_conflict(
        err: &StorageError,
        expect_start_ts: impl Into<TimeStamp>,
        expect_owner_start_ts: impl Into<TimeStamp>,
        expect_key: &[u8],
        expect_reason: kvrpcpb::LockUpgradeConflictReason,
    ) {
        match &*err.0 {
            StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::LockUpgradeConflict {
                    start_ts,
                    owner_start_ts,
                    key,
                    reason,
                },
            )))) => {
                assert_eq!(*start_ts, expect_start_ts.into());
                assert_eq!(*owner_start_ts, expect_owner_start_ts.into());
                assert_eq!(key, expect_key);
                assert_eq!(*reason, expect_reason);
            }
            e => panic!("unexpected error: {:?}", e),
        }
    }

    fn expect_shared_lock_lost(
        err: &StorageError,
        expect_start_ts: impl Into<TimeStamp>,
        expect_key: &[u8],
    ) {
        match &*err.0 {
            StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::SharedLockLost { start_ts, key },
            )))) => {
                assert_eq!(*start_ts, expect_start_ts.into());
                assert_eq!(key, expect_key);
            }
            e => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_simple_push_pop() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        assert_eq!(queues.entry_count(), 0);
        assert!(queues.is_empty());

        queues.mock_lock_wait(b"k1", 10, 5, false);
        queues.mock_lock_wait(b"k2", 11, 5, false);
        assert_eq!(queues.entry_count(), 2);
        assert!(!queues.is_empty());

        queues
            .must_pop(b"k1", 5, 6)
            .check_key(b"k1")
            .check_start_ts(10);
        queues.must_pop_none(b"k1", 5, 6);
        queues.must_not_contain_key(b"k1");
        assert_eq!(queues.entry_count(), 1);
        assert!(!queues.is_empty());

        queues
            .must_pop(b"k2", 5, 6)
            .check_key(b"k2")
            .check_start_ts(11);
        queues.must_pop_none(b"k2", 5, 6);
        queues.must_not_contain_key(b"k2");
        assert_eq!(queues.entry_count(), 0);
        assert!(queues.is_empty());
    }

    #[test]
    fn test_popping_priority() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        assert_eq!(queues.entry_count(), 0);

        queues.mock_lock_wait(b"k1", 10, 5, false);
        queues.mock_lock_wait(b"k1", 20, 5, false);
        queues.mock_lock_wait(b"k1", 12, 5, false);
        queues.mock_lock_wait(b"k1", 13, 5, false);
        // Duplication is possible considering network issues and RPC retrying.
        queues.mock_lock_wait(b"k1", 12, 5, false);
        assert_eq!(queues.entry_count(), 5);

        // Ordered by start_ts
        for &expected_start_ts in &[10u64, 12, 12, 13, 20] {
            queues
                .must_pop(b"k1", 5, 6)
                .check_key(b"k1")
                .check_start_ts(expected_start_ts);
        }

        queues.must_not_contain_key(b"k1");
        assert_eq!(queues.entry_count(), 0);
    }

    #[test]
    fn test_removing_by_token() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        assert_eq!(queues.entry_count(), 0);

        queues.mock_lock_wait(b"k1", 10, 5, false);
        let token11 = queues.mock_lock_wait(b"k1", 11, 5, false).token;
        queues.mock_lock_wait(b"k1", 12, 5, false);
        let token13 = queues.mock_lock_wait(b"k1", 13, 5, false).token;
        queues.mock_lock_wait(b"k1", 14, 5, false);
        assert_eq!(queues.get_queue_length_of_key(b"k1"), 5);
        assert_eq!(queues.entry_count(), 5);

        queues
            .remove_by_token(&Key::from_raw(b"k1"), token11)
            .unwrap()
            .check_key(b"k1")
            .check_start_ts(11);
        queues
            .remove_by_token(&Key::from_raw(b"k1"), token13)
            .unwrap()
            .check_key(b"k1")
            .check_start_ts(13);
        assert_eq!(queues.get_queue_length_of_key(b"k1"), 3);
        assert_eq!(queues.entry_count(), 3);

        // Removing not-existing entry takes no effect.
        assert!(
            queues
                .remove_by_token(&Key::from_raw(b"k1"), token11)
                .is_none()
        );
        assert!(
            queues
                .remove_by_token(&Key::from_raw(b"k2"), token11)
                .is_none()
        );
        assert_eq!(queues.get_queue_length_of_key(b"k1"), 3);
        assert_eq!(queues.entry_count(), 3);

        queues.must_pop(b"k1", 5, 6).check_start_ts(10);
        queues.must_pop(b"k1", 5, 6).check_start_ts(12);
        queues.must_pop(b"k1", 5, 6).check_start_ts(14);
        queues.must_not_contain_key(b"k1");
        assert_eq!(queues.entry_count(), 0);
    }

    #[test]
    fn test_dropping_cancelled_entries() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        assert_eq!(queues.entry_count(), 0);

        let h10 = queues.mock_lock_wait(b"k1", 10, 5, false);
        let h11 = queues.mock_lock_wait(b"k1", 11, 5, false);
        queues.mock_lock_wait(b"k1", 12, 5, false);
        let h13 = queues.mock_lock_wait(b"k1", 13, 5, false);
        queues.mock_lock_wait(b"k1", 14, 5, false);

        assert_eq!(queues.get_queue_length_of_key(b"k1"), 5);
        assert_eq!(queues.entry_count(), 5);

        h10.cancel();
        h11.cancel();
        h13.cancel();

        assert_eq!(queues.get_queue_length_of_key(b"k1"), 2);
        assert_eq!(queues.entry_count(), 2);

        for &expected_start_ts in &[12u64, 14] {
            queues
                .must_pop(b"k1", 5, 6)
                .check_start_ts(expected_start_ts);
        }
        queues.must_not_contain_key(b"k1");
        assert_eq!(queues.entry_count(), 0);
    }

    #[tokio::test]
    async fn test_delayed_notify_all() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        assert_eq!(queues.entry_count(), 0);

        queues.mock_lock_wait(b"k1", 8, 5, false);

        let handles1 = vec![
            queues.mock_lock_wait(b"k1", 11, 5, false),
            queues.mock_lock_wait(b"k1", 12, 5, false),
            queues.mock_lock_wait(b"k1", 13, 5, false),
        ];

        // Current queue: [8, 11, 12, 13]
        assert_eq!(queues.entry_count(), 4);

        let (entry, delay_wake_up_future) = queues.must_pop_with_delayed_notify(b"k1", 5, 6);
        entry.check_key(b"k1").check_start_ts(8);

        // Current queue: [11*, 12*, 13*] (Items marked with * means it has
        // legacy_wake_up_index less than that in KeyLockWaitState, so it might
        // be woken up when calling delayed_notify_all).
        assert_eq!(queues.entry_count(), 3);

        let handles2 = vec![
            queues.mock_lock_wait(b"k1", 14, 5, false),
            queues.mock_lock_wait(b"k1", 15, 5, true),
            queues.mock_lock_wait(b"k1", 16, 5, false),
        ];

        // Current queue: [11*, 12*, 13*, 14, 15, 16]
        assert_eq!(queues.entry_count(), 6);

        assert!(
            handles1[0]
                .wait_for_result_timeout(Duration::from_millis(100))
                .is_none()
        );

        // Wakes up transaction 11 to 13, and cancels them.
        assert!(delay_wake_up_future.await.is_none());
        assert!(queues.get_delayed_notify_id(b"k1").is_none());
        handles1
            .into_iter()
            .for_each(|h| expect_write_conflict(&h.wait_for_result().unwrap_err().0, 5, 6));
        // 14 is not woken up.
        assert!(
            handles2[0]
                .wait_for_result_timeout(Duration::from_millis(100))
                .is_none()
        );

        // Current queue: [14, 15, 16]
        assert_eq!(queues.entry_count(), 3);

        queues.mock_lock_wait(b"k1", 9, 5, false);
        // Current queue: [9, 14, 15, 16]
        assert_eq!(queues.entry_count(), 4);

        // 9 will be woken up and delayed wake up should be scheduled. After delaying,
        // 14 to 16 should be all woken up later if they are all not resumable.
        // However since 15 is resumable, it will only wake up 14 and return 15
        // through the result of the `delay_wake_up_future`.
        let (entry, delay_wake_up_future) = queues.must_pop_with_delayed_notify(b"k1", 7, 8);
        entry.check_key(b"k1").check_start_ts(9);

        // Current queue: [14*, 15*, 16*]
        assert_eq!(queues.entry_count(), 3);

        queues.mock_lock_wait(b"k1", 17, 5, false);
        let handle18 = queues.mock_lock_wait(b"k1", 18, 5, false);

        // Current queue: [14*, 15*, 16*, 17, 18]
        assert_eq!(queues.entry_count(), 5);

        // Wakes up 14, and stops at 15 which is resumable. Then, 15 should be returned
        // and the caller should be responsible for waking it up.
        let entry15 = delay_wake_up_future.await.unwrap();
        entry15.check_key(b"k1").check_start_ts(15);

        // Current queue: [16*, 17, 18]
        assert_eq!(queues.entry_count(), 3);

        let mut it = handles2.into_iter();
        // Receive 14.
        expect_write_conflict(&it.next().unwrap().wait_for_result().unwrap_err().0, 7, 8);
        // 15 is not woken up.
        assert!(
            it.next()
                .unwrap()
                .wait_for_result_timeout(Duration::from_millis(100))
                .is_none()
        );
        // Neither did 16.
        let handle16 = it.next().unwrap();
        assert!(
            handle16
                .wait_for_result_timeout(Duration::from_millis(100))
                .is_none()
        );

        queues.must_have_next_entry(b"k1", 16);

        // Call delayed_notify_all when the key does not have
        // `delayed_notify_all_state`. This case may happen when the key is
        // removed and recreated in the map. Nothing would happen.
        assert!(queues.get_delayed_notify_id(b"k1").is_none());
        assert!(
            queues
                .delayed_notify_all(&Key::from_raw(b"k1"), 1)
                .is_none()
        );
        queues.must_have_next_entry(b"k1", 16);
        assert!(
            handle16
                .wait_for_result_timeout(Duration::from_millis(100))
                .is_none()
        );

        // Current queue: [16*, 17, 18]
        assert_eq!(queues.entry_count(), 3);

        let (entry, delayed_wake_up_future) = queues.must_pop_with_delayed_notify(b"k1", 7, 8);
        entry.check_key(b"k1").check_start_ts(16);
        queues.must_have_next_entry(b"k1", 17);
        let notify_id = queues.get_delayed_notify_id(b"k1").unwrap();
        // Call `delayed_notify_all` with a different ID. Nothing happens.
        assert!(
            queues
                .delayed_notify_all(&Key::from_raw(b"k1"), notify_id - 1)
                .is_none()
        );
        queues.must_have_next_entry(b"k1", 17);

        // Current queue: [17*, 18*]
        assert_eq!(queues.entry_count(), 2);

        // Don't need to create new future if there already exists one for the key.
        let entry = queues.must_pop_with_no_delayed_notify(b"k1", 9, 10);
        entry.check_key(b"k1").check_start_ts(17);
        queues.must_have_next_entry(b"k1", 18);

        // Current queue: [18*]
        assert_eq!(queues.entry_count(), 1);

        queues.mock_lock_wait(b"k1", 19, 5, false);
        // Current queue: [18*, 19]
        assert_eq!(queues.entry_count(), 2);
        assert!(delayed_wake_up_future.await.is_none());
        // 18 will be cancelled with ts of the latest wake-up event.
        expect_write_conflict(&handle18.wait_for_result().unwrap_err().0, 9, 10);
        // Current queue: [19]
        assert_eq!(queues.entry_count(), 1);

        // Don't need to create new future if the queue is cleared.
        let entry = queues.must_pop_with_no_delayed_notify(b"k1", 9, 10);
        entry.check_key(b"k1").check_start_ts(19);
        // Current queue: empty
        assert_eq!(queues.entry_count(), 0);
        queues.must_not_contain_key(b"k1");

        // Calls delayed_notify_all on keys that not exists (maybe deleted due to
        // completely waking up). Nothing would happen.
        assert!(
            queues
                .delayed_notify_all(&Key::from_raw(b"k1"), 1)
                .is_none()
        );
        queues.must_not_contain_key(b"k1");
        assert_eq!(queues.entry_count(), 0);
    }

    #[test]
    fn test_pop_shared_group() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        assert_eq!(queues.entry_count(), 0);

        queues.mock_lock_wait_with_shared(b"k1", 10, 5, false, true);
        queues.mock_lock_wait_with_shared(b"k1", 11, 5, false, true);
        queues.mock_lock_wait_with_shared(b"k1", 12, 5, false, true);
        queues.mock_lock_wait_with_shared(b"k1", 20, 5, false, false);
        assert_eq!(queues.entry_count(), 4);

        let (entries, future) =
            queues.pop_for_waking_up_impl(&Key::from_raw(b"k1"), 5.into(), 6.into(), Some(50));
        assert_eq!(entries.len(), 3);
        let mut start_ts_set = vec![];
        for entry in &entries {
            entry.check_shared(true);
            start_ts_set.push(entry.parameters.start_ts.into_inner());
        }
        start_ts_set.sort();
        assert_eq!(start_ts_set, vec![10, 11, 12]);
        assert!(future.is_some());

        let (next_entries, next_future) =
            queues.pop_for_waking_up_impl(&Key::from_raw(b"k1"), 7.into(), 8.into(), Some(50));
        assert_eq!(next_entries.len(), 1);
        next_entries[0].check_shared(false).check_start_ts(20);
        assert!(next_future.is_none());

        queues.must_not_contain_key(b"k1");
        assert_eq!(queues.entry_count(), 0);
    }

    #[bench]
    fn bench_update_lock_wait_empty(b: &mut test::Bencher) {
        let queues = LockWaitQueues::new(MockLockManager::new());
        queues.mock_lock_wait(b"k1", 5, 6, false);

        let mut lock_info = kvrpcpb::LockInfo::default();
        let key = b"t\x00\x00\x00\x00\x00\x00\x00\x01_r\x00\x00\x00\x00\x00\x00\x00\x01";
        lock_info.set_key(key.to_vec());
        lock_info.set_primary_lock(key.to_vec());
        lock_info.set_lock_version(10);
        lock_info.set_lock_for_update_ts(10);
        let lock_info = vec![lock_info];

        b.iter(|| {
            queues.update_lock_wait(lock_info.clone());
        });
    }

    #[bench]
    fn bench_update_lock_wait_queue_len_512(b: &mut test::Bencher) {
        let queues = LockWaitQueues::new(MockLockManager::new());

        let key = b"t\x00\x00\x00\x00\x00\x00\x00\x01_r\x00\x00\x00\x00\x00\x00\x00\x01";

        for i in 0..512 {
            queues.mock_lock_wait(key, 15 + i, 10, true);
        }

        let mut lock_info = kvrpcpb::LockInfo::default();
        lock_info.set_key(key.to_vec());
        lock_info.set_primary_lock(key.to_vec());
        lock_info.set_lock_version(10);
        lock_info.set_lock_for_update_ts(10);
        let lock_info = vec![lock_info];

        b.iter(|| {
            queues.update_lock_wait(lock_info.clone());
        });
    }

    #[test]
    fn test_shared_lock_upgrade_second_upgrader_records_conflict_metric() {
        let _metric_guard = LOCK_UPGRADE_CONFLICT_METRIC_TEST_MUTEX
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let count_before = lock_upgrade_conflict_count("second_upgrader");
        let queues = LockWaitQueues::new(MockLockManager::new());

        let first = queues.mock_upgrade_wait(b"k-upgrade", 10, 5);
        let second = queues.mock_upgrade_wait(b"k-upgrade", 20, 5);

        assert_eq!(queues.get_queue_length_of_key(b"k-upgrade"), 1);
        let pending = queues.pending_upgrader(b"k-upgrade").unwrap();
        assert_eq!(pending.start_ts, 10.into());
        assert_eq!(pending.token, first.token);

        let err = second
            .wait_for_result_timeout(Duration::from_secs(1))
            .expect("second upgrader should not be queued")
            .unwrap_err();
        expect_lock_upgrade_conflict(
            &err.0,
            20,
            10,
            b"k-upgrade",
            kvrpcpb::LockUpgradeConflictReason::SecondUpgrader,
        );
        assert_eq!(
            lock_upgrade_conflict_count("second_upgrader"),
            count_before + 1.0
        );
        assert!(
            first
                .wait_for_result_timeout(Duration::from_millis(100))
                .is_none()
        );
    }

    #[test]
    fn test_shared_lock_upgrade_duplicate_in_flight_records_conflict_metric() {
        let _metric_guard = LOCK_UPGRADE_CONFLICT_METRIC_TEST_MUTEX
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let count_before = lock_upgrade_conflict_count("duplicate_in_flight");
        let queues = LockWaitQueues::new(MockLockManager::new());

        let first = queues.mock_upgrade_wait(b"k-duplicate", 10, 5);
        let duplicate = queues.mock_upgrade_wait(b"k-duplicate", 10, 5);

        assert_eq!(queues.get_queue_length_of_key(b"k-duplicate"), 1);
        let pending = queues.pending_upgrader(b"k-duplicate").unwrap();
        assert_eq!(pending.start_ts, 10.into());
        assert_eq!(pending.token, first.token);

        let err = duplicate
            .wait_for_result_timeout(Duration::from_secs(1))
            .expect("duplicate upgrader should not be queued")
            .unwrap_err();
        expect_lock_upgrade_conflict(
            &err.0,
            10,
            10,
            b"k-duplicate",
            kvrpcpb::LockUpgradeConflictReason::DuplicateInFlight,
        );
        assert_eq!(
            lock_upgrade_conflict_count("duplicate_in_flight"),
            count_before + 1.0
        );
        assert!(
            first
                .wait_for_result_timeout(Duration::from_millis(100))
                .is_none()
        );
    }

    #[test]
    fn test_delayed_notify_clears_pending_upgrader() {
        let queues = LockWaitQueues::new(MockLockManager::new());

        queues.mock_lock_wait(b"k-delayed-upgrader", 10, 5, false);
        let upgrader = queues.mock_upgrade_wait(b"k-delayed-upgrader", 20, 5);

        let (first, _delayed_notify) =
            queues.must_pop_with_delayed_notify(b"k-delayed-upgrader", 5, 6);
        first.check_start_ts(10);

        // This waiter is newer than the delayed batch, so it keeps the key state alive.
        queues.mock_lock_wait(b"k-delayed-upgrader", 30, 5, false);

        let notify_id = queues.get_delayed_notify_id(b"k-delayed-upgrader").unwrap();
        assert!(
            queues
                .delayed_notify_all(&Key::from_raw(b"k-delayed-upgrader"), notify_id)
                .is_none()
        );
        expect_write_conflict(&upgrader.wait_for_result().unwrap_err().0, 5, 6);

        assert!(queues.pending_upgrader(b"k-delayed-upgrader").is_none());

        let fresh_upgrader = queues.mock_upgrade_wait(b"k-delayed-upgrader", 20, 5);
        assert_eq!(queues.get_queue_length_of_key(b"k-delayed-upgrader"), 2);
        let pending = queues.pending_upgrader(b"k-delayed-upgrader").unwrap();
        assert_eq!(pending.start_ts, 20.into());
        assert_eq!(pending.token, fresh_upgrader.token);
        assert!(
            fresh_upgrader
                .wait_for_result_timeout(Duration::from_millis(100))
                .is_none()
        );
    }

    #[test]
    fn test_upgrade_hint_retries_matching_pending_upgrader() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        let upgrader = queues.mock_upgrade_wait(b"k-matching-hint", 10, 5);

        queues.consume_shared_lock_upgrade_hints(vec![upgrade_hint(b"k-matching-hint", 10)]);

        let err = upgrader.wait_for_result().unwrap_err();
        expect_write_conflict(&err.0, 0, 0);
        assert!(queues.pending_upgrader(b"k-matching-hint").is_none());
        assert_eq!(queues.entry_count(), 0);
    }

    #[test]
    fn test_removed_upgrader_releases_slot_with_two_holders_left() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        let key = b"three-holders";
        let old = queues.mock_upgrade_wait(key, 10, 20);
        let mut info = kvrpcpb::LockInfo::default();
        info.set_key(key.to_vec());
        info.set_lock_type(kvrpcpb::Op::SharedLock);
        for ts in [20, 30] {
            let mut member = kvrpcpb::LockInfo::default();
            member.set_lock_version(ts);
            info.mut_shared_lock_infos().push(member);
        }
        queues.update_lock_wait(vec![info]);
        expect_shared_lock_lost(&old.wait_for_result().unwrap_err().0, 10, key);
        assert!(!queues.has_pending_upgrader(&Key::from_raw(key)));
        let next = queues.mock_upgrade_wait(key, 20, 30);
        assert_eq!(queues.pending_upgrader(key).unwrap().token, next.token);
    }

    #[test]
    fn test_upgrade_hint_aborts_mismatching_pending_upgrader() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        let upgrader = queues.mock_upgrade_wait(b"k-mismatching-hint", 10, 5);

        queues.consume_shared_lock_upgrade_hints(vec![upgrade_hint(b"k-mismatching-hint", 20)]);

        let err = upgrader.wait_for_result().unwrap_err();
        expect_shared_lock_lost(&err.0, 10, b"k-mismatching-hint");
        assert!(queues.pending_upgrader(b"k-mismatching-hint").is_none());
        assert_eq!(queues.entry_count(), 0);
    }

    #[test]
    fn test_upgrade_hints_are_idempotent_and_preserve_other_waiters() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        let first = queues.mock_lock_wait(b"k-idempotent-hint", 5, 1, false);
        let upgrader = queues.mock_upgrade_wait(b"k-idempotent-hint", 10, 1);
        let last = queues.mock_lock_wait(b"k-idempotent-hint", 20, 1, false);

        queues.consume_shared_lock_upgrade_hints(vec![
            upgrade_hint(b"k-idempotent-hint", 10),
            upgrade_hint(b"k-idempotent-hint", 10),
        ]);

        let err = upgrader.wait_for_result().unwrap_err();
        expect_write_conflict(&err.0, 0, 0);
        assert_eq!(queues.entry_count(), 2);
        assert_eq!(
            queues
                .must_pop(b"k-idempotent-hint", 30, 40)
                .lock_wait_token,
            first.token
        );
        assert_eq!(
            queues
                .must_pop(b"k-idempotent-hint", 30, 40)
                .lock_wait_token,
            last.token
        );
    }

    #[test]
    fn test_upgrade_hint_races_with_cancellation_by_exact_token() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        let TestUpgradeWaitRequestHandle {
            request_rx,
            cancel_cb,
        } = queues.mock_upgrade_wait_with_request_callback(b"k-cancel-hint", 10, 5);

        cancel_cb();
        queues.consume_shared_lock_upgrade_hints(vec![upgrade_hint(b"k-cancel-hint", 10)]);

        let err = request_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("cancellation should finish the request")
            .unwrap_err();
        assert!(matches!(
            &*err.0,
            StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::KeyIsLocked(_),
            ))))
        ));
        assert!(queues.pending_upgrader(b"k-cancel-hint").is_none());
        assert_eq!(queues.entry_count(), 0);
        assert!(queues.is_empty());

        let queues = LockWaitQueues::new(MockLockManager::new());
        let TestUpgradeWaitRequestHandle {
            request_rx,
            cancel_cb,
        } = queues.mock_upgrade_wait_with_request_callback(b"k-cancel-hint", 10, 5);

        queues.consume_shared_lock_upgrade_hints(vec![upgrade_hint(b"k-cancel-hint", 10)]);
        cancel_cb();

        let err = request_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("matching upgrade hint should finish the request")
            .unwrap_err();
        expect_write_conflict(&err, 0, 0);
        assert!(matches!(
            &*err.0,
            StorageErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
                box MvccErrorInner::WriteConflict {
                    reason: kvrpcpb::WriteConflictReason::PessimisticRetry,
                    ..
                },
            ))))
        ));
        assert!(queues.pending_upgrader(b"k-cancel-hint").is_none());
        assert_eq!(queues.entry_count(), 0);
        assert!(queues.is_empty());
    }

    #[test]
    fn test_upgrade_hint_callback_runs_without_queue_guard() {
        let queues = LockWaitQueues::new(MockLockManager::new());
        let key = b"k-callback-hint";
        let ordinary_waiter = queues.mock_lock_wait(key, 20, 5, false);
        let mut current_lock = queues.make_lock_info_pb(key, 5);
        current_lock.set_lock_type(kvrpcpb::Op::SharedLock);
        let (mut entry, _unused_handle) =
            queues.make_mock_lock_wait_entry(key, 10, current_lock.clone());
        entry.is_lock_upgrade = true;

        let callback_queues = queues.clone();
        let (callback_tx, callback_rx) = channel();
        entry.key_cb = Some(SyncWrapper::new(Box::new(move |result, _| {
            let queue_len = callback_queues.get_queue_length_of_key(key);
            callback_tx.send((queue_len, result)).unwrap();
        })));
        assert!(queues.push_lock_wait(entry, current_lock));

        let consumer_queues = queues.clone();
        let consumer = thread::spawn(move || {
            consumer_queues.consume_shared_lock_upgrade_hints(vec![upgrade_hint(key, 10)]);
        });
        let (queue_len, result) = callback_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("callback should re-enter the same-key queue without deadlocking");
        consumer.join().unwrap();

        assert_eq!(queue_len, 1);
        expect_write_conflict(&result.unwrap_err().0, 0, 0);
        assert_eq!(
            queues.must_pop(key, 30, 40).lock_wait_token,
            ordinary_waiter.token
        );
    }
}

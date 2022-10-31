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
    result::Result,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use dashmap;
use futures_util::compat::Future01CompatExt;
use keyed_priority_queue::KeyedPriorityQueue;
use kvproto::kvrpcpb;
use smallvec::SmallVec;
use sync_wrapper::SyncWrapper;
use tikv_util::{time::InstantExt, timer::GLOBAL_TIMER_HANDLE};
use txn_types::{Key, TimeStamp};

use crate::storage::{
    errors::SharedError,
    lock_manager::{LockManager, LockWaitToken},
    metrics::*,
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
    txn::Error as TxnError,
    types::{PessimisticLockParameters, PessimisticLockRes},
    Error as StorageError,
};

pub type CallbackWithSharedError<T> = Box<dyn FnOnce(Result<T, SharedError>) + Send + 'static>;
pub type PessimisticLockKeyCallback = CallbackWithSharedError<PessimisticLockRes>;

/// Represents an `AcquirePessimisticLock` request that's waiting for a lock,
/// and contains the request's parameters.
pub struct LockWaitEntry {
    pub key: Key,
    pub lock_hash: u64,
    pub parameters: PessimisticLockParameters,
    pub lock_wait_token: LockWaitToken,
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
        }
    }
}

pub type DelayedNotifyAllFuture = Pin<Box<dyn Future<Output = Option<Box<LockWaitEntry>>> + Send>>;

pub struct LockWaitQueueInner<L: LockManager> {
    queue_map: dashmap::DashMap<Key, KeyLockWaitState>,
    id_allocated: AtomicU64,
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
                lock_mgr,
            }),
        }
    }

    /// Enqueues a lock wait entry. The key is indicated by the `key` field of
    /// the `lock_wait_entry`. The caller also needs to provide the
    /// information of the current-holding lock.
    pub fn push_lock_wait(
        &self,
        mut lock_wait_entry: Box<LockWaitEntry>,
        current_lock: kvrpcpb::LockInfo,
    ) {
        let mut new_key = false;
        let mut key_state = self
            .inner
            .queue_map
            .entry(lock_wait_entry.key.clone())
            .or_insert_with(|| {
                new_key = true;
                KeyLockWaitState::new()
            });
        key_state.current_lock = current_lock;

        if lock_wait_entry.legacy_wake_up_index.is_none() {
            lock_wait_entry.legacy_wake_up_index = Some(key_state.value().legacy_wake_up_index);
        }
        key_state
            .value_mut()
            .queue
            .push(lock_wait_entry.lock_wait_token, lock_wait_entry);

        let len = key_state.value_mut().queue.len();
        drop(key_state);
        LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.waiters.inc();
        LOCK_WAIT_QUEUE_LENGTH_HISTOGRAM.observe(len as f64);
        if new_key {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.keys.inc()
        }
    }

    /// Dequeues the head of the lock waiting queue of the specified key,
    /// assuming the popped entry will be woken up.
    ///
    /// If it's waking up a legacy request and the queue is not empty, a future
    /// will be returned and the caller will be responsible for executing it.
    /// The future waits until `wake_up_delay_duration` is elapsed since the
    /// most recent waking-up, and then wakes up all lock waiting entries that
    /// exists at the time when the latest waking-up happens. The future
    /// will return a `LockWaitEntry` if a resumable entry is popped out
    /// from the queue while executing, and in this case the caller will be
    /// responsible to wake it up.
    pub fn pop_for_waking_up(
        &self,
        key: &Key,
        conflicting_start_ts: TimeStamp,
        conflicting_commit_ts: TimeStamp,
        wake_up_delay_duration_ms: u64,
    ) -> Option<(Box<LockWaitEntry>, Option<DelayedNotifyAllFuture>)> {
        self.pop_for_waking_up_impl(
            key,
            conflicting_start_ts,
            conflicting_commit_ts,
            Some(wake_up_delay_duration_ms),
        )
    }

    fn pop_for_waking_up_impl(
        &self,
        key: &Key,
        conflicting_start_ts: TimeStamp,
        conflicting_commit_ts: TimeStamp,
        wake_up_delay_duration_ms: Option<u64>,
    ) -> Option<(Box<LockWaitEntry>, Option<DelayedNotifyAllFuture>)> {
        let mut result = None;
        // For statistics.
        let mut removed_waiters = 0;

        // We don't want other threads insert any more entries between finding the
        // queue is empty and removing the queue from the map. Wrap the logic
        // within a call to `remove_if_mut` to avoid releasing lock during the
        // procedure.
        let removed_key = self.inner.queue_map.remove_if_mut(key, |_, v| {
            v.last_conflict_start_ts = conflicting_start_ts;
            v.last_conflict_commit_ts = conflicting_commit_ts;

            if let Some((_, lock_wait_entry)) = v.queue.pop() {
                removed_waiters += 1;

                if !lock_wait_entry.parameters.allow_lock_with_conflict {
                    // If a pessimistic lock request in legacy mode is woken up, increase the
                    // counter.
                    v.legacy_wake_up_index += 1;
                    let notify_all_future = match wake_up_delay_duration_ms {
                        Some(delay) if !v.queue.is_empty() => {
                            self.handle_delayed_wake_up(v, key, delay)
                        }
                        _ => None,
                    };
                    result = Some((lock_wait_entry, notify_all_future));
                } else {
                    result = Some((lock_wait_entry, None));
                }
            }

            // Remove the queue if it's emptied.
            v.queue.is_empty()
        });

        if removed_waiters != 0 {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC
                .waiters
                .sub(removed_waiters);
        }
        if removed_key.is_some() {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.keys.dec();
        }

        result
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

        self.delayed_notify_all(&key, notify_id)
    }

    fn delayed_notify_all(&self, key: &Key, notify_id: u64) -> Option<Box<LockWaitEntry>> {
        let mut popped_lock_wait_entries = SmallVec::<[_; 4]>::new();

        let mut woken_up_resumable_entry = None;
        let mut conflicting_start_ts = TimeStamp::zero();
        let mut conflicting_commit_ts = TimeStamp::zero();

        let mut removed_waiters = 0;

        // We don't want other threads insert any more entries between finding the
        // queue is empty and removing the queue from the map. Wrap the logic
        // within a call to `remove_if_mut` to avoid releasing lock during the
        // procedure.
        let removed_key = self.inner.queue_map.remove_if_mut(key, |_, v| {
            // The KeyLockWaitState of the key might have been removed from the map and then
            // recreated. Skip.
            if v.delayed_notify_all_state
                .as_ref()
                .map_or(true, |(id, ..)| *id != notify_id)
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
                    .map_or(false, |idx| idx >= legacy_wake_up_index)
                {
                    // This entry is added after the legacy-wakeup that issued the current
                    // delayed_notify_all operation. Keep it and other remaining items in the queue.
                    break;
                }
                let (_, lock_wait_entry) = v.queue.pop().unwrap();
                removed_waiters += 1;
                if lock_wait_entry.parameters.allow_lock_with_conflict {
                    woken_up_resumable_entry = Some(lock_wait_entry);
                    break;
                }
                popped_lock_wait_entries.push(lock_wait_entry);
            }

            // If the queue is empty, remove it from the map.
            v.queue.is_empty()
        });

        if removed_waiters != 0 {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC
                .waiters
                .sub(removed_waiters);
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
            cb(Err(e.into()));
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
                LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.waiters.dec();
                result = Some(res);
            }
            v.queue.is_empty()
        });

        if removed_key.is_some() {
            LOCK_WAIT_QUEUE_ENTRIES_GAUGE_VEC.keys.dec();
        }

        result
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
        sync::mpsc::{channel, Receiver, RecvTimeoutError},
        time::Duration,
    };

    use super::*;
    use crate::storage::{
        lock_manager::{lock_wait_context::LockWaitContext, MockLockManager, WaitTimeout},
        txn::ErrorInner as TxnErrorInner,
        ErrorInner as StorageErrorInner, StorageCallback,
    };

    struct TestLockWaitEntryHandle {
        token: LockWaitToken,
        wake_up_rx: Receiver<Result<PessimisticLockRes, SharedError>>,
        cancel_cb: Box<dyn FnOnce()>,
    }

    impl TestLockWaitEntryHandle {
        fn wait_for_result_timeout(
            &self,
            timeout: Duration,
        ) -> Option<Result<PessimisticLockRes, SharedError>> {
            match self.wake_up_rx.recv_timeout(timeout) {
                Ok(res) => Some(res),
                Err(RecvTimeoutError::Timeout) => None,
                Err(e) => panic!(
                    "unexpected error when receiving result of a LockWaitEntry: {:?}",
                    e
                ),
            }
        }

        fn wait_for_result(self) -> Result<PessimisticLockRes, SharedError> {
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
                allow_lock_with_conflict: false,
            };

            let key = Key::from_raw(key);
            let lock_hash = key.gen_hash();
            let (tx, rx) = channel();
            let lock_wait_entry = Box::new(LockWaitEntry {
                key,
                lock_hash,
                parameters,
                lock_wait_token: token,
                legacy_wake_up_index: None,
                key_cb: Some(SyncWrapper::new(Box::new(move |res| tx.send(res).unwrap()))),
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
            let lock_info_pb = self.make_lock_info_pb(key, encountered_lock_ts);
            let (mut entry, handle) =
                self.make_mock_lock_wait_entry(key, start_ts, lock_info_pb.clone());
            entry.parameters.allow_lock_with_conflict = resumable;
            self.push_lock_wait(entry, lock_info_pb);
            handle
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
            let (entry, f) = self
                .pop_for_waking_up_impl(
                    &Key::from_raw(key),
                    conflicting_start_ts.into(),
                    conflicting_commit_ts.into(),
                    None,
                )
                .unwrap();
            assert!(f.is_none());
            entry
        }

        fn must_pop_none(
            &self,
            key: &[u8],
            conflicting_start_ts: impl Into<TimeStamp>,
            conflicting_commit_ts: impl Into<TimeStamp>,
        ) {
            let res = self.pop_for_waking_up_impl(
                &Key::from_raw(key),
                conflicting_start_ts.into(),
                conflicting_commit_ts.into(),
                Some(1),
            );
            assert!(res.is_none());
        }

        fn must_pop_with_delayed_notify(
            &self,
            key: &[u8],
            conflicting_start_ts: impl Into<TimeStamp>,
            conflicting_commit_ts: impl Into<TimeStamp>,
        ) -> (Box<LockWaitEntry>, DelayedNotifyAllFuture) {
            let (res, f) = self
                .pop_for_waking_up_impl(
                    &Key::from_raw(key),
                    conflicting_start_ts.into(),
                    conflicting_commit_ts.into(),
                    Some(50),
                )
                .unwrap();
            (res, f.unwrap())
        }

        fn must_pop_with_no_delayed_notify(
            &self,
            key: &[u8],
            conflicting_start_ts: impl Into<TimeStamp>,
            conflicting_commit_ts: impl Into<TimeStamp>,
        ) -> Box<LockWaitEntry> {
            let (res, f) = self
                .pop_for_waking_up_impl(
                    &Key::from_raw(key),
                    conflicting_start_ts.into(),
                    conflicting_commit_ts.into(),
                    Some(50),
                )
                .unwrap();
            assert!(f.is_none());
            res
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
    }

    impl LockWaitEntry {
        fn check_key(&self, expected_key: &[u8]) -> &Self {
            assert_eq!(self.key, Key::from_raw(expected_key));
            self
        }

        fn check_start_ts(&self, expected_start_ts: impl Into<TimeStamp>) -> &Self {
            assert_eq!(self.parameters.start_ts, expected_start_ts.into());
            self
        }
    }

    fn expect_write_conflict(
        err: &StorageErrorInner,
        expect_conflict_start_ts: impl Into<TimeStamp>,
        expect_conflict_commit_ts: impl Into<TimeStamp>,
    ) {
        match err {
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

    #[test]
    fn test_simple_push_pop() {
        let queues = LockWaitQueues::new(MockLockManager::new());

        queues.mock_lock_wait(b"k1", 10, 5, false);
        queues.mock_lock_wait(b"k2", 11, 5, false);

        queues
            .must_pop(b"k1", 5, 6)
            .check_key(b"k1")
            .check_start_ts(10);
        queues.must_pop_none(b"k1", 5, 6);
        queues.must_not_contain_key(b"k1");

        queues
            .must_pop(b"k2", 5, 6)
            .check_key(b"k2")
            .check_start_ts(11);
        queues.must_pop_none(b"k2", 5, 6);
        queues.must_not_contain_key(b"k2");
    }

    #[test]
    fn test_popping_priority() {
        let queues = LockWaitQueues::new(MockLockManager::new());

        queues.mock_lock_wait(b"k1", 10, 5, false);
        queues.mock_lock_wait(b"k1", 20, 5, false);
        queues.mock_lock_wait(b"k1", 12, 5, false);
        queues.mock_lock_wait(b"k1", 13, 5, false);
        // Duplication is possible considering network issues and RPC retrying.
        queues.mock_lock_wait(b"k1", 12, 5, false);

        // Ordered by start_ts
        for &expected_start_ts in &[10u64, 12, 12, 13, 20] {
            queues
                .must_pop(b"k1", 5, 6)
                .check_key(b"k1")
                .check_start_ts(expected_start_ts);
        }

        queues.must_not_contain_key(b"k1");
    }

    #[test]
    fn test_removing_by_token() {
        let queues = LockWaitQueues::new(MockLockManager::new());

        queues.mock_lock_wait(b"k1", 10, 5, false);
        let token11 = queues.mock_lock_wait(b"k1", 11, 5, false).token;
        queues.mock_lock_wait(b"k1", 12, 5, false);
        let token13 = queues.mock_lock_wait(b"k1", 13, 5, false).token;
        queues.mock_lock_wait(b"k1", 14, 5, false);
        assert_eq!(queues.get_queue_length_of_key(b"k1"), 5);

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

        queues.must_pop(b"k1", 5, 6).check_start_ts(10);
        queues.must_pop(b"k1", 5, 6).check_start_ts(12);
        queues.must_pop(b"k1", 5, 6).check_start_ts(14);
    }

    #[test]
    fn test_dropping_cancelled_entries() {
        let queues = LockWaitQueues::new(MockLockManager::new());

        let h10 = queues.mock_lock_wait(b"k1", 10, 5, false);
        let h11 = queues.mock_lock_wait(b"k1", 11, 5, false);
        queues.mock_lock_wait(b"k1", 12, 5, false);
        let h13 = queues.mock_lock_wait(b"k1", 13, 5, false);
        queues.mock_lock_wait(b"k1", 14, 5, false);

        assert_eq!(queues.get_queue_length_of_key(b"k1"), 5);

        h10.cancel();
        h11.cancel();
        h13.cancel();

        assert_eq!(queues.get_queue_length_of_key(b"k1"), 2);

        for &expected_start_ts in &[12u64, 14] {
            queues
                .must_pop(b"k1", 5, 6)
                .check_start_ts(expected_start_ts);
        }
        queues.must_not_contain_key(b"k1");
    }

    #[tokio::test]
    async fn test_delayed_notify_all() {
        let queues = LockWaitQueues::new(MockLockManager::new());

        queues.mock_lock_wait(b"k1", 8, 5, false);

        let handles1 = vec![
            queues.mock_lock_wait(b"k1", 11, 5, false),
            queues.mock_lock_wait(b"k1", 12, 5, false),
            queues.mock_lock_wait(b"k1", 13, 5, false),
        ];

        // Current queue: [8, 11, 12, 13]

        let (entry, delay_wake_up_future) = queues.must_pop_with_delayed_notify(b"k1", 5, 6);
        entry.check_key(b"k1").check_start_ts(8);

        // Current queue: [11*, 12*, 13*] (Items marked with * means it has
        // legacy_wake_up_index less than that in KeyLockWaitState, so it might
        // be woken up when calling delayed_notify_all).

        let handles2 = vec![
            queues.mock_lock_wait(b"k1", 14, 5, false),
            queues.mock_lock_wait(b"k1", 15, 5, true),
            queues.mock_lock_wait(b"k1", 16, 5, false),
        ];

        // Current queue: [11*, 12*, 13*, 14, 15, 16]

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

        queues.mock_lock_wait(b"k1", 9, 5, false);
        // Current queue: [9, 14, 15, 16]

        // 9 will be woken up and delayed wake up should be scheduled. After delaying,
        // 14 to 16 should be all woken up later if they are all not resumable.
        // However since 15 is resumable, it will only wake up 14 and return 15
        // through the result of the `delay_wake_up_future`.
        let (entry, delay_wake_up_future) = queues.must_pop_with_delayed_notify(b"k1", 7, 8);
        entry.check_key(b"k1").check_start_ts(9);

        // Current queue: [14*, 15*, 16*]

        queues.mock_lock_wait(b"k1", 17, 5, false);
        let handle18 = queues.mock_lock_wait(b"k1", 18, 5, false);

        // Current queue: [14*, 15*, 16*, 17, 18]

        // Wakes up 14, and stops at 15 which is resumable. Then, 15 should be returned
        // and the caller should be responsible for waking it up.
        let entry15 = delay_wake_up_future.await.unwrap();
        entry15.check_key(b"k1").check_start_ts(15);

        // Current queue: [16*, 17, 18]

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

        // Don't need to create new future if there already exists one for the key.
        let entry = queues.must_pop_with_no_delayed_notify(b"k1", 9, 10);
        entry.check_key(b"k1").check_start_ts(17);
        queues.must_have_next_entry(b"k1", 18);

        // Current queue: [18*]

        queues.mock_lock_wait(b"k1", 19, 5, false);
        // Current queue: [18*, 19]
        assert!(delayed_wake_up_future.await.is_none());
        // 18 will be cancelled with ts of the latest wake-up event.
        expect_write_conflict(&handle18.wait_for_result().unwrap_err().0, 9, 10);
        // Current queue: [19]

        // Don't need to create new future if the queue is cleared.
        let entry = queues.must_pop_with_no_delayed_notify(b"k1", 9, 10);
        entry.check_key(b"k1").check_start_ts(19);
        // Current queue: empty
        queues.must_not_contain_key(b"k1");

        // Calls delayed_notify_all on keys that not exists (maybe deleted due to
        // completely waking up). Nothing would happen.
        assert!(
            queues
                .delayed_notify_all(&Key::from_raw(b"k1"), 1)
                .is_none()
        );
        queues.must_not_contain_key(b"k1");
    }
}

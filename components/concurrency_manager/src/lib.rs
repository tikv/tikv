// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! The concurrency manager is responsible for concurrency control of
//! transactions.
//!
//! The concurrency manager contains a lock table in memory. Lock information
//! can be stored in it and reading requests can check if these locks block
//! the read.
//!
//! In order to mutate the lock of a key stored in the lock table, it needs
//! to be locked first using `lock_key` or `lock_keys`.

use fail::fail_point;

mod key_handle;
mod lock_table;

use std::{
    fmt::Display,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use crossbeam::atomic::AtomicCell;
use lazy_static::lazy_static;
use prometheus::{register_int_gauge, IntGauge};
use thiserror::Error;
use tikv_util::{error, time::Instant};
use txn_types::{Key, Lock, TimeStamp};

pub use self::{
    key_handle::{KeyHandle, KeyHandleGuard},
    lock_table::LockTable,
};

lazy_static! {
    static ref MAX_TS_LIMIT_GAUGE: IntGauge = register_int_gauge!(
        "tikv_concurrency_manager_max_ts_limit",
        "Current value of max_ts_limit"
    )
    .unwrap();
    static ref MAX_TS_GAUGE: IntGauge =
        register_int_gauge!("tikv_concurrency_manager_max_ts", "Current value of max_ts").unwrap();
}

const DEFAULT_LIMIT_VALID_DURATION: Duration = Duration::from_secs(60);

// It is suggested that limit_valid_duration = sync_interval *
// LIMIT_VALID_TIME_MULTIPLIER, to balance between
// 1. tolerate temporary issues in updating the limit.
// 2. avoid long-term blocking of max_ts update caused by network partition
//    between TiKV and PD.
pub const LIMIT_VALID_TIME_MULTIPLIER: u64 = 3;

#[derive(Copy, Clone)]
struct MaxTsLimit {
    limit: TimeStamp,
    update_time: Instant,
}

// Pay attention that the async functions of ConcurrencyManager should not hold
// the mutex.
#[derive(Clone)]
pub struct ConcurrencyManager {
    max_ts: Arc<AtomicU64>,
    lock_table: LockTable,

    // max_ts_limit and its update time.
    //
    // max_ts_limit is an assertion: max_ts should not be updated to a value greater than this
    // limit.
    //
    // The update time is defined as start_instant.elapsed().as_millis(), because we want atomic
    // variable + monotonic clock. When the limit is not updated for a long time(exceeding the
    // threshold), we don't check the limit, to avoid blocking the max_ts update caused by
    // temporary issues in fetching TSO, e.g. network partition between this TiKV and PD leader
    max_ts_limit: Arc<AtomicCell<MaxTsLimit>>,
    limit_valid_duration: Duration,
    panic_on_invalid_max_ts: Arc<AtomicBool>,

    time_provider: Arc<dyn TimeProvider>,
}

impl ConcurrencyManager {
    pub fn new(latest_ts: TimeStamp) -> Self {
        Self::new_with_config(latest_ts, DEFAULT_LIMIT_VALID_DURATION, true)
    }

    pub fn new_with_config(
        latest_ts: TimeStamp,
        limit_valid_duration: Duration,
        panic_on_invalid_max_ts: bool,
    ) -> Self {
        let initial_limit = MaxTsLimit {
            limit: TimeStamp::new(0),
            update_time: Instant::now(),
        };

        ConcurrencyManager {
            max_ts: Arc::new(AtomicU64::new(latest_ts.into_inner())),
            max_ts_limit: Arc::new(AtomicCell::new(initial_limit)),
            lock_table: LockTable::default(),
            panic_on_invalid_max_ts: Arc::new(AtomicBool::new(panic_on_invalid_max_ts)),
            limit_valid_duration,
            time_provider: Arc::new(CoarseInstantTimeProvider),
        }
    }

    #[cfg(test)]
    fn new_with_time_provider(
        latest_ts: TimeStamp,
        limit_valid_duration: Duration,
        panic_on_invalid_max_ts: bool,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let initial_limit = MaxTsLimit {
            limit: TimeStamp::new(0),
            update_time: time_provider.now(),
        };
        ConcurrencyManager {
            max_ts: Arc::new(AtomicU64::new(latest_ts.into_inner())),
            max_ts_limit: Arc::new(AtomicCell::new(initial_limit)),
            lock_table: LockTable::default(),
            panic_on_invalid_max_ts: Arc::new(AtomicBool::new(panic_on_invalid_max_ts)),
            limit_valid_duration,
            time_provider,
        }
    }

    pub fn max_ts(&self) -> TimeStamp {
        TimeStamp::new(self.max_ts.load(Ordering::SeqCst))
    }

    /// Updates max_ts with the given new_ts. It has no effect if
    /// max_ts >= new_ts or new_ts is TimeStamp::max().
    ///
    /// To avoid invalid ts breaking the invariants, the new_ts should be
    /// less than or equal to the max_ts_limit.
    ///
    /// # Returns
    /// - Ok(()): If the update is successful or has no effect
    /// - Err(limit): If new_ts is greater than the max_ts_limit, returns the
    ///   current limit value

    pub fn update_max_ts(
        &self,
        new_ts: TimeStamp,
        source: impl IntoErrorSource,
    ) -> Result<(), InvalidMaxTsUpdate> {
        if new_ts.is_max() {
            return Ok(());
        }
        let limit = self.max_ts_limit.load();

        // check that new_ts is less than or equal to the limit
        if !limit.limit.is_zero() && new_ts > limit.limit {
            // NOTE: `limit` and `last_update` are read non-atomically as a whole, so they
            // can be inconsistent, i.e. they may not be from the same event of
            // setting the limit. The consequence is that we may mistakenly
            // treat an "invalid" limit as a "valid" one. This is acceptable
            // because the limit is just an assertion, and the inconsistency
            // is not harmful.
            let last_update = limit.update_time;
            let now = self.time_provider.now();
            assert!(now >= last_update);
            let duration_to_last_limit_update = now - last_update;

            if duration_to_last_limit_update < self.limit_valid_duration {
                // limit is valid
                let source = source.into_error_source();
                self.report_error(new_ts, limit.limit, source, true)?;
            } else {
                // limit is stale
                // use an approximate limit to avoid false alerts caused by failed limit updates

                let approximate_limit = TimeStamp::compose(
                    limit.limit.physical() + duration_to_last_limit_update.as_millis() as u64,
                    limit.limit.logical(),
                );

                if new_ts > approximate_limit {
                    let source = source.into_error_source();
                    self.report_error(new_ts, approximate_limit, source, false)?;
                }
            }
        }

        MAX_TS_GAUGE.set(
            self.max_ts
                .fetch_max(new_ts.into_inner(), Ordering::SeqCst)
                .max(new_ts.into_inner()) as i64,
        );
        Ok(())
    }

    fn report_error(
        &self,
        new_ts: TimeStamp,
        limit: TimeStamp,
        source: impl slog::Value + Display,
        can_panic: bool,
    ) -> Result<(), InvalidMaxTsUpdate> {
        error!("invalid max_ts update";
            "attempted_ts" => new_ts,
            "max_allowed" => limit.into_inner(),
            "source" => &source,
        );
        if can_panic && self.panic_on_invalid_max_ts.load(Ordering::SeqCst) {
            panic!(
                "invalid max_ts update: {} exceeds the limit {}, source={}",
                new_ts,
                limit.into_inner(),
                source
            );
        }
        Err(InvalidMaxTsUpdate {
            attempted_ts: new_ts,
            max_allowed: limit,
        })
    }

    /// Set the maximum allowed value for max_ts updates, except for the updates
    /// from PD TSO. The limit must be updated regularly to prevent the
    /// blocking of max_ts. It prevents max_ts from being updated to an
    /// unreasonable value, which is usually caused by bugs or unsafe
    /// usages.
    ///
    /// # Note
    /// If the new limit is smaller than the current limit, this operation will
    /// have no effect and return silently.
    pub fn set_max_ts_limit(&self, limit: TimeStamp) {
        if limit.is_max() {
            error!("max_ts_limit cannot be set to u64::max");
            return;
        }

        let current = self.max_ts_limit.load();
        if limit.into_inner() > current.limit.into_inner() {
            let new_state = MaxTsLimit {
                limit,
                update_time: self.time_provider.now(),
            };
            self.max_ts_limit.store(new_state);
            MAX_TS_LIMIT_GAUGE.set(limit.into_inner() as i64);
        }
    }

    /// Acquires a mutex of the key and returns an RAII guard. When the guard
    /// goes out of scope, the mutex will be unlocked.
    ///
    /// The guard can be used to store Lock in the table. The stored lock
    /// is visible to `read_key_check` and `read_range_check`.
    pub async fn lock_key(&self, key: &Key) -> KeyHandleGuard {
        self.lock_table.lock_key(key).await
    }

    /// Acquires mutexes of the keys and returns the RAII guards. The order of
    /// the guards is the same with the given keys.
    ///
    /// The guards can be used to store Lock in the table. The stored lock
    /// is visible to `read_key_check` and `read_range_check`.
    pub async fn lock_keys(&self, keys: impl Iterator<Item = &Key>) -> Vec<KeyHandleGuard> {
        let mut keys_with_index: Vec<_> = keys.enumerate().collect();
        // To prevent deadlock, we sort the keys and lock them one by one.
        keys_with_index.sort_by_key(|(_, key)| *key);
        let mut result: Vec<MaybeUninit<KeyHandleGuard>> = Vec::new();
        result.resize_with(keys_with_index.len(), MaybeUninit::uninit);
        for (index, key) in keys_with_index {
            result[index] = MaybeUninit::new(self.lock_table.lock_key(key).await);
        }
        unsafe { tikv_util::memory::vec_transmute(result) }
    }

    /// Checks if there is a memory lock of the key which blocks the read.
    /// The given `check_fn` should return false iff the lock passed in
    /// blocks the read.
    pub fn read_key_check<E>(
        &self,
        key: &Key,
        check_fn: impl FnOnce(&Lock) -> Result<(), E>,
    ) -> Result<(), E> {
        let res = self.lock_table.check_key(key, check_fn);
        fail_point!("cm_after_read_key_check");
        res
    }

    /// Checks if there is a memory lock in the range which blocks the read.
    /// The given `check_fn` should return false iff the lock passed in
    /// blocks the read.
    pub fn read_range_check<E>(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        check_fn: impl FnMut(&Key, &Lock) -> Result<(), E>,
    ) -> Result<(), E> {
        let res = self.lock_table.check_range(start_key, end_key, check_fn);
        fail_point!("cm_after_read_range_check");
        res
    }

    /// Find the minimum start_ts among all locks in memory.
    pub fn global_min_lock_ts(&self) -> Option<TimeStamp> {
        let mut min_lock_ts = None;
        // TODO: The iteration looks not so efficient. It's better to be optimized.
        self.lock_table.for_each(|handle| {
            if let Some(curr_ts) = handle.with_lock(|lock| lock.as_ref().map(|l| l.ts)) {
                if min_lock_ts.map(|ts| ts > curr_ts).unwrap_or(true) {
                    min_lock_ts = Some(curr_ts);
                }
            }
        });
        min_lock_ts
    }

    pub fn global_min_lock(&self) -> Option<(TimeStamp, Key)> {
        let mut min_lock: Option<(TimeStamp, Key)> = None;
        // TODO: The iteration looks not so efficient. It's better to be optimized.
        self.lock_table.for_each_kv(|key, handle| {
            if let Some(curr_ts) = handle.with_lock(|lock| lock.as_ref().map(|l| l.ts)) {
                if min_lock
                    .as_ref()
                    .map(|(ts, _)| ts > &curr_ts)
                    .unwrap_or(true)
                {
                    min_lock = Some((curr_ts, key.clone()));
                }
            }
        });
        min_lock
    }

    pub fn set_panic_on_invalid_max_ts(&self, panic: bool) {
        self.panic_on_invalid_max_ts.store(panic, Ordering::SeqCst);
    }
}

#[derive(Debug, Error, Clone)]
#[error("invalid max_ts update: {attempted_ts} exceeds the limit {max_allowed}")]
pub struct InvalidMaxTsUpdate {
    pub attempted_ts: TimeStamp,
    pub max_allowed: TimeStamp,
}

pub trait ValueDisplay: slog::Value + Display {}
impl ValueDisplay for String {}
impl ValueDisplay for &str {}

mod sealed {
    pub trait Sealed {}
}

pub trait IntoErrorSource: sealed::Sealed {
    type Output: ValueDisplay;
    fn into_error_source(self) -> Self::Output;
}

// &str impl
impl<'a> sealed::Sealed for &'a str {}
impl<'a> IntoErrorSource for &'a str {
    type Output = &'a str;
    fn into_error_source(self) -> Self::Output {
        self
    }
}

// String impl
impl sealed::Sealed for String {}
impl IntoErrorSource for String {
    type Output = String;
    fn into_error_source(self) -> Self::Output {
        self
    }
}

// Closure impl
impl<F, T> sealed::Sealed for F
where
    F: FnOnce() -> T,
    T: ValueDisplay,
{
}
impl<F, T> IntoErrorSource for F
where
    F: FnOnce() -> T,
    T: ValueDisplay,
{
    type Output = T;
    fn into_error_source(self) -> T {
        self()
    }
}

/// Trait to abstract time-related functionality, for a monotonic clock
pub trait TimeProvider: Send + Sync {
    /// Returns the current instant.
    fn now(&self) -> Instant;

    /// Returns the duration elapsed since the provided instant.
    fn elapsed(&self, since: Instant) -> Duration;
}

pub struct CoarseInstantTimeProvider;

impl TimeProvider for CoarseInstantTimeProvider {
    fn now(&self) -> Instant {
        Instant::now_coarse()
    }

    fn elapsed(&self, since: Instant) -> Duration {
        self.now().saturating_duration_since(since)
    }
}

#[derive(Clone)]
pub struct MockTimeProvider {
    current_time: Arc<Mutex<Instant>>,
}

impl MockTimeProvider {
    /// Creates a new MockTimeProvider initialized with the given instant.
    pub fn new(start_time: Instant) -> Self {
        MockTimeProvider {
            current_time: Arc::new(Mutex::new(start_time)),
        }
    }

    /// Advances the current time by the specified duration.
    pub fn advance(&self, duration: Duration) {
        let mut time = self.current_time.lock().unwrap();
        // Note: Instant doesn't support addition, so we mock behavior.
        // This simplistic approach assumes no overflow.
        *time += duration;
    }

    /// Sets the current time to the specified instant.
    pub fn set_time(&self, new_time: Instant) {
        let mut time = self.current_time.lock().unwrap();
        *time = new_time;
    }
}

impl TimeProvider for MockTimeProvider {
    fn now(&self) -> Instant {
        let time = self.current_time.lock().unwrap();
        *time
    }

    fn elapsed(&self, since: Instant) -> Duration {
        let now = self.now();
        now.saturating_duration_since(since)
    }
}

#[cfg(test)]
mod tests {
    use txn_types::LockType;

    use super::*;

    #[tokio::test]
    async fn test_lock_keys_order() {
        let concurrency_manager = ConcurrencyManager::new(1.into());
        let keys: Vec<_> = [b"c", b"a", b"b"]
            .iter()
            .copied()
            .map(|k| Key::from_raw(k))
            .collect();
        let guards = concurrency_manager.lock_keys(keys.iter()).await;
        for (key, guard) in keys.iter().zip(&guards) {
            assert_eq!(key, guard.key());
        }
    }

    #[tokio::test]
    async fn test_update_max_ts() {
        let concurrency_manager = ConcurrencyManager::new(10.into());
        let _ = concurrency_manager.update_max_ts(20.into(), "");
        assert_eq!(concurrency_manager.max_ts(), 20.into());

        let _ = concurrency_manager.update_max_ts(5.into(), "");
        assert_eq!(concurrency_manager.max_ts(), 20.into());

        let _ = concurrency_manager.update_max_ts(TimeStamp::max(), "");
        assert_eq!(concurrency_manager.max_ts(), 20.into());
    }

    fn new_lock(ts: impl Into<TimeStamp>, primary: &[u8], lock_type: LockType) -> Lock {
        let ts = ts.into();
        Lock::new(
            lock_type,
            primary.to_vec(),
            ts,
            0,
            None,
            0.into(),
            1,
            ts,
            false,
        )
    }

    #[tokio::test]
    async fn test_global_min_lock_ts() {
        let concurrency_manager = ConcurrencyManager::new(1.into());

        assert_eq!(concurrency_manager.global_min_lock_ts(), None);
        let guard = concurrency_manager.lock_key(&Key::from_raw(b"a")).await;
        assert_eq!(concurrency_manager.global_min_lock_ts(), None);
        guard.with_lock(|l| *l = Some(new_lock(10, b"a", LockType::Put)));
        assert_eq!(concurrency_manager.global_min_lock_ts(), Some(10.into()));
        drop(guard);
        assert_eq!(concurrency_manager.global_min_lock_ts(), None);

        let ts_seqs = vec![
            vec![20, 30, 40],
            vec![40, 30, 20],
            vec![20, 40, 30],
            vec![30, 20, 40],
        ];
        let keys: Vec<_> = [b"a", b"b", b"c"]
            .iter()
            .copied()
            .map(|k| Key::from_raw(k))
            .collect();

        for ts_seq in ts_seqs {
            let guards = concurrency_manager.lock_keys(keys.iter()).await;
            assert_eq!(concurrency_manager.global_min_lock_ts(), None);
            for (ts, guard) in ts_seq.into_iter().zip(guards.iter()) {
                guard.with_lock(|l| *l = Some(new_lock(ts, b"pk", LockType::Put)));
            }
            assert_eq!(concurrency_manager.global_min_lock_ts(), Some(20.into()));
        }
    }

    #[test]
    fn test_max_ts_limit() {
        let cm = ConcurrencyManager::new_with_config(
            TimeStamp::new(100),
            DEFAULT_LIMIT_VALID_DURATION,
            false,
        );

        // Initially limit should be 0
        cm.update_max_ts(TimeStamp::new(150), "").unwrap();

        // Set initial limit to 200
        cm.set_max_ts_limit(TimeStamp::new(200));

        // Try to lower limit to 150 - should be ignored
        cm.set_max_ts_limit(TimeStamp::new(150));
        cm.update_max_ts(TimeStamp::new(180), "").unwrap(); // Should still work up to 200
        assert!(cm.update_max_ts(TimeStamp::new(250), "").is_err()); // Should fail above 200

        // Increase limit to 300 - should work
        cm.set_max_ts_limit(TimeStamp::new(300));
        cm.update_max_ts(TimeStamp::new(250), "").unwrap();
    }

    #[test]
    fn test_max_ts_limit_edge_cases() {
        let cm = ConcurrencyManager::new(TimeStamp::new(100));

        // Test transition from zero limit
        assert_eq!(cm.max_ts_limit.load().limit, 0.into());
        cm.set_max_ts_limit(TimeStamp::new(1000));
        assert_eq!(cm.max_ts_limit.load().limit, 1000.into());

        // Try to lower from 1000 to 500 - should be ignored
        cm.set_max_ts_limit(TimeStamp::new(500));
        assert_eq!(cm.max_ts_limit.load().limit, 1000.into());

        // Test setting limit to max, should have no effect
        cm.set_max_ts_limit(TimeStamp::max());
        assert_eq!(cm.max_ts_limit.load().limit, 1000.into());
    }

    #[test]
    fn test_max_ts_updates_with_monotonic_limit() {
        let cm = ConcurrencyManager::new_with_config(
            TimeStamp::new(100),
            DEFAULT_LIMIT_VALID_DURATION,
            false,
        );

        // Set limit to 200
        cm.set_max_ts_limit(TimeStamp::new(200));

        // Update max_ts to 150
        cm.update_max_ts(TimeStamp::new(150), "").unwrap();
        assert_eq!(cm.max_ts(), TimeStamp::new(150));

        // Try to lower limit to 180 - should be ignored
        cm.set_max_ts_limit(TimeStamp::new(180));

        // Should still fail for values above 200
        let result = cm.update_max_ts(TimeStamp::new(250), "");
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.attempted_ts, TimeStamp::new(250));
            assert_eq!(e.max_allowed, TimeStamp::new(200));
        }
    }

    #[test]
    fn test_limit_valid_duration_boundary() {
        let start_time = Instant::now();
        let mock_time = MockTimeProvider::new(start_time);
        let time_provider = Arc::new(mock_time.clone());

        let cm = ConcurrencyManager::new_with_time_provider(
            TimeStamp::new(100),
            Duration::from_secs(60),
            false,
            time_provider.clone(),
        );

        cm.set_max_ts_limit(TimeStamp::new(200));

        time_provider.advance(Duration::from_secs(59));
        assert!(cm.update_max_ts(TimeStamp::new(250), "").is_err());

        time_provider.advance(Duration::from_secs(1));
        cm.update_max_ts(TimeStamp::new(250), "").unwrap();
        assert_eq!(cm.max_ts().into_inner(), 250);
    }

    #[test]
    fn test_max_ts_limit_expired_allows_update() {
        let start_time = Instant::now();
        let mock_time = MockTimeProvider::new(start_time);
        let time_provider = Arc::new(mock_time.clone());

        let cm = ConcurrencyManager::new_with_time_provider(
            TimeStamp::new(100),
            Duration::from_secs(60),
            false,
            time_provider.clone(),
        );

        cm.set_max_ts_limit(TimeStamp::new(200));

        mock_time.advance(Duration::from_secs(61));

        // Updating to 250 should be allowed, since the limit should be invalidated
        cm.update_max_ts(TimeStamp::new(250), "test_source".to_string())
            .unwrap();
        assert_eq!(cm.max_ts().into_inner(), 250);
    }

    #[test]
    #[should_panic(expected = "invalid max_ts update")]
    fn test_panic_on_invalid_max_ts_enabled() {
        let cm = ConcurrencyManager::new(TimeStamp::new(100));

        cm.set_max_ts_limit(TimeStamp::new(200));

        // should panic
        cm.update_max_ts(TimeStamp::new(250), "test_source".to_string())
            .unwrap();
    }

    #[test]
    fn test_update_max_ts_without_limit() {
        let cm = ConcurrencyManager::new(TimeStamp::new(100));

        cm.update_max_ts(TimeStamp::new(500), "test_source".to_string())
            .unwrap();
        assert_eq!(cm.max_ts().into_inner(), 500);
    }
}

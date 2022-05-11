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
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use txn_types::{Key, Lock, TimeStamp};

pub use self::{
    key_handle::{KeyHandle, KeyHandleGuard},
    lock_table::LockTable,
};

// Pay attention that the async functions of ConcurrencyManager should not hold
// the mutex.
#[derive(Clone)]
pub struct ConcurrencyManager {
    max_ts: Arc<AtomicU64>,
    lock_table: LockTable,
}

impl ConcurrencyManager {
    pub fn new(latest_ts: TimeStamp) -> Self {
        ConcurrencyManager {
            max_ts: Arc::new(AtomicU64::new(latest_ts.into_inner())),
            lock_table: LockTable::default(),
        }
    }

    pub fn max_ts(&self) -> TimeStamp {
        TimeStamp::new(self.max_ts.load(Ordering::SeqCst))
    }

    /// Updates max_ts with the given new_ts. It has no effect if
    /// max_ts >= new_ts or new_ts is TimeStamp::max().
    pub fn update_max_ts(&self, new_ts: TimeStamp) {
        if new_ts != TimeStamp::max() {
            self.max_ts.fetch_max(new_ts.into_inner(), Ordering::SeqCst);
        }
    }

    /// Acquires a mutex of the key and returns an RAII guard. When the guard goes
    /// out of scope, the mutex will be unlocked.
    ///
    /// The guard can be used to store Lock in the table. The stored lock
    /// is visible to `read_key_check` and `read_range_check`.
    pub async fn lock_key(&self, key: &Key) -> KeyHandleGuard {
        self.lock_table.lock_key(key).await
    }

    /// Acquires mutexes of the keys and returns the RAII guards. The order of the
    /// guards is the same with the given keys.
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
            .map(|k| Key::from_raw(*k))
            .collect();
        let guards = concurrency_manager.lock_keys(keys.iter()).await;
        for (key, guard) in keys.iter().zip(&guards) {
            assert_eq!(key, guard.key());
        }
    }

    #[tokio::test]
    async fn test_update_max_ts() {
        let concurrency_manager = ConcurrencyManager::new(10.into());
        concurrency_manager.update_max_ts(20.into());
        assert_eq!(concurrency_manager.max_ts(), 20.into());

        concurrency_manager.update_max_ts(5.into());
        assert_eq!(concurrency_manager.max_ts(), 20.into());

        concurrency_manager.update_max_ts(TimeStamp::max());
        assert_eq!(concurrency_manager.max_ts(), 20.into());
    }

    fn new_lock(ts: impl Into<TimeStamp>, primary: &[u8], lock_type: LockType) -> Lock {
        let ts = ts.into();
        Lock::new(lock_type, primary.to_vec(), ts, 0, None, 0.into(), 1, ts)
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
        let keys: Vec<_> = vec![b"a", b"b", b"c"]
            .into_iter()
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
}

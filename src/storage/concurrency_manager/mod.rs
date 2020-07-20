// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! The concurrency manager is responsible for concurrency control of
//! transactions.
//!
//! The concurrency manager can be considered as a lock table in memory.
//! Transactional commands can acquire key mutexes from the concurrency manager
//! to ensure serializability. Lock information can be also stored in the
//! manager and reading requests can check if these locks block the read.

mod lock_table;
mod memory_lock;

pub use self::memory_lock::{MemoryLock, TxnMutexGuard};

use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    mem::{self, MaybeUninit},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use txn_types::{Key, TimeStamp};

pub type OrderedLockMap = Mutex<BTreeMap<Key, Arc<MemoryLock>>>;
pub type LockTable = self::lock_table::LockTable<OrderedLockMap>;

#[derive(Clone)]
pub struct ConcurrencyManager {
    max_read_ts: Arc<AtomicU64>,
    lock_table: LockTable,
}

impl ConcurrencyManager {
    pub fn new(latest_ts: TimeStamp) -> Self {
        ConcurrencyManager {
            max_read_ts: Arc::new(AtomicU64::new(latest_ts.into_inner())),
            lock_table: LockTable::default(),
        }
    }

    pub fn max_read_ts(&self) -> TimeStamp {
        TimeStamp::new(self.max_read_ts.load(Ordering::SeqCst))
    }

    /// Acquires a mutex of the key and returns an RAII guard. When the guard goes
    /// out of scope, the mutex will be unlocked.
    ///
    /// The guard can be used to store LockInfo in the lock table. The stored lock
    /// is visible to `read_key_check` and `read_range_check`.
    pub async fn lock_key(&self, key: &Key) -> TxnMutexGuard<'_, OrderedLockMap> {
        self.lock_table.lock_key(key).await
    }

    /// Acquires mutexes of the keys and returns the RAII guards. The order of the
    /// guards is the same with the given keys.
    ///
    /// The guards can be used to store LockInfo in the lock table. The stored lock
    /// is visible to `read_key_check` and `read_range_check`.
    pub async fn lock_keys(
        &self,
        keys: impl IntoIterator<Item = &Key>,
    ) -> Vec<TxnMutexGuard<'_, OrderedLockMap>> {
        let mut keys_with_index: Vec<_> = keys.into_iter().enumerate().collect();
        // To prevent deadlock, we sort the keys and lock them one by one.
        keys_with_index.sort_by_key(|(_, key)| *key);
        let mut result: Vec<MaybeUninit<TxnMutexGuard<'_, OrderedLockMap>>> = Vec::new();
        result.resize_with(keys_with_index.len(), || MaybeUninit::uninit());
        for (index, key) in keys_with_index {
            result[index] = MaybeUninit::new(self.lock_table.lock_key(key).await);
        }
        #[allow(clippy::unsound_collection_transmute)]
        unsafe {
            mem::transmute(result)
        }
    }

    /// Checks if there is a memory lock of the key which blocks the read.
    /// The given `check_fn` should return false iff the lock passed in
    /// blocks the read.
    ///
    /// It will also updates the max_read_ts.
    pub fn read_key_check(
        &self,
        key: &Key,
        ts: TimeStamp,
        check_fn: impl FnOnce(&LockInfo) -> bool,
    ) -> Result<(), LockInfo> {
        self.max_read_ts
            .fetch_max(ts.into_inner(), Ordering::SeqCst);
        self.lock_table.check_key(key, check_fn)
    }

    /// Checks if there is a memory lock in the range which blocks the read.
    /// The given `check_fn` should return false iff the lock passed in
    /// blocks the read.
    ///
    /// It will also updates the max_read_ts.
    pub fn read_range_check(
        &self,
        start_key: &Key,
        end_key: &Key,
        ts: TimeStamp,
        check_fn: impl FnMut(&LockInfo) -> bool,
    ) -> Result<(), LockInfo> {
        self.max_read_ts
            .fetch_max(ts.into_inner(), Ordering::SeqCst);
        self.lock_table.check_range(start_key, end_key, check_fn)
    }
}

#[cfg(test)]
mod tests {
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
    async fn test_update_max_read_ts() {
        let concurrency_manager = ConcurrencyManager::new(10.into());
        let key_k = Key::from_raw(b"k");
        let key_a = Key::from_raw(b"a");
        let key_b = Key::from_raw(b"b");

        assert!(concurrency_manager
            .read_key_check(&key_k, 15.into(), |_| false)
            .is_ok());
        assert_eq!(concurrency_manager.max_read_ts(), 15.into());

        assert!(concurrency_manager
            .read_key_check(&key_k, 5.into(), |_| false)
            .is_ok());
        assert_eq!(concurrency_manager.max_read_ts(), 15.into());

        assert!(concurrency_manager
            .read_range_check(&key_a, &key_b, 10.into(), |_| false)
            .is_ok());
        assert_eq!(concurrency_manager.max_read_ts(), 15.into());

        assert!(concurrency_manager
            .read_range_check(&key_a, &key_b, 20.into(), |_| false)
            .is_ok());
        assert_eq!(concurrency_manager.max_read_ts(), 20.into());
    }
}

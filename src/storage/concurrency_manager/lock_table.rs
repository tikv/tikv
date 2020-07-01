// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::memory_lock::{MemoryLock, MemoryLockRef, TxnMutexGuard};

use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Default)]
pub struct LockTable<M>(Arc<M>);

impl<M: OrderedLockMap> Clone for LockTable<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M: OrderedLockMap> LockTable<M> {
    pub async fn lock_key(&self, key: &[u8]) -> TxnMutexGuard<'_, M> {
        loop {
            if let Some(lock) = self.0.get(key) {
                return lock.mutex_lock().await;
            } else {
                let lock = Arc::new(MemoryLock::new(key.to_vec()));
                let lock_ref = lock.clone().get_ref(&*self.0).unwrap();
                let guard = lock_ref.mutex_lock().await;
                if self.0.insert_if_not_exist(key.to_vec(), lock) {
                    return guard;
                }
            }
            // If the program goes here, the lock is about to be removed from the map.
            // We should retry and get or insert the new lock.
            // Question: should we yield here?
        }
    }

    pub fn check_key(
        &self,
        key: &[u8],
        check_fn: impl FnOnce(&LockInfo) -> bool,
    ) -> Result<(), LockInfo> {
        if let Some(lock_ref) = self.0.get(key) {
            return lock_ref.with_lock_info(|lock_info| {
                if let Some(lock_info) = &*lock_info {
                    if !check_fn(lock_info) {
                        return Err(lock_info.clone());
                    }
                }
                Ok(())
            });
        }
        Ok(())
    }

    pub fn check_range(
        &self,
        _start_key: &[u8],
        _end_key: &[u8],
        _check_fn: impl FnMut(&LockInfo) -> bool,
    ) -> Result<(), LockInfo> {
        // FIXME: check locks in the range
        Ok(())
    }
}

/// A concurrent ordered map which maps raw keys to memory locks.
pub trait OrderedLockMap: Default + Send + Sync + 'static {
    /// Inserts a key lock to the map if the key does not exists in the map.
    /// Returns whether the lock is successfully inserted into the map.
    fn insert_if_not_exist(&self, key: Vec<u8>, lock: Arc<MemoryLock>) -> bool;

    /// Gets the lock of the key.
    fn get<'m>(&'m self, key: &[u8]) -> Option<MemoryLockRef<'m, Self>>;

    /// Removes the key and its lock from the map.
    fn remove(&self, key: &[u8]);
}

impl OrderedLockMap for Mutex<BTreeMap<Vec<u8>, Arc<MemoryLock>>> {
    fn insert_if_not_exist(&self, key: Vec<u8>, lock: Arc<MemoryLock>) -> bool {
        use std::collections::btree_map::Entry;

        match self.lock().entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(lock);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    fn get<'m>(&'m self, key: &[u8]) -> Option<MemoryLockRef<'m, Self>> {
        self.lock()
            .get(key)
            .and_then(|lock| lock.clone().get_ref(self))
    }

    fn remove(&self, key: &[u8]) {
        self.lock().remove(key);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::time::delay_for;

    #[tokio::test]
    async fn test_lock_key() {
        let lock_table = LockTable::<Mutex<BTreeMap<Vec<u8>, Arc<MemoryLock>>>>::default();

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let lock_table = lock_table.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let _guard = lock_table.lock_key(b"k").await;
                // Modify an atomic counter with a mutex guard. The value of the counter
                // should remain unchanged if the mutex works.
                let counter_val = counter.fetch_add(1, Ordering::SeqCst) + 1;
                delay_for(Duration::from_millis(1)).await;
                assert_eq!(counter.load(Ordering::SeqCst), counter_val);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 100);
    }

    #[tokio::test]
    async fn test_check_key() {
        let lock_table = LockTable::<Mutex<BTreeMap<Vec<u8>, Arc<MemoryLock>>>>::default();

        // no lock found
        assert!(lock_table.check_key(b"k", |_| false).is_ok());

        let mut lock_info = LockInfo::default();
        lock_info.set_lock_version(10);
        lock_table.lock_key(b"k").await.with_lock_info(|l| {
            *l = Some(lock_info.clone());
        });

        // lock passes check_fn
        assert!(lock_table
            .check_key(b"k", |l| l.get_lock_version() < 20)
            .is_ok());

        // lock does not pass check_fn
        assert_eq!(
            lock_table.check_key(b"k", |l| l.get_lock_version() < 5),
            Err(lock_info)
        );
    }

    // FIXME: implement check_range and remove #[should_panic]
    #[tokio::test]
    #[should_panic]
    async fn test_check_range() {
        let lock_table = LockTable::<Mutex<BTreeMap<Vec<u8>, Arc<MemoryLock>>>>::default();

        let mut lock_info = LockInfo::default();
        lock_info.set_lock_version(10);
        lock_table.lock_key(b"k").await.with_lock_info(|l| {
            *l = Some(lock_info.clone());
        });

        // no lock found
        assert!(lock_table.check_range(b"m", b"n", |_| false).is_ok());

        // lock passes check_fn
        assert!(lock_table
            .check_range(b"h", b"l", |l| l.get_lock_version() < 20)
            .is_ok());

        // lock does not pass check_fn
        assert_eq!(
            lock_table.check_range(b"h", b"l", |l| l.get_lock_version() < 5),
            Err(lock_info)
        );
    }
}

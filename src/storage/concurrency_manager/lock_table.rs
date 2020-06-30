// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::memory_lock::{MemoryLock, TxnMutexGuard};

use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{Arc, Weak},
};

#[derive(Default)]
pub struct LockTable<M>(Arc<M>);

impl<M: OrderedLockMap> Clone for LockTable<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M: OrderedLockMap> LockTable<M> {
    pub async fn lock_key(&self, key: &[u8]) -> TxnMutexGuard<M> {
        loop {
            if let Some(lock) = self.0.get(key) {
                if let Some(Ok(guard)) = MemoryLock::mutex_lock(lock, self.0.clone()).await {
                    return guard;
                }
            // If the program goes here, the lock is about to be removed from the map.
            // We should retry and get or insert the new lock.
            // Question: should we yield here?
            } else {
                let lock = Arc::new(MemoryLock::new(key.to_vec()));
                if self
                    .0
                    .insert_if_not_exist(key.to_vec(), Arc::downgrade(&lock))
                {
                    // Because we have a strong lock here, the lock cannot be obselete
                    // and we can simply unwrap here.
                    return MemoryLock::mutex_lock(Arc::downgrade(&lock), self.0.clone())
                        .await
                        .unwrap()
                        .unwrap();
                }
            }
        }
    }

    pub fn check_key(
        &self,
        key: &[u8],
        mut check_fn: impl FnMut(&LockInfo) -> bool,
    ) -> Result<(), LockInfo> {
        if let Some(read_guard) = self
            .0
            .get(key)
            .and_then(|lock| MemoryLock::read(lock, self.0.clone()))
        {
            return read_guard.with_lock_info(|lock_info| {
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
        start_key: &[u8],
        end_key: &[u8],
        mut check_fn: impl FnMut(&LockInfo) -> bool,
    ) -> Result<(), LockInfo> {
        if let Some(read_guard) = self
            .0
            .lower_bound(start_key)
            .and_then(|lock| MemoryLock::read(lock, self.0.clone()))
        {
            if read_guard.key() < end_key {
                return read_guard.with_lock_info(|lock_info| {
                    if let Some(lock_info) = &*lock_info {
                        if !check_fn(lock_info) {
                            return Err(lock_info.clone());
                        }
                    }
                    Ok(())
                });
            }
        }
        Ok(())
    }
}

/// A concurrent ordered map which maps raw keys to memory locks.
pub trait OrderedLockMap: Default + Send + Sync + 'static {
    /// Inserts a key lock to the map if the key does not exists in the map.
    /// Returns whether the lock is successfully inserted into the map.
    fn insert_if_not_exist(&self, key: Vec<u8>, lock: Weak<MemoryLock>) -> bool;

    /// Gets the lock of the key.
    fn get(&self, key: &[u8]) -> Option<Weak<MemoryLock>>;

    /// Gets the lock with the smallest key which is greater than or equal to the given key.
    fn lower_bound(&self, key: &[u8]) -> Option<Weak<MemoryLock>>;

    /// Removes the key and its lock from the map.
    fn remove(&self, key: &[u8]);
}

impl OrderedLockMap for Mutex<BTreeMap<Vec<u8>, Weak<MemoryLock>>> {
    fn insert_if_not_exist(&self, key: Vec<u8>, lock: Weak<MemoryLock>) -> bool {
        use std::collections::btree_map::Entry;

        match self.lock().entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(lock);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    fn get(&self, key: &[u8]) -> Option<Weak<MemoryLock>> {
        self.lock().get(key).cloned()
    }

    fn lower_bound(&self, key: &[u8]) -> Option<Weak<MemoryLock>> {
        self.lock()
            .range::<[u8], _>((Bound::Included(key), Bound::Unbounded))
            .next()
            .map(|(_, lock)| lock.clone())
    }

    fn remove(&self, key: &[u8]) {
        self.lock().remove(key);
    }
}

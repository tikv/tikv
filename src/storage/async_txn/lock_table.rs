// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::memory_lock::{MemoryLock, TxnMutexGuard};

use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;
use std::{collections::BTreeMap, ops::Bound, sync::Arc};

#[derive(Default)]
pub struct LockTable<M>(Arc<M>);

impl<M: OrderedMap> Clone for LockTable<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M: OrderedMap> LockTable<M> {
    pub async fn lock_key(&self, key: &[u8]) -> TxnMutexGuard<M> {
        loop {
            if let Some(lock) = self.0.get(key) {
                if let Ok(guard) = lock.mutex_lock().await {
                    return TxnMutexGuard::new(self.0.clone(), guard);
                }
            } else {
                let lock = Arc::new(MemoryLock::default());
                let guard = lock.clone().mutex_lock().await.unwrap();
                if self.0.insert_if_not_exist(key.to_vec(), lock) {
                    return TxnMutexGuard::new(self.0.clone(), guard);
                }
            }
        }
    }

    pub fn check_key(
        &self,
        key: &[u8],
        mut check_fn: impl FnMut(&LockInfo) -> bool,
    ) -> Result<(), LockInfo> {
        if let Some(lock) = self.0.get(key) {
            let lock_info = lock.lock_info.lock();
            if let Some(lock_info) = &*lock_info {
                if !check_fn(lock_info) {
                    return Err(lock_info.clone());
                }
            }
        }
        Ok(())
    }

    pub fn check_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        mut check_fn: impl FnMut(&LockInfo) -> bool,
    ) -> Result<(), LockInfo> {
        if let Some((key, lock)) = self.0.lower_bound(start_key) {
            if key.as_slice() < end_key {
                let lock_info = lock.lock_info.lock();
                if let Some(lock_info) = &*lock_info {
                    if !check_fn(lock_info) {
                        return Err(lock_info.clone());
                    }
                }
            }
        }
        Ok(())
    }
}

pub trait OrderedMap: Default + Send + Sync + 'static {
    /// Inserts a key lock to the map if the key does not exists in the map.
    /// Returns whether the lock is successfully inserted into the map.
    fn insert_if_not_exist(&self, key: Vec<u8>, lock: Arc<MemoryLock>) -> bool;

    /// Gets the lock of the key.
    fn get(&self, key: &[u8]) -> Option<Arc<MemoryLock>>;

    /// Gets the smallest key which is greater than or equal to the given key
    /// and its lock.
    fn lower_bound(&self, key: &[u8]) -> Option<(Vec<u8>, Arc<MemoryLock>)>;

    /// Removes the key and its lock from the map.
    fn remove(&self, key: &[u8]);
}

impl OrderedMap for Mutex<BTreeMap<Vec<u8>, Arc<MemoryLock>>> {
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

    fn get(&self, key: &[u8]) -> Option<Arc<MemoryLock>> {
        self.lock().get(key).cloned()
    }

    fn lower_bound(&self, key: &[u8]) -> Option<(Vec<u8>, Arc<MemoryLock>)> {
        self.lock()
            .range::<[u8], _>((Bound::Included(key), Bound::Unbounded))
            .next()
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    fn remove(&self, key: &[u8]) {
        self.lock().remove(key);
    }
}

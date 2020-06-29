// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::memory_lock::{MemoryLock, TxnMutexGuard};

use std::sync::Arc;

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

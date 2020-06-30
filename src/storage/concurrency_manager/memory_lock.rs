// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::lock_table::OrderedLockMap;

use event_listener::Event;
use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Weak,
};

const UNLOCKED: u64 = 0;
const LOCKED: u64 = 1;
const OBSOLETE: u64 = 2;

pub struct MemoryLock {
    key: Vec<u8>,
    mutex_state: AtomicU64,
    mutex_event: Event,
    lock_info: Mutex<Option<LockInfo>>,
    pessimistic_event: Event,
}

impl MemoryLock {
    pub fn new(key: Vec<u8>) -> MemoryLock {
        MemoryLock {
            key,
            mutex_state: AtomicU64::new(UNLOCKED),
            mutex_event: Event::new(),
            lock_info: Mutex::new(None),
            pessimistic_event: Event::new(),
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn read<M: OrderedLockMap>(lock: Weak<Self>, map: Arc<M>) -> Option<LockReadGuard<M>> {
        lock.upgrade().map(|lock| LockReadGuard { map, lock })
    }

    pub async fn mutex_lock<M: OrderedLockMap>(
        lock: Weak<Self>,
        map: Arc<M>,
    ) -> Option<Result<TxnMutexGuard<M>, ObseleteLock>> {
        let lock = lock.upgrade()?;
        loop {
            match lock
                .mutex_state
                .compare_and_swap(UNLOCKED, LOCKED, Ordering::SeqCst)
            {
                UNLOCKED => {
                    return Some(Ok(TxnMutexGuard { map, lock }));
                }
                OBSOLETE => {
                    return Some(Err(ObseleteLock));
                }
                _ => {
                    lock.mutex_event.listen().await;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ObseleteLock;

pub struct TxnMutexGuard<M: OrderedLockMap> {
    map: Arc<M>,
    lock: Arc<MemoryLock>,
}

impl<M: OrderedLockMap> TxnMutexGuard<M> {
    pub fn key(&self) -> &[u8] {
        &self.lock.key
    }

    pub fn with_lock_info<T>(&self, f: impl FnOnce(&mut Option<LockInfo>) -> T) -> T {
        let mut lock_info = self.lock.lock_info.lock();
        let ret = f(&mut *lock_info);
        if lock_info.is_none() {
            self.lock.pessimistic_event.notify(usize::MAX);
        }
        ret
    }

    pub async fn lock_released(&self) {
        self.lock.pessimistic_event.listen().await
    }
}

impl<M: OrderedLockMap> Drop for TxnMutexGuard<M> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.lock) == 1 && self.lock.lock_info.lock().is_none() {
            self.map.remove(&self.lock.key);
            assert_eq!(
                self.lock.mutex_state.swap(OBSOLETE, Ordering::SeqCst),
                LOCKED
            );
            self.lock.mutex_event.notify(usize::MAX);
        } else {
            assert_eq!(
                self.lock.mutex_state.swap(UNLOCKED, Ordering::SeqCst),
                LOCKED
            );
            self.lock.mutex_event.notify(1);
        }
    }
}

pub struct LockReadGuard<M: OrderedLockMap> {
    map: Arc<M>,
    lock: Arc<MemoryLock>,
}

impl<M: OrderedLockMap> LockReadGuard<M> {
    pub fn key(&self) -> &[u8] {
        &self.lock.key
    }

    pub fn with_lock_info<T>(&self, f: impl FnOnce(&Option<LockInfo>) -> T) -> T {
        let guard = self.lock.lock_info.lock();
        f(&*guard)
    }
}

impl<M: OrderedLockMap> Drop for LockReadGuard<M> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.lock) == 1 {
            if self
                .lock
                .mutex_state
                .compare_and_swap(UNLOCKED, OBSOLETE, Ordering::SeqCst)
                == UNLOCKED
            {
                self.map.remove(&self.lock.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, time::Duration};
    use tokio::time::delay_for;

    #[tokio::test]
    async fn test_txn_mutex() {
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        let lock = Arc::new(MemoryLock::new(b"k".to_vec()));
        map.lock().insert(b"k".to_vec(), Arc::downgrade(&lock));

        let counter = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let map = map.clone();
            let lock = Arc::downgrade(&lock);
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let _guard = MemoryLock::mutex_lock(lock, map).await.unwrap().unwrap();
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
    }
}

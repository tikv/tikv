// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::lock_table::OrderedLockMap;

use event_listener::Event;
use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;
use std::{
    future::Future,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

const INIT_REF_COUNT: usize = usize::MAX;

pub struct MemoryLock {
    mutex_locked: AtomicBool,
    mutex_event: Event,
    lock_info: Mutex<Option<LockInfo>>,
    pessimistic_event: Event,
    ref_count: AtomicUsize,
}

impl Default for MemoryLock {
    fn default() -> Self {
        MemoryLock {
            mutex_locked: AtomicBool::new(false),
            mutex_event: Event::new(),
            lock_info: Mutex::new(None),
            pessimistic_event: Event::new(),
            ref_count: AtomicUsize::new(INIT_REF_COUNT),
        }
    }
}

pub struct MemoryLockRef<'m, M: OrderedLockMap> {
    map: &'m M,
    key: Vec<u8>,
    lock: Arc<MemoryLock>,
}

impl<'m, M: OrderedLockMap> MemoryLockRef<'m, M> {
    pub fn new(map: &'m M, key: Vec<u8>, lock: Arc<MemoryLock>) -> Option<Self> {
        if lock
            .ref_count
            .compare_and_swap(INIT_REF_COUNT, 1, Ordering::SeqCst)
            == INIT_REF_COUNT
        {
            Some(MemoryLockRef { map, key, lock })
        } else {
            if lock.ref_count.fetch_add(1, Ordering::SeqCst) == 0 {
                None
            } else {
                Some(MemoryLockRef { map, key, lock })
            }
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub async fn mutex_lock(self) -> TxnMutexGuard<'m, M> {
        loop {
            match self
                .mutex_locked
                .compare_and_swap(false, true, Ordering::SeqCst)
            {
                false => {
                    return TxnMutexGuard(self);
                }
                _ => {
                    self.mutex_event.listen().await;
                }
            }
        }
    }

    pub fn with_lock_info<T>(&self, f: impl FnOnce(&Option<LockInfo>) -> T) -> T {
        f(&self.lock_info.lock())
    }
}

impl<'m, M: OrderedLockMap> Deref for MemoryLockRef<'m, M> {
    type Target = Arc<MemoryLock>;

    fn deref(&self) -> &Arc<MemoryLock> {
        &self.lock
    }
}

impl<'m, M: OrderedLockMap> Drop for MemoryLockRef<'m, M> {
    fn drop(&mut self) {
        if self.lock.ref_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.map.remove(&self.key);
        }
    }
}

pub struct TxnMutexGuard<'m, M: OrderedLockMap>(MemoryLockRef<'m, M>);

impl<'m, M: OrderedLockMap> TxnMutexGuard<'m, M> {
    pub fn with_lock_info<T>(&self, f: impl FnOnce(&mut Option<LockInfo>) -> T) -> T {
        let mut lock_info = self.0.lock_info.lock();
        let before = lock_info.is_some() as i32;
        let ret = f(&mut *lock_info);
        let after = lock_info.is_some() as i32;
        let diff = after - before;
        if diff > 0 {
            // A new lock is stored, increase the reference count by one to prevent
            // it from being removed from the lock table.
            self.0.ref_count.fetch_add(1, Ordering::SeqCst);
        } else if diff < 0 {
            // The lock stored inside is released, notify all other tasks that are
            // blocked by this lock.
            self.0.pessimistic_event.notify(usize::MAX);
        }
        ret
    }

    pub fn lock_released(&self) -> impl Future<Output = ()> {
        self.0.pessimistic_event.listen()
    }
}

impl<'m, M: OrderedLockMap> Drop for TxnMutexGuard<'m, M> {
    fn drop(&mut self) {
        self.0.mutex_locked.store(false, Ordering::SeqCst);
        self.0.mutex_event.notify(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, time::Duration};
    use tokio::{sync::mpsc, time::delay_for};

    #[tokio::test]
    async fn test_txn_mutex() {
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        let lock = Arc::new(MemoryLock::default());
        map.insert_if_not_exist(b"k".to_vec(), lock.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let map = map.clone();
            let lock = lock.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let lock_ref = MemoryLockRef::new(&*map, b"k".to_vec(), lock).unwrap();
                let _guard = lock_ref.mutex_lock().await;
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
    async fn test_wait_for_lock_released() {
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        let lock = Arc::new(MemoryLock::default());
        map.insert_if_not_exist(b"k".to_vec(), lock.clone());

        let mut lock_info = LockInfo::default();
        lock_info.set_key(b"k".to_vec());
        *lock.lock_info.lock() = Some(lock_info.clone());

        let (tx, mut rx) = mpsc::unbounded_channel();

        let map2 = map.clone();
        let lock2 = lock.clone();
        let handle = tokio::spawn(async move {
            rx.recv().await;

            let lock_ref = MemoryLockRef::new(&*map2, b"k".to_vec(), lock2).unwrap();
            let guard = lock_ref.mutex_lock().await;
            // Clear lock_info
            guard.with_lock_info(|lock_info| *lock_info = None);

            // After lock is cleared, we should be able to receive the messages
            // sent by other tasks blocked by the lock.
            for _ in 0..5 {
                rx.recv().await;
            }
        });

        for _ in 0..5 {
            let map = map.clone();
            let lock = lock.clone();
            let lock_ref = MemoryLockRef::new(&*map, b"k".to_vec(), lock).unwrap();
            let guard = lock_ref.mutex_lock().await;
            guard.with_lock_info(|lock_info| assert!(lock_info.is_some()));

            let wait_future = guard.lock_released();
            let tx = tx.clone();
            tokio::spawn(async move {
                wait_future.await;
                tx.send(()).unwrap();
            });
        }

        tx.send(()).unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_ref_count() {
        let map = Mutex::new(BTreeMap::new());

        // simple case
        map.insert_if_not_exist(b"k".to_vec(), Arc::new(MemoryLock::default()));
        let lock_ref1 = map.get(b"k").unwrap();
        let lock_ref2 = map.get(b"k").unwrap();
        drop(lock_ref1);
        assert!(map.get(b"k").is_some());
        drop(lock_ref2);
        assert!(map.get(b"k").is_none());

        // should not removed it from the lock table if a lock is stored in it
        map.insert_if_not_exist(b"k".to_vec(), Arc::new(MemoryLock::default()));
        let guard = map.get(b"k").unwrap().mutex_lock().await;
        guard.with_lock_info(|lock_info| *lock_info = Some(LockInfo::default()));
        drop(guard);
        assert!(map.get(b"k").is_some());

        // remove the lock stored in the table and
        let guard = map.get(b"k").unwrap().mutex_lock().await;
        guard.with_lock_info(|lock_info| *lock_info = None);
        drop(guard);
        assert!(map.get(b"k").is_some());
    }
}

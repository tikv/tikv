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
use txn_types::Key;

const INIT_REF_COUNT: usize = usize::MAX;

/// An entry in the in-memory lock table. You should always use it with
/// `MemoryLockRef`.
///
/// `MemoryLock` can be used as a key mutex. Acquiring a mutex with `mutex_lock`
/// can block concurrent writing operations on the same key. With the mutex,
/// a `LockInfo` can be also stored in it. The event of cleaning the stored lock
/// can be subscribed using `lock_released`.
// TODO: extract the mutex function and the lock store function to separate
// structs.
pub struct MemoryLock {
    key: Key,
    mutex_locked: AtomicBool,
    mutex_released_event: Event,
    lock_info: Mutex<Option<LockInfo>>,
    lock_cleaned_event: Event,
    ref_count: AtomicUsize,
}

impl MemoryLock {
    pub fn new(key: Key) -> Self {
        MemoryLock {
            key,
            mutex_locked: AtomicBool::new(false),
            mutex_released_event: Event::new(),
            lock_info: Mutex::new(None),
            lock_cleaned_event: Event::new(),
            ref_count: AtomicUsize::new(INIT_REF_COUNT),
        }
    }

    pub fn get_ref<'m, M: OrderedLockMap>(
        self: Arc<Self>,
        map: &'m M,
    ) -> Option<MemoryLockRef<'m, M>> {
        let mut ref_count = self.ref_count.load(Ordering::SeqCst);
        loop {
            // It is possible that the reference count has just decreased to zero and not
            // been removed from the map. In this case, we should not create a new reference
            // because the lock will be removed from the map immediately.
            if ref_count == 0 {
                return None;
            }
            let new_value = if ref_count == INIT_REF_COUNT {
                1
            } else {
                ref_count + 1
            };
            match self.ref_count.compare_exchange(
                ref_count,
                new_value,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(MemoryLockRef { lock: self, map });
                }
                Err(n) => ref_count = n,
            }
        }
    }
}

pub struct MemoryLockRef<'m, M: OrderedLockMap> {
    lock: Arc<MemoryLock>,
    map: &'m M,
}

impl<'m, M: OrderedLockMap> MemoryLockRef<'m, M> {
    pub fn key(&self) -> &Key {
        &self.key
    }

    pub async fn mutex_lock(self) -> TxnMutexGuard<'m, M> {
        loop {
            if self
                .mutex_locked
                .compare_and_swap(false, true, Ordering::SeqCst)
            {
                // current value of locked is true, listen to the release event
                self.mutex_released_event.listen().await;
            } else {
                return TxnMutexGuard(self);
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
    pub fn key(&self) -> &Key {
        &self.0.key()
    }

    pub fn with_lock_info<T>(&self, f: impl FnOnce(&mut Option<LockInfo>) -> T) -> T {
        use std::cmp::Ordering::*;

        let mut lock_info = self.0.lock_info.lock();
        let before = lock_info.is_some() as i32;
        let ret = f(&mut *lock_info);
        let after = lock_info.is_some() as i32;
        match after.cmp(&before) {
            Greater => {
                // A new lock is stored, increase the reference count by one to prevent
                // it from being removed from the lock table.
                self.0.ref_count.fetch_add(1, Ordering::SeqCst);
            }
            Less => {
                // The lock stored inside is released, notify all other tasks that are
                // blocked by this lock.
                self.0.lock_cleaned_event.notify(usize::MAX);
            }
            Equal => {}
        }
        ret
    }

    // Question: should we accept a key here and redirect the key
    // to the deadlock detector in this method?
    pub fn lock_released(&self) -> impl Future<Output = ()> {
        self.0.lock_cleaned_event.listen()
    }
}

impl<'m, M: OrderedLockMap> Drop for TxnMutexGuard<'m, M> {
    fn drop(&mut self) {
        self.0.mutex_locked.store(false, Ordering::SeqCst);
        self.0.mutex_released_event.notify(1);
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
        let lock = Arc::new(MemoryLock::new(Key::from_raw(b"k")));
        map.insert_if_not_exist(Key::from_raw(b"k"), lock.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let map = map.clone();
            let lock = lock.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let lock_ref = lock.get_ref(&*map).unwrap();
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
        let lock = Arc::new(MemoryLock::new(Key::from_raw(b"k")));
        map.insert_if_not_exist(Key::from_raw(b"k"), lock.clone());

        let mut lock_info = LockInfo::default();
        lock_info.set_key(b"k".to_vec());
        lock.clone()
            .get_ref(&*map.clone())
            .unwrap()
            .mutex_lock()
            .await
            .with_lock_info(|l| *l = Some(lock_info.clone()));

        let mut handles = Vec::new();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..5 {
            let map = map.clone();
            let lock = lock.clone();
            let lock_ref = lock.get_ref(&*map).unwrap();
            let guard = lock_ref.mutex_lock().await;
            guard.with_lock_info(|lock_info| assert!(lock_info.is_some()));

            let wait_future = guard.lock_released();
            let counter = counter.clone();
            handles.push(tokio::spawn(async move {
                wait_future.await;
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }

        delay_for(Duration::from_millis(100)).await;
        // still waiting for the lock to be released
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        let map2 = map.clone();
        let lock2 = lock.clone();
        tokio::spawn(async move {
            let lock_ref = lock2.get_ref(&*map2).unwrap();
            let guard = lock_ref.mutex_lock().await;
            // Clear lock_info
            guard.with_lock_info(|lock_info| *lock_info = None);
        })
        .await
        .unwrap();

        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_ref_count() {
        let map = Mutex::new(BTreeMap::new());

        let k = Key::from_raw(b"k");

        // simple case
        map.insert_if_not_exist(k.clone(), Arc::new(MemoryLock::new(k.clone())));
        let lock_ref1 = map.get(&k).unwrap();
        let lock_ref2 = map.get(&k).unwrap();
        drop(lock_ref1);
        assert!(map.get(&k).is_some());
        drop(lock_ref2);
        assert!(map.get(&k).is_none());

        // should not removed it from the lock table if a lock is stored in it
        map.insert_if_not_exist(k.clone(), Arc::new(MemoryLock::new(k.clone())));
        let guard = map.get(&k).unwrap().mutex_lock().await;
        guard.with_lock_info(|lock_info| *lock_info = Some(LockInfo::default()));
        drop(guard);
        assert!(map.get(&k).is_some());

        // remove the lock stored in the table and
        let guard = map.get(&k).unwrap().mutex_lock().await;
        guard.with_lock_info(|lock_info| *lock_info = None);
        drop(guard);
        assert!(map.get(&k).is_some());
    }
}

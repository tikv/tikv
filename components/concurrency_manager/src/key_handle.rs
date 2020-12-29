// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::lock_table::LockTable;
use parking_lot::Mutex;
use std::{mem, sync::Arc};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use txn_types::{Key, Lock};

/// An entry in the in-memory table providing functions related to a specific
/// key.
pub struct KeyHandle {
    pub key: Key,
    table: LockTable,
    mutex: AsyncMutex<()>,
    lock_store: Mutex<Option<Lock>>,
}

impl KeyHandle {
    pub fn new(key: Key, table: LockTable) -> Self {
        KeyHandle {
            key,
            table,
            mutex: AsyncMutex::new(()),
            lock_store: Mutex::new(None),
        }
    }

    pub async fn lock(self: Arc<Self>) -> KeyHandleGuard {
        // Safety: `_mutex_guard` is declared before `handle_ref` in `KeyHandleGuard`.
        // So the mutex guard will be released earlier than the `Arc<KeyHandle>`.
        // Then we can make sure the mutex guard doesn't point to released memory.
        let mutex_guard = unsafe { mem::transmute(self.mutex.lock().await) };
        KeyHandleGuard {
            _mutex_guard: mutex_guard,
            handle: self,
        }
    }

    pub fn with_lock<T>(&self, f: impl FnOnce(&Option<Lock>) -> T) -> T {
        f(&*self.lock_store.lock())
    }
}

impl Drop for KeyHandle {
    fn drop(&mut self) {
        self.table.remove(&self.key);
    }
}

/// A `KeyHandle` with its mutex locked.
pub struct KeyHandleGuard {
    // It must be declared before `handle` so it will be dropped before
    // `handle`.
    _mutex_guard: AsyncMutexGuard<'static, ()>,
    // It is unsafe to mutate `handle` to point at another `KeyHandle`.
    // Otherwise `_mutex_guard` can be invalidated.
    handle: Arc<KeyHandle>,
}

impl KeyHandleGuard {
    pub fn key(&self) -> &Key {
        &self.handle.key
    }

    pub fn with_lock<T>(&self, f: impl FnOnce(&mut Option<Lock>) -> T) -> T {
        f(&mut *self.handle.lock_store.lock())
    }
}

impl Drop for KeyHandleGuard {
    fn drop(&mut self) {
        let mut l = self.handle.lock_store.lock();
        info!("drop KeyHandleGuard"; "lock" => ?l);
        *l = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tokio::time::delay_for;

    #[tokio::test]
    async fn test_key_mutex() {
        let table = LockTable::default();
        let key_handle = Arc::new(KeyHandle::new(Key::from_raw(b"k"), table.clone()));
        table
            .0
            .insert(Key::from_raw(b"k"), Arc::downgrade(&key_handle));

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let key_handle = key_handle.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let _guard = key_handle.lock().await;
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
    async fn test_ref_count() {
        let table = LockTable::default();

        let k = Key::from_raw(b"k");

        let handle = Arc::new(KeyHandle::new(k.clone(), table.clone()));
        table.0.insert(k.clone(), Arc::downgrade(&handle));
        let lock_ref1 = table.get(&k).unwrap();
        let lock_ref2 = table.get(&k).unwrap();
        drop(handle);
        drop(lock_ref1);
        assert!(table.get(&k).is_some());
        drop(lock_ref2);
        assert!(table.get(&k).is_none());
    }
}

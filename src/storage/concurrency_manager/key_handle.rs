// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod lock_store;

use self::lock_store::LockStore;
use super::handle_table::HandleTable;

use std::{
    mem,
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use txn_types::{Key, Lock};

/// An entry in the in-memory table providing functions related to a specific
/// key.
///
/// You should always use it with `KeyHandleRef` so useless `KeyHandle`s can
/// be removed from the table automatically.
pub struct KeyHandle {
    key: Key,
    ref_count: AtomicUsize,
    key_mutex: AsyncMutex<()>,
    lock_store: LockStore,
}

impl KeyHandle {
    pub fn new_with_ref(key: Key, table: &HandleTable) -> KeyHandleWithRef<'_> {
        let key_handle = Arc::new(KeyHandle {
            key,
            ref_count: AtomicUsize::new(1),
            key_mutex: AsyncMutex::new(()),
            lock_store: LockStore::new(),
        });
        let key_handle_ref = KeyHandleRef {
            handle: key_handle.clone(),
            table,
        };
        KeyHandleWithRef {
            key_handle_ref,
            key_handle,
        }
    }

    pub fn get_ref<'m>(self: Arc<Self>, table: &'m HandleTable) -> Option<KeyHandleRef<'m>> {
        let mut ref_count = self.ref_count.load(Ordering::SeqCst);
        loop {
            // It is possible that the reference count has just decreased to zero and not
            // been removed from the map. In this case, we should not create a new reference
            // because the handle will be removed from the map immediately.
            if ref_count == 0 {
                return None;
            }
            match self.ref_count.compare_exchange(
                ref_count,
                ref_count + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(KeyHandleRef {
                        handle: self,
                        table,
                    });
                }
                Err(n) => ref_count = n,
            }
        }
    }
}

pub struct KeyHandleRef<'m> {
    handle: Arc<KeyHandle>,
    table: &'m HandleTable,
}

impl<'m> KeyHandleRef<'m> {
    pub fn key(&self) -> &Key {
        &self.key
    }

    pub async fn mutex_lock(self) -> KeyHandleMutexGuard<'m> {
        // Safety: `_mutex_guard` is declared after `handle_ref` in `KeyHandleMutexGuard`.
        // So the mutex guard will be released earlier than the `Arc<KeyHandle>`.
        // Then we can make sure the mutex guard doesn't point to released memory.
        let mutex_guard = unsafe { mem::transmute(self.key_mutex.lock().await) };
        KeyHandleMutexGuard {
            handle_ref: self,
            _mutex_guard: mutex_guard,
        }
    }

    pub fn with_lock<T>(&self, f: impl FnOnce(&Option<Lock>) -> T) -> T {
        self.lock_store.read(f)
    }
}

impl<'m> Deref for KeyHandleRef<'m> {
    type Target = Arc<KeyHandle>;

    fn deref(&self) -> &Arc<KeyHandle> {
        &self.handle
    }
}

impl<'m> Drop for KeyHandleRef<'m> {
    fn drop(&mut self) {
        if self.handle.ref_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.table.remove(&self.key);
        }
    }
}

pub struct KeyHandleWithRef<'m> {
    pub(super) key_handle_ref: KeyHandleRef<'m>,
    pub(super) key_handle: Arc<KeyHandle>,
}

/// A `KeyHandleRef` with its mutex locked.
pub struct KeyHandleMutexGuard<'m> {
    // It must be declared before `handle_ref` so it will be dropped before
    // `handle_ref`.
    _mutex_guard: AsyncMutexGuard<'m, ()>,
    handle_ref: KeyHandleRef<'m>,
}

impl<'m> KeyHandleMutexGuard<'m> {
    pub fn key(&self) -> &Key {
        &self.handle_ref.key()
    }

    pub fn with_lock<T>(&self, f: impl FnOnce(&mut Option<Lock>) -> T) -> T {
        self.handle_ref
            .lock_store
            .write(f, &self.handle_ref.ref_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::{collections::BTreeMap, time::Duration};
    use tokio::time::delay_for;
    use txn_types::LockType;

    #[tokio::test]
    async fn test_key_mutex() {
        let table = HandleTable(Arc::new(Mutex::new(BTreeMap::new())));
        let handle_with_ref = KeyHandle::new_with_ref(Key::from_raw(b"k"), &table);
        table.insert_if_not_exist(Key::from_raw(b"k"), handle_with_ref.key_handle.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let table = table.clone();
            let handle = handle_with_ref.key_handle.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let lock_ref = handle.get_ref(&table).unwrap();
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
    async fn test_ref_count() {
        let table = HandleTable(Arc::new(Mutex::new(BTreeMap::new())));

        let k = Key::from_raw(b"k");

        // simple case
        let with_ref = KeyHandle::new_with_ref(k.clone(), &table);
        table.insert_if_not_exist(k.clone(), with_ref.key_handle);
        let lock_ref1 = table.get(&k).unwrap();
        let lock_ref2 = table.get(&k).unwrap();
        drop(with_ref.key_handle_ref);
        drop(lock_ref1);
        assert!(table.get(&k).is_some());
        drop(lock_ref2);
        assert!(table.get(&k).is_none());

        // should not removed it from the table if a lock is stored in it
        let with_ref = KeyHandle::new_with_ref(k.clone(), &table);
        table.insert_if_not_exist(k.clone(), with_ref.key_handle);
        let guard = table.get(&k).unwrap().mutex_lock().await;
        guard.with_lock(|lock| {
            *lock = Some(Lock::new(
                LockType::Lock,
                b"k".to_vec(),
                1.into(),
                100,
                None,
                1.into(),
                1,
                1.into(),
            ))
        });
        drop(with_ref.key_handle_ref);
        drop(guard);
        assert!(table.get(&k).is_some());

        // remove the lock stored in, then the handle should be removed from the table
        let guard = table.get(&k).unwrap().mutex_lock().await;
        guard.with_lock(|lock| *lock = None);
        drop(guard);
        assert!(table.get(&k).is_some());
    }
}

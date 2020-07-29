// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::key_handle::{KeyHandle, KeyHandleMutexGuard, KeyHandleRef};

use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;
use std::{collections::BTreeMap, ops::Bound, sync::Arc};
use txn_types::Key;

#[derive(Default)]
pub struct HandleTable<M>(Arc<M>);

impl<M: OrderedMap> Clone for HandleTable<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M: OrderedMap> HandleTable<M> {
    pub async fn lock_key(&self, key: &Key) -> KeyHandleMutexGuard<'_, M> {
        loop {
            if let Some(handle) = self.0.get(key) {
                return handle.mutex_lock().await;
            } else {
                let handle = Arc::new(KeyHandle::new(key.clone()));
                let handle_ref = handle.clone().get_ref(&*self.0).unwrap();
                let guard = handle_ref.mutex_lock().await;
                if self.0.insert_if_not_exist(key.clone(), handle) {
                    return guard;
                }
            }
        }
    }

    pub fn check_key(
        &self,
        key: &Key,
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
        start_key: &Key,
        end_key: &Key,
        mut check_fn: impl FnMut(&LockInfo) -> bool,
    ) -> Result<(), LockInfo> {
        let blocking_lock = self.0.find_first(start_key, end_key, |lock_ref| {
            lock_ref.with_lock_info(|lock_info| {
                lock_info.as_ref().and_then(|lock_info| {
                    if check_fn(lock_info) {
                        None
                    } else {
                        Some(lock_info.clone())
                    }
                })
            })
        });
        if let Some(lock) = blocking_lock {
            Err(lock)
        } else {
            Ok(())
        }
    }
}

/// A concurrent ordered map which maps encoded keys to key handle.
pub trait OrderedMap: Default + Send + Sync + 'static {
    /// Inserts a key handle to the map if the key does not exists in the map.
    /// Returns whether the handle is successfully inserted into the map.
    fn insert_if_not_exist(&self, key: Key, handle: Arc<KeyHandle>) -> bool;

    /// Gets the handle of the key.
    fn get<'m>(&'m self, key: &Key) -> Option<KeyHandleRef<'m, Self>>;

    /// Finds the first handle in the given range that `pred` returns `Some`.
    /// The `Some` return value of `pred` will be returned by `find_first`.
    fn find_first<'m, T>(
        &'m self,
        start_key: &Key,
        end_key: &Key,
        pred: impl FnMut(KeyHandleRef<'m, Self>) -> Option<T>,
    ) -> Option<T>;

    /// Removes the key and its key handle from the map.
    fn remove(&self, key: &Key);
}

impl OrderedMap for Mutex<BTreeMap<Key, Arc<KeyHandle>>> {
    fn insert_if_not_exist(&self, key: Key, handle: Arc<KeyHandle>) -> bool {
        use std::collections::btree_map::Entry;

        match self.lock().entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(handle);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    fn get<'m>(&'m self, key: &Key) -> Option<KeyHandleRef<'m, Self>> {
        self.lock()
            .get(key)
            .and_then(|handle| handle.clone().get_ref(self))
    }

    fn find_first<'m, T>(
        &'m self,
        start_key: &Key,
        end_key: &Key,
        mut pred: impl FnMut(KeyHandleRef<'m, Self>) -> Option<T>,
    ) -> Option<T> {
        for (_, handle) in self
            .lock()
            .range::<Key, _>((Bound::Included(start_key), Bound::Excluded(end_key)))
        {
            if let Some(v) = handle.clone().get_ref(self).and_then(&mut pred) {
                return Some(v);
            }
        }
        None
    }

    fn remove(&self, key: &Key) {
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
        let lock_table = HandleTable::<Mutex<BTreeMap<Key, Arc<KeyHandle>>>>::default();

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let lock_table = lock_table.clone();
            let counter = counter.clone();
            let handle = tokio::spawn(async move {
                let _guard = lock_table.lock_key(&Key::from_raw(b"k")).await;
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
        let lock_table = HandleTable::<Mutex<BTreeMap<Key, Arc<KeyHandle>>>>::default();
        let key_k = Key::from_raw(b"k");

        // no lock found
        assert!(lock_table.check_key(&key_k, |_| false).is_ok());

        let mut lock_info = LockInfo::default();
        lock_info.set_lock_version(10);
        lock_table.lock_key(&key_k).await.with_lock_info(|l| {
            *l = Some(lock_info.clone());
        });

        // lock passes check_fn
        assert!(lock_table
            .check_key(&key_k, |l| l.get_lock_version() < 20)
            .is_ok());

        // lock does not pass check_fn
        assert_eq!(
            lock_table.check_key(&key_k, |l| l.get_lock_version() < 5),
            Err(lock_info)
        );
    }

    #[tokio::test]
    async fn test_check_range() {
        let lock_table = HandleTable::<Mutex<BTreeMap<Key, Arc<KeyHandle>>>>::default();

        let mut lock_k = LockInfo::default();
        lock_k.set_lock_version(10);
        lock_table
            .lock_key(&Key::from_raw(b"k"))
            .await
            .with_lock_info(|l| {
                *l = Some(lock_k.clone());
            });

        let mut lock_l = LockInfo::default();
        lock_l.set_lock_version(20);
        lock_table
            .lock_key(&Key::from_raw(b"l"))
            .await
            .with_lock_info(|l| {
                *l = Some(lock_l.clone());
            });

        // no lock found
        assert!(lock_table
            .check_range(&Key::from_raw(b"m"), &Key::from_raw(b"n"), |_| false)
            .is_ok());

        // lock passes check_fn
        assert!(lock_table
            .check_range(&Key::from_raw(b"a"), &Key::from_raw(b"z"), |l| l
                .get_lock_version()
                < 50)
            .is_ok());

        // first lock does not pass check_fn
        assert_eq!(
            lock_table.check_range(&Key::from_raw(b"a"), &Key::from_raw(b"z"), |l| l
                .get_lock_version()
                < 5),
            Err(lock_k)
        );

        // first lock passes check_fn but the second does not
        assert_eq!(
            lock_table.check_range(&Key::from_raw(b"a"), &Key::from_raw(b"z"), |l| l
                .get_lock_version()
                < 15),
            Err(lock_l)
        );
    }
}

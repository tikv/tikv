// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use parking_lot::Mutex;
use txn_types::Lock;

use std::sync::atomic::{AtomicUsize, Ordering};

pub struct LockStore {
    lock: Mutex<Option<Lock>>,
}

impl LockStore {
    pub fn new() -> Self {
        LockStore {
            lock: Mutex::new(None),
        }
    }

    pub fn read<T>(&self, f: impl FnOnce(&Option<Lock>) -> T) -> T {
        f(&*self.lock.lock())
    }

    pub fn write<T>(&self, f: impl FnOnce(&mut Option<Lock>) -> T, ref_count: &AtomicUsize) -> T {
        let mut lock = self.lock.lock();
        let has_lock_before = lock.is_some();
        let ret = f(&mut *lock);
        let has_lock_after = lock.is_some();
        match (has_lock_before, has_lock_after) {
            (false, true) => {
                // A new lock is stored, increase the reference count by one to prevent
                // it from being removed from the table.
                ref_count.fetch_add(1, Ordering::SeqCst);
            }
            (true, false) => {
                // TODO: wake up tasks waiting for releasing the lock
            }
            _ => {}
        }
        ret
    }
}

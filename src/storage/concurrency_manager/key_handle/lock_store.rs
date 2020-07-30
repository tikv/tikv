// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::KeyHandle;

use parking_lot::Mutex;
use txn_types::Lock;

use std::sync::Arc;

pub struct LockStore {
    lock: Mutex<(Option<Lock>, Option<Arc<KeyHandle>>)>,
}

impl LockStore {
    pub fn new() -> Self {
        LockStore {
            lock: Mutex::new((None, None)),
        }
    }

    pub fn read<T>(&self, f: impl FnOnce(&Option<Lock>) -> T) -> T {
        f(&self.lock.lock().0)
    }

    pub fn write<T>(&self, f: impl FnOnce(&mut Option<Lock>) -> T, handle: &Arc<KeyHandle>) -> T {
        let mut guard = self.lock.lock();
        let has_lock_before = guard.0.is_some();
        let ret = f(&mut guard.0);
        let has_lock_after = guard.0.is_some();
        match (has_lock_before, has_lock_after) {
            (false, true) => {
                // A new lock is stored, increase the reference count by one to prevent
                // it from being removed from the table.
                guard.1 = Some(handle.clone());
            }
            (true, false) => {
                // TODO: wake up tasks waiting for releasing the lock
            }
            _ => {}
        }
        ret
    }
}

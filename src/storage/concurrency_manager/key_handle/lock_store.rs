// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;

use std::sync::atomic::{AtomicUsize, Ordering};

pub struct LockStore {
    lock_info: Mutex<Option<LockInfo>>,
}

impl LockStore {
    pub fn new() -> Self {
        LockStore {
            lock_info: Mutex::new(None),
        }
    }

    pub fn read<T>(&self, f: impl FnOnce(&Option<LockInfo>) -> T) -> T {
        f(&*self.lock_info.lock())
    }

    pub fn write<T>(
        &self,
        f: impl FnOnce(&mut Option<LockInfo>) -> T,
        ref_count: &AtomicUsize,
    ) -> T {
        let mut lock_info = self.lock_info.lock();
        let has_lock_before = lock_info.is_some();
        let ret = f(&mut *lock_info);
        let has_lock_after = lock_info.is_some();
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

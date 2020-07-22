// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use event_listener::Event;
use kvproto::kvrpcpb::LockInfo;
use parking_lot::Mutex;

use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct LockStore {
    lock_info: Mutex<Option<LockInfo>>,
    lock_cleaned_event: Event,
}

impl LockStore {
    pub fn new() -> Self {
        LockStore {
            lock_info: Mutex::new(None),
            lock_cleaned_event: Event::new(),
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
        use std::cmp::Ordering::*;

        let mut lock_info = self.lock_info.lock();
        let before = lock_info.is_some() as i32;
        let ret = f(&mut *lock_info);
        let after = lock_info.is_some() as i32;
        match after.cmp(&before) {
            Greater => {
                // A new lock is stored, increase the reference count by one to prevent
                // it from being removed from the table.
                ref_count.fetch_add(1, Ordering::SeqCst);
            }
            Less => {
                // The lock stored inside is released, notify all other tasks that are
                // blocked by this lock.
                self.lock_cleaned_event.notify(usize::MAX);
            }
            Equal => {}
        }
        ret
    }

    pub fn lock_released(&self) -> impl Future<Output = ()> {
        debug_assert!(self.lock_info.lock().is_some());
        self.lock_cleaned_event.listen()
    }
}

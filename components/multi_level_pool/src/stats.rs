// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam::epoch::{self, Atomic};

use dashmap::DashMap;

// Swap interval (in secs)
const SWAP_INTERVAL: u64 = 20;

#[derive(Debug, Default)]
pub struct TaskStats {
    pub elapsed: AtomicU64,
}

#[derive(Clone)]
pub struct StatsMap {
    new: Atomic<DashMap<u64, Arc<TaskStats>>>,
    old: Atomic<DashMap<u64, Arc<TaskStats>>>,
}

impl StatsMap {
    pub fn new() -> Self {
        let new = Atomic::new(DashMap::default());
        let old = Atomic::new(DashMap::default());
        let new2 = new.clone();
        let old2 = old.clone();
        thread::spawn(move || {
            let guard = &epoch::pin();
            loop {
                thread::sleep(Duration::from_secs(SWAP_INTERVAL));
                let new_ptr = new2.load(Ordering::SeqCst, guard);
                let old_ptr = old2.swap(new_ptr, Ordering::SeqCst, guard);
                unsafe {
                    old_ptr.deref().clear();
                }
                new2.store(old_ptr, Ordering::SeqCst);
            }
        });
        StatsMap { new, old }
    }

    pub fn get_stats(&self, key: u64) -> Arc<TaskStats> {
        unsafe {
            let guard = &epoch::pin();
            let new = self.new.load(Ordering::SeqCst, guard);
            if let Some(v) = new.deref().get(&key) {
                return v.clone();
            }
            let old = self.old.load(Ordering::SeqCst, guard);
            if let Some((_, v)) = old.deref().remove(&key) {
                new.deref().insert(key, v.clone());
                return v;
            }
            let stats = new
                .deref()
                .get_or_insert(&key, Arc::new(TaskStats::default()));
            stats.clone()
        }
    }
}

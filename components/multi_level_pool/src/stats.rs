// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam::epoch::{self, Atomic};
use dashmap::DashMap;

// Swap interval
const SWAP_INTERVAL: Duration = Duration::from_secs(20);

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

    pub fn async_cleanup(&self) -> impl Future<Output = ()> {
        let new = self.new.clone();
        let old = self.old.clone();
        async move {
            loop {
                tokio_timer::delay_for(SWAP_INTERVAL).await;
                let guard = &epoch::pin();
                let new_ptr = new.load(Ordering::SeqCst, guard);
                let old_ptr = old.swap(new_ptr, Ordering::SeqCst, guard);
                unsafe {
                    old_ptr.deref().clear();
                }
                new.store(old_ptr, Ordering::SeqCst);
            }
        }
    }
}

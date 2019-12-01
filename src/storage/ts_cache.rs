// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::mvcc::TimeStamp;

pub trait Cache: Sync + Send {
    fn get(&self) -> TimeStamp;

    fn update(&self, ts: TimeStamp);
}

pub struct AtomicCache(AtomicU64);

impl AtomicCache {
    // TODO: Get Ts from PD
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }
}

impl Cache for AtomicCache {
    fn get(&self) -> TimeStamp {
        self.0.load(Ordering::Relaxed).into()
    }

    fn update(&self, ts: TimeStamp) {
        if ts != TimeStamp::max() {
            self.0.fetch_max(ts.into_inner(), Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_cache() {
        let cache = AtomicCache::new();
        cache.update(10.into());
        assert_eq!(cache.get(), 10.into());
        cache.update(5.into());
        assert_eq!(cache.get(), 10.into());
        cache.update(TimeStamp::max());
        assert_eq!(cache.get(), 10.into());
    }
}

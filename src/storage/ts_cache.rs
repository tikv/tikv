// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::mvcc::TimeStamp;

pub trait Cache {
    fn get(&self) -> TimeStamp;

    fn update(&self, ts: TimeStamp);
}

pub struct AtomicCache(AtomicU64);

impl AtomicCache {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }
}

impl Cache for AtomicCache {
    fn get(&self) -> TimeStamp {
        self.0.load(Ordering::Relaxed).into()
    }

    fn update(&self, ts: TimeStamp) {
        self.0.fetch_max(ts.into_inner(), Ordering::Relaxed);
    }
}

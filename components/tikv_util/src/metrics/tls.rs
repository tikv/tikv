// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use coarsetime::{Instant, Updater};
use std::cell::Cell;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

lazy_static! {
    static ref UPDATER_IS_RUNNING: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
}

fn ensure_updater() {
    if !UPDATER_IS_RUNNING.compare_and_swap(false, true, Ordering::SeqCst) {
        Updater::new(200).start().unwrap();
    }
}

pub trait TLSMetricGroupInner {
    fn flush_all(&self);
}

pub struct TLSMetricGroup<T> {
    inner: T,
    last_flush_time: Cell<Instant>,
}

impl<T: TLSMetricGroupInner> TLSMetricGroup<T> {
    pub fn new(inner: T) -> Self {
        ensure_updater();
        Self {
            inner,
            last_flush_time: Cell::new(Instant::recent()),
        }
    }

    /// Flushes the inner metrics if at least 1 second is passed since last flush.
    pub fn may_flush_all(&self) -> bool {
        let recent = Instant::recent();
        if (recent - self.last_flush_time.get()).as_secs() == 0 {
            // Minimum flush interval is 1s
            return false;
        }
        self.inner.flush_all();
        self.last_flush_time.set(recent);
        true
    }

    /// Flushes the inner metrics immediately.
    pub fn force_flush_all(&self) {
        self.inner.flush_all()
    }
}

impl<T: TLSMetricGroupInner> Deref for TLSMetricGroup<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

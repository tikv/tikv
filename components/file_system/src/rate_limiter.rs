// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{IOOp, IOType};

use crossbeam_utils::CachePadded;
use std::future::{self, Future};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

/// Record accumulated bytes through of different types.
/// Used for testing and metrics.
#[derive(Debug)]
pub struct IORateLimiterStatistics {
    read: [CachePadded<AtomicUsize>; IOType::VARIANT_COUNT],
    write: [CachePadded<AtomicUsize>; IOType::VARIANT_COUNT],
}

impl IORateLimiterStatistics {
    pub fn new() -> Self {
        IORateLimiterStatistics {
            read: Default::default(),
            write: Default::default(),
        }
    }

    pub fn add(&self, io_type: IOType, io_op: IOOp, len: usize) {
        match io_op {
            IOOp::Read => {
                self.read[io_type as usize].fetch_add(len, Ordering::Relaxed);
            }
            IOOp::Write => {
                self.write[io_type as usize].fetch_add(len, Ordering::Relaxed);
            }
        }
    }

    pub fn fetch(&self, io_type: IOType, io_op: IOOp) -> usize {
        match io_op {
            IOOp::Read => self.read[io_type as usize].load(Ordering::Relaxed),
            IOOp::Write => self.write[io_type as usize].load(Ordering::Relaxed),
        }
    }

    #[allow(dead_code)]
    pub fn reset(&self) {
        for i in self.read.iter() {
            i.store(0, Ordering::Relaxed);
        }
        for i in self.write.iter() {
            i.store(0, Ordering::Relaxed);
        }
    }
}

/// No-op limiter
/// An instance of `IORateLimiter` should be safely shared between threads.
#[derive(Debug)]
pub struct IORateLimiter {
    refill_bytes: usize,
    enable_statistics: bool,
    stats: Arc<IORateLimiterStatistics>,
}

impl IORateLimiter {
    pub fn new(refill_bytes: usize, enable_statistics: bool) -> IORateLimiter {
        IORateLimiter {
            refill_bytes,
            enable_statistics,
            stats: Arc::new(IORateLimiterStatistics::new()),
        }
    }

    pub fn statistics(&self) -> Arc<IORateLimiterStatistics> {
        self.stats.clone()
    }

    /// Request for token for bytes and potentially update statistics. If this
    /// request can not be satisfied, the call is blocked. Granted token can be
    /// less than the requested bytes, but must be greater than zero.
    pub fn request(&self, io_type: IOType, io_op: IOOp, bytes: usize) -> usize {
        let bytes = if self.refill_bytes > 0 {
            std::cmp::min(self.refill_bytes, bytes)
        } else {
            bytes
        };
        if self.enable_statistics {
            self.stats.add(io_type, io_op, bytes);
        }
        bytes
    }

    pub fn async_request(
        &self,
        io_type: IOType,
        io_op: IOOp,
        bytes: usize,
    ) -> impl Future<Output = usize> {
        future::ready(self.request(io_type, io_op, bytes))
    }
}

lazy_static! {
    static ref IO_RATE_LIMITER: Mutex<Option<Arc<IORateLimiter>>> = Mutex::new(None);
}

pub fn set_io_rate_limiter(limiter: Option<Arc<IORateLimiter>>) {
    *IO_RATE_LIMITER.lock().unwrap() = limiter;
}

pub fn get_io_rate_limiter() -> Option<Arc<IORateLimiter>> {
    if let Some(ref limiter) = *IO_RATE_LIMITER.lock().unwrap() {
        Some(limiter.clone())
    } else {
        None
    }
}

pub struct WithIORateLimit {
    previous_io_rate_limiter: Option<Arc<IORateLimiter>>,
}

impl WithIORateLimit {
    pub fn new(refill_bytes: usize) -> (Self, Arc<IORateLimiterStatistics>) {
        let previous_io_rate_limiter = get_io_rate_limiter();
        let limiter = Arc::new(IORateLimiter::new(
            refill_bytes,
            true, /*enable_statistics*/
        ));
        let stats = limiter.statistics();
        set_io_rate_limiter(Some(limiter));
        (
            WithIORateLimit {
                previous_io_rate_limiter,
            },
            stats,
        )
    }
}

impl Drop for WithIORateLimit {
    fn drop(&mut self) {
        set_io_rate_limiter(self.previous_io_rate_limiter.take());
    }
}

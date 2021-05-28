// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{IOOp, IOType};

use std::future::{self, Future};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use strum::EnumCount;

/// Record accumulated bytes through of different types.
/// Used for testing and metrics.
#[derive(Debug)]
pub struct IORateLimiterStatistics {
    read: [CachePadded<AtomicUsize>; IOType::COUNT],
    write: [CachePadded<AtomicUsize>; IOType::COUNT],
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
    stats: Option<Arc<IORateLimiterStatistics>>,
}

impl IORateLimiter {
    pub fn new(refill_bytes: usize, enable_statistics: bool) -> IORateLimiter {
        IORateLimiter {
            refill_bytes,
            stats: if enable_statistics {
                Some(Arc::new(IORateLimiterStatistics::new()))
            } else {
                None
            },
        }
    }

    pub fn statistics(&self) -> Option<Arc<IORateLimiterStatistics>> {
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
        if let Some(stats) = &self.stats {
            stats.add(io_type, io_op, bytes);
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

// Do NOT use this method in test environment.
pub fn set_io_rate_limiter(limiter: Option<Arc<IORateLimiter>>) {
    *IO_RATE_LIMITER.lock() = limiter;
}

pub fn get_io_rate_limiter() -> Option<Arc<IORateLimiter>> {
    (*IO_RATE_LIMITER.lock()).clone()
}

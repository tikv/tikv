// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{IOOp, IOType};

use std::future::{self, Future};
use std::sync::{atomic, Arc, Mutex};

/// No-op limiter
/// An instance of `IORateLimiter` should be safely shared between threads.
#[derive(Debug)]
pub struct IORateLimiter {
    unit: atomic::AtomicUsize,
}

impl IORateLimiter {
    pub fn new(_unit: usize) -> IORateLimiter {
        IORateLimiter {
            unit: atomic::AtomicUsize::new(_unit),
        }
    }

    /// Request for token for bytes and potentially update statistics. If this
    /// request can not be satisfied, the call is blocked. Granted token can be
    /// less than the requested amount.
    pub fn request(&self, _io_type: IOType, _io_op: IOOp, bytes: usize) -> usize {
        std::cmp::min(self.unit.load(atomic::Ordering::Relaxed), bytes)
    }

    pub fn async_request(
        &self,
        io_type: IOType,
        io_op: IOOp,
        bytes: usize,
    ) -> impl Future<Output = usize> {
        future::ready(self.request(io_type, io_op, bytes))
    }

    pub fn disable_rate_limit(&self, _io_type: IOType) {}

    pub fn enable_rate_limit(&self, _io_type: IOType) {}
}

lazy_static! {
    static ref IO_RATE_LIMITER: Mutex<Option<Arc<IORateLimiter>>> = Mutex::new(None);
}

pub fn set_io_rate_limiter(limiter: IORateLimiter) {
    *IO_RATE_LIMITER.lock().unwrap() = Some(Arc::new(limiter));
}

pub fn get_io_rate_limiter() -> Option<Arc<IORateLimiter>> {
    if let Some(ref limiter) = *IO_RATE_LIMITER.lock().unwrap() {
        Some(limiter.clone())
    } else {
        None
    }
}

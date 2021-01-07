// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{IOOp, IOType};

use std::future::{self, Future};
use std::sync::{
    atomic::{self, AtomicUsize, Ordering},
    Arc, Mutex,
};

/// Record accumulated bytes through of different types.
/// Used for testing and metrics.
#[derive(Debug)]
pub struct BytesRecorder {
    read: [AtomicUsize; IOType::VARIANT_COUNT],
    write: [AtomicUsize; IOType::VARIANT_COUNT],
}

impl BytesRecorder {
    pub fn new() -> Self {
        BytesRecorder {
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
    refill_bytes: AtomicUsize,
    recorder: Option<Arc<BytesRecorder>>,
}

impl IORateLimiter {
    pub fn new(refill_bytes: usize, recorder: Option<Arc<BytesRecorder>>) -> IORateLimiter {
        IORateLimiter {
            refill_bytes: AtomicUsize::new(refill_bytes),
            recorder,
        }
    }

    /// Request for token for bytes and potentially update statistics. If this
    /// request can not be satisfied, the call is blocked. Granted token can be
    /// less than the requested bytes, but must be greater than zero.
    pub fn request(&self, io_type: IOType, io_op: IOOp, bytes: usize) -> usize {
        let bytes = std::cmp::min(self.refill_bytes.load(atomic::Ordering::Relaxed), bytes);
        if let Some(recorder) = &self.recorder {
            recorder.add(io_type, io_op, bytes);
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

    pub fn disable_rate_limit(&self, _io_type: IOType) {}

    pub fn enable_rate_limit(&self, _io_type: IOType) {}
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

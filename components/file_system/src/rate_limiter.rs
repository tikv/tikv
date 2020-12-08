// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{condvar::Condvar, IOOp, IOType, IO_TYPE_VARIANTS};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use time::Timespec;

use self::inner::monotonic_now;
/// Returns the monotonic raw time since some unspecified starting point.
use self::inner::monotonic_raw_now;

#[cfg(not(target_os = "linux"))]
mod inner {
    const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;
    use time::{self, Timespec};

    pub fn monotonic_raw_now() -> Timespec {
        // TODO Add monotonic raw clock time impl for macos and windows
        // Currently use `time::get_precise_ns()` instead.
        let ns = time::precise_time_ns();
        let s = ns / NANOSECONDS_PER_SECOND;
        let ns = ns % NANOSECONDS_PER_SECOND;
        Timespec::new(s as i64, ns as i32)
    }

    pub fn monotonic_now() -> Timespec {
        // TODO Add monotonic clock time impl for macos and windows
        monotonic_raw_now()
    }
}

#[cfg(target_os = "linux")]
mod inner {
    use std::io;
    use time::Timespec;

    #[inline]
    pub fn monotonic_raw_now() -> Timespec {
        get_time(libc::CLOCK_MONOTONIC_RAW)
    }

    #[inline]
    pub fn monotonic_now() -> Timespec {
        get_time(libc::CLOCK_MONOTONIC)
    }

    #[inline]
    fn get_time(clock: libc::clockid_t) -> Timespec {
        let mut t = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let errno = unsafe { libc::clock_gettime(clock, &mut t) };
        if errno != 0 {
            panic!(
                "failed to get clocktime, err {}",
                io::Error::last_os_error()
            );
        }
        Timespec::new(t.tv_sec, t.tv_nsec as _)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum IOPriority {
    Limited,
    NotLimited,
}

#[derive(Debug)]
struct PerTypeIORateLimiter {
    bytes_per_refill: AtomicUsize,
    consumed: AtomicUsize,
    refill_period: Duration,
    last_refill_time: Mutex<Timespec>,
    condv: Condvar,
}

impl PerTypeIORateLimiter {
    pub fn new(bytes_per_sec: usize, refill_period: Duration) -> PerTypeIORateLimiter {
        PerTypeIORateLimiter {
            bytes_per_refill: AtomicUsize::new(
                bytes_per_sec * refill_period.as_millis() as usize / 1000,
            ),
            consumed: AtomicUsize::new(0),
            refill_period,
            last_refill_time: Mutex::new(monotonic_raw_now()),
            condv: Condvar::new(),
        }
    }

    pub fn request(&self, bytes: usize, priority: IOPriority) -> usize {
        if priority == IOPriority::NotLimited {
            self.consumed.fetch_add(bytes as usize, Ordering::Relaxed);
            return bytes;
        }
        let cached_bytes_per_refill = self.bytes_per_refill.load(Ordering::Relaxed);
        let mut cached_consumed = self.consumed.load(Ordering::Relaxed);
        loop {
            if cached_consumed < cached_bytes_per_refill {
                let after = self.consumed.fetch_add(bytes, Ordering::Relaxed);
                let exceeded = std::cmp::max(0, after - cached_bytes_per_refill);
                if exceeded < bytes {
                    break bytes - exceeded;
                }
            }
            let now = monotonic_raw_now();
            let mut last_refill_time = self.last_refill_time.lock().unwrap();
            if (now - *last_refill_time).to_std().unwrap() >= self.refill_period {
                *last_refill_time = now;
                let token = std::cmp::min(cached_bytes_per_refill, bytes);
                self.consumed.store(token, Ordering::Relaxed);
                self.condv.notify_all();
                break token;
            }
            let cached_last_refill_time = *last_refill_time;
            cached_consumed = self.consumed.load(Ordering::Relaxed);
            if cached_consumed >= cached_bytes_per_refill {
                let mut last_refill_time = self
                    .condv
                    .wait_timeout(last_refill_time, self.refill_period);
                if *last_refill_time == cached_last_refill_time {
                    *last_refill_time = monotonic_raw_now();
                    let token = std::cmp::min(cached_bytes_per_refill, bytes);
                    self.consumed.store(token, Ordering::Relaxed);
                    break token;
                }
            }
        }
    }

    async fn async_request(&self, bytes: usize, priority: IOPriority) -> usize {
        if priority == IOPriority::NotLimited {
            self.consumed.fetch_add(bytes as usize, Ordering::Relaxed);
            return bytes;
        }
        let cached_bytes_per_refill = self.bytes_per_refill.load(Ordering::Relaxed);
        let mut cached_consumed = self.consumed.load(Ordering::Relaxed);
        loop {
            if cached_consumed < cached_bytes_per_refill {
                let after = self.consumed.fetch_add(bytes, Ordering::Relaxed);
                let exceeded = std::cmp::max(0, after - cached_bytes_per_refill);
                if exceeded < bytes {
                    break bytes - exceeded;
                }
            }
            let now = monotonic_raw_now();
            let mut last_refill_time = self.last_refill_time.lock().unwrap();
            if (now - *last_refill_time).to_std().unwrap() >= self.refill_period {
                *last_refill_time = now;
                let token = std::cmp::min(cached_bytes_per_refill, bytes);
                self.consumed.store(token, Ordering::Relaxed);
                self.condv.notify_all();
                break token;
            }
            let cached_last_refill_time = *last_refill_time;
            cached_consumed = self.consumed.load(Ordering::Relaxed);
            if cached_consumed >= cached_bytes_per_refill {
                let mut last_refill_time = self
                    .condv
                    .async_wait_timeout(
                        &self.last_refill_time,
                        last_refill_time,
                        self.refill_period,
                    )
                    .await;
                if *last_refill_time == cached_last_refill_time {
                    *last_refill_time = monotonic_raw_now();
                    let token = std::cmp::min(cached_bytes_per_refill, bytes);
                    self.consumed.store(token, Ordering::Relaxed);
                    break token;
                }
            }
        }
    }
}

impl Default for PerTypeIORateLimiter {
    fn default() -> PerTypeIORateLimiter {
        PerTypeIORateLimiter::new(0, Duration::from_millis(10))
    }
}

/// No-op limiter
/// An instance of `IORateLimiter` should be safely shared between threads.
#[derive(Debug)]
pub struct IORateLimiter {
    write_limiters: [PerTypeIORateLimiter; IO_TYPE_VARIANTS],
    read_limiters: [PerTypeIORateLimiter; IO_TYPE_VARIANTS],
    total_limiters: [PerTypeIORateLimiter; IO_TYPE_VARIANTS],
}

impl IORateLimiter {
    pub fn new(_bytes_per_sec: usize) -> IORateLimiter {
        IORateLimiter {
            write_limiters: Default::default(),
            read_limiters: Default::default(),
            total_limiters: Default::default(),
        }
    }

    /// Request for token for bytes and potentially update statistics. If this
    /// request can not be satisfied, the call is blocked. Granted token can be
    /// less than the requested bytes, but must be greater than zero.
    pub fn request(&self, io_type: IOType, io_op: IOOp, bytes: usize) -> usize {
        let bytes = self.total_limiters[0].request(bytes, IOPriority::Limited);
        let bytes = self.total_limiters[io_type as usize].request(bytes, IOPriority::Limited);
        match io_op {
            IOOp::Write => {
                let bytes = self.write_limiters[0].request(bytes, IOPriority::Limited);
                self.write_limiters[io_type as usize].request(bytes, IOPriority::Limited)
            }
            IOOp::Read => {
                let bytes = self.read_limiters[0].request(bytes, IOPriority::Limited);
                self.read_limiters[io_type as usize].request(bytes, IOPriority::Limited)
            }
        }
    }

    pub async fn async_request(&self, io_type: IOType, io_op: IOOp, bytes: usize) -> usize {
        let bytes = self.total_limiters[0]
            .async_request(bytes, IOPriority::Limited)
            .await;
        let bytes = self.total_limiters[io_type as usize]
            .async_request(bytes, IOPriority::Limited)
            .await;
        match io_op {
            IOOp::Write => {
                let bytes = self.write_limiters[0]
                    .async_request(bytes, IOPriority::Limited)
                    .await;
                self.write_limiters[io_type as usize]
                    .async_request(bytes, IOPriority::Limited)
                    .await
            }
            IOOp::Read => {
                let bytes = self.read_limiters[0]
                    .async_request(bytes, IOPriority::Limited)
                    .await;
                self.read_limiters[io_type as usize]
                    .async_request(bytes, IOPriority::Limited)
                    .await
            }
        }
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

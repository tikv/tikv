// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use rocksdb::RateLimiter;
use engine_traits::{IOLimiterExt, IOLimiter};

const PRIORITY_HIGH: u8 = 1;
const REFILL_PERIOD: i64 = 100 * 1000;
const FARENESS: i32 = 10;
const SNAP_MAX_BYTES_PER_TIME: i64 = 4 * 1024 * 1024;

impl IOLimiterExt for RocksEngine {
    type IOLimiter = RocksIOLimiter;
}

pub struct RocksIOLimiter {
    inner: RateLimiter,
}

impl IOLimiter for RocksIOLimiter {
    /// # Arguments
    ///
    /// - `bytes_per_sec`: controls the total write rate of compaction and flush in bytes per second.
    fn new(bytes_per_sec: u64) -> Self {
        RocksIOLimiter {
            inner: RateLimiter::new(bytes_per_sec as i64, REFILL_PERIOD, FARENESS),
        }
    }

    /// Sets the rate limit in bytes per second
    fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        self.inner.set_bytes_per_second(bytes_per_sec)
    }

    /// Requests an access token to read or write bytes. If this request can not be satisfied, the call is blocked.
    fn request(&self, bytes: i64) {
        self.inner.request(bytes, PRIORITY_HIGH)
    }

    /// Gets the max bytes that can be granted in a single burst.
    /// Note: it will be less than or equal to `SNAP_MAX_BYTES_PER_TIME`.
    fn get_max_bytes_per_time(&self) -> i64 {
        if self.inner.get_singleburst_bytes() > SNAP_MAX_BYTES_PER_TIME {
            SNAP_MAX_BYTES_PER_TIME
        } else {
            self.inner.get_singleburst_bytes()
        }
    }

    /// Gets the total bytes that have gone through the rate limiter.
    fn get_total_bytes_through(&self) -> i64 {
        self.inner.get_total_bytes_through(PRIORITY_HIGH)
    }

    /// Gets the rate limit in bytes per second.
    fn get_bytes_per_second(&self) -> i64 {
        self.inner.get_bytes_per_second()
    }

    /// Gets the total number of requests that have gone through rate limiter
    fn get_total_requests(&self) -> i64 {
        self.inner.get_total_requests(PRIORITY_HIGH)
    }
}

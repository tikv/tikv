// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

/// An I/O rate limiter
///
/// Throttles the maximum bytes per second written or read.
pub trait IOLimiterExt {
    type IOLimiter: IOLimiter;
}

pub trait IOLimiter {
    /// # Arguments
    ///
    /// - `bytes_per_sec`: controls the total write rate of compaction and flush in bytes per second.
    fn new(bytes_per_sec: u64) -> Self;

    /// Sets the rate limit in bytes per second
    fn set_bytes_per_second(&self, bytes_per_sec: i64);

    /// Requests an access token to read or write bytes. If this request can not be satisfied, the call is blocked.
    fn request(&self, bytes: i64);

    /// Gets the max bytes that can be granted in a single burst.
    fn get_max_bytes_per_time(&self) -> i64;

    /// Gets the total bytes that have gone through the rate limiter.
    fn get_total_bytes_through(&self) -> i64;

    /// Gets the rate limit in bytes per second.
    fn get_bytes_per_second(&self) -> i64;

    /// Gets the total number of requests that have gone through rate limiter
    fn get_total_requests(&self) -> i64;
}

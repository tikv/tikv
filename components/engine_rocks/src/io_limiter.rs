// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine_traits::{IOLimiter, IOLimiterExt};
use rocksdb::RateLimiter;

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
    fn new(bytes_per_sec: i64) -> Self {
        RocksIOLimiter {
            inner: RateLimiter::new(bytes_per_sec, REFILL_PERIOD, FARENESS),
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

#[cfg(test)]
mod tests {
    use engine_traits::{LimitReader, LimitWriter};
    use std::fs::{self, File};
    use std::io::{Read, Write};
    use std::sync::Arc;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_io_limiter() {
        let limiter = RocksIOLimiter::new(10 * 1024 * 1024);
        assert!(limiter.get_max_bytes_per_time() <= SNAP_MAX_BYTES_PER_TIME);

        limiter.set_bytes_per_second(20 * 1024 * 1024);
        assert_eq!(limiter.get_bytes_per_second(), 20 * 1024 * 1024);

        assert_eq!(limiter.get_total_bytes_through(), 0);

        limiter.request(1024 * 1024);
        assert_eq!(limiter.get_total_bytes_through(), 1024 * 1024);

        assert_eq!(limiter.get_total_requests(), 1);
    }

    #[test]
    fn test_limit_writer() {
        let dir = Builder::new()
            .prefix("_test_limit_writer")
            .tempdir()
            .unwrap();
        let path = dir.path().join("test-file");
        let mut file = File::create(&path).unwrap();
        let mut limit_writer =
            LimitWriter::new(Some(Arc::new(RocksIOLimiter::new(1024))), &mut file);

        let mut s = String::new();
        for _ in 0..100 {
            s.push_str("Hello, World!");
        }
        limit_writer.write_all(s.as_bytes()).unwrap();
        limit_writer.flush().unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(contents, s);
    }

    #[test]
    fn test_limit_reader() {
        let mut buf = Vec::with_capacity(512);
        let bytes_per_sec = 10 * 1024 * 1024; // 10MB/s
        for c in 0..1024usize {
            let mut source = std::io::repeat(b'7').take(c as _);
            let mut limit_reader = LimitReader::new(
                Some(Arc::new(RocksIOLimiter::new(bytes_per_sec))),
                &mut source,
            );
            let count = limit_reader.read_to_end(&mut buf).unwrap();
            assert_eq!(count, c);
            assert_eq!(count, buf.len());
            buf.clear();
        }
    }
}

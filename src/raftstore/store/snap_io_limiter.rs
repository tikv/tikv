// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::io::{Result, Write};

use rocksdb::RateLimiter;

use server::config::{DEFAULT_SNAP_MAX_BYTES_PER_SEC, DEFAULT_SNAP_MAX_BYTES_PER_TIME,
                     DEFAULT_SNAP_MIN_BYTES_PER_TIME};

const REFILL_PERIOD: i64 = 100 * 1000;
const FARENESS: i32 = 10;

pub struct SnapshotIOLimiter {
    inner: RateLimiter,
    min_bytes_per_time: i64,
    max_bytes_per_time: i64,
}

impl SnapshotIOLimiter {
    pub fn new(
        min_bytes_per_time: u64,
        max_bytes_per_time: u64,
        bytes_per_sec: u64,
    ) -> SnapshotIOLimiter {
        SnapshotIOLimiter {
            inner: RateLimiter::new(bytes_per_sec as i64, REFILL_PERIOD, FARENESS),
            min_bytes_per_time: min_bytes_per_time as i64,
            max_bytes_per_time: max_bytes_per_time as i64,
        }
    }

    pub fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        self.inner.set_bytes_per_second(bytes_per_sec);
    }

    pub fn request(&self, bytes: i64) {
        self.inner.request(bytes, 1);
    }

    pub fn get_min_bytes_per_time(&self) -> i64 {
        self.min_bytes_per_time
    }

    pub fn get_max_bytes_per_time(&self) -> i64 {
        let single = self.inner.get_singleburst_bytes();
        if single > self.max_bytes_per_time {
            self.max_bytes_per_time
        } else {
            single
        }
    }

    pub fn get_total_bytes_through(&self) -> i64 {
        self.inner.get_total_bytes_through(1)
    }

    pub fn get_bytes_per_second(&self) -> i64 {
        self.inner.get_bytes_per_second()
    }

    pub fn get_total_requests(&self) -> i64 {
        self.inner.get_total_requests(1)
    }
}

impl Default for SnapshotIOLimiter {
    fn default() -> SnapshotIOLimiter {
        SnapshotIOLimiter {
            inner: RateLimiter::new(
                DEFAULT_SNAP_MAX_BYTES_PER_SEC as i64,
                REFILL_PERIOD,
                FARENESS,
            ),
            min_bytes_per_time: DEFAULT_SNAP_MIN_BYTES_PER_TIME as i64,
            max_bytes_per_time: DEFAULT_SNAP_MAX_BYTES_PER_TIME as i64,
        }
    }
}

pub struct LimiterWriter<'a, T: 'a>
where
    T: Write,
{
    pub limiter: Arc<SnapshotIOLimiter>,
    pub writer: &'a mut T,
}

impl<'a, T> Write for LimiterWriter<'a, T>
where
    T: Write,
{
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let total = buf.len();
        let single = self.limiter.get_max_bytes_per_time() as usize;
        let mut curr = 0;
        let mut end;
        while curr < total {
            if curr + single >= total {
                end = total;
                self.limiter.request((total - curr) as i64);
            } else {
                end = curr + single;
                self.limiter.request(single as i64);
            }
            self.writer.write_all(&buf[curr..end])?;
            curr = end;
        }
        Ok(total)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::{self, File};
    use std::io::{Read, Write};
    use std::sync::Arc;

    use server::config::{DEFAULT_SNAP_MAX_BYTES_PER_SEC, DEFAULT_SNAP_MAX_BYTES_PER_TIME,
                         DEFAULT_SNAP_MIN_BYTES_PER_TIME};

    use super::{LimiterWriter, SnapshotIOLimiter};

    #[test]
    fn test_default_snapshot_io_limiter() {
        let limiter = SnapshotIOLimiter::default();
        assert_eq!(
            limiter.get_min_bytes_per_time(),
            DEFAULT_SNAP_MIN_BYTES_PER_TIME as i64
        );
        assert!(limiter.get_max_bytes_per_time() <= DEFAULT_SNAP_MAX_BYTES_PER_TIME as i64);
        assert_eq!(
            limiter.get_bytes_per_second(),
            DEFAULT_SNAP_MAX_BYTES_PER_SEC as i64
        );
    }

    #[test]
    fn test_snapshot_io_limiter() {
        let limiter = SnapshotIOLimiter::new(64 * 1024, 1024 * 1024, 10 * 1024 * 1024);
        assert_eq!(limiter.get_min_bytes_per_time(), 64 * 1024);
        assert!(limiter.get_max_bytes_per_time() <= 1024 * 1024);

        limiter.set_bytes_per_second(20 * 1024 * 1024);
        assert_eq!(limiter.get_bytes_per_second(), 20 * 1024 * 1024);

        assert_eq!(limiter.get_total_bytes_through(), 0);

        limiter.request(1024 * 1024);
        assert_eq!(limiter.get_total_bytes_through(), 1024 * 1024);

        assert_eq!(limiter.get_total_requests(), 1);
    }

    #[test]
    fn test_limiter_writer() {
        let mut file = File::create("./test_limiter_writer.txt").unwrap();
        let mut limiter_writer = LimiterWriter {
            limiter: Arc::new(SnapshotIOLimiter::default()),
            writer: &mut file,
        };
        limiter_writer.write_all(b"Hello, World!").unwrap();
        limiter_writer.flush().unwrap();

        let mut file = File::open("./test_limiter_writer.txt").unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "Hello, World!");
        fs::remove_file("./test_limiter_writer.txt").unwrap();
    }
}

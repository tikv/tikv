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
use std::option::Option;
use std::io::{Result, Write};

use rocksdb::RateLimiter;

const REQUEST_PRIORITY: u8 = 1;
const REFILL_PERIOD: i64 = 100 * 1000;
const FARENESS: i32 = 10;
pub const DEFAULT_SNAP_MIN_BYTES_PER_TIME: u64 = 128 * 1024;
pub const DEFAULT_SNAP_MAX_BYTES_PER_TIME: u64 = 2 * 1024 * 1024;
pub const DEFAULT_SNAP_MAX_BYTES_PER_SEC: u64 = 20 * 1024 * 1024;

pub struct IOLimiter {
    inner: Option<RateLimiter>,
    min_bytes_per_time: i64,
    max_bytes_per_time: i64,
}

impl IOLimiter {
    pub fn new(min_bytes_per_time: u64, max_bytes_per_time: u64, bytes_per_sec: u64) -> IOLimiter {
        let limiter = if bytes_per_sec > 0 {
            Some(RateLimiter::new(
                bytes_per_sec as i64,
                REFILL_PERIOD,
                FARENESS,
            ))
        } else {
            None
        };
        IOLimiter {
            inner: limiter,
            min_bytes_per_time: min_bytes_per_time as i64,
            max_bytes_per_time: max_bytes_per_time as i64,
        }
    }

    pub fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        if let Some(ref limiter) = self.inner {
            limiter.set_bytes_per_second(bytes_per_sec)
        }
    }

    pub fn request(&self, bytes: i64) {
        if let Some(ref limiter) = self.inner {
            limiter.request(bytes, REQUEST_PRIORITY)
        }
    }

    pub fn get_min_bytes_per_time(&self) -> i64 {
        match self.inner {
            Some(_) => self.min_bytes_per_time,
            None => 0,
        }
    }

    pub fn get_max_bytes_per_time(&self) -> i64 {
        match self.inner {
            Some(ref limiter) => if limiter.get_singleburst_bytes() > self.max_bytes_per_time {
                self.max_bytes_per_time
            } else {
                limiter.get_singleburst_bytes()
            },
            None => 0,
        }
    }

    pub fn get_total_bytes_through(&self) -> i64 {
        match self.inner {
            Some(ref limiter) => limiter.get_total_bytes_through(1),
            None => 0,
        }
    }

    pub fn get_bytes_per_second(&self) -> i64 {
        match self.inner {
            Some(ref limiter) => limiter.get_bytes_per_second(),
            None => 0,
        }
    }

    pub fn get_total_requests(&self) -> i64 {
        match self.inner {
            Some(ref limiter) => limiter.get_total_requests(1),
            None => 0,
        }
    }
}

impl Default for IOLimiter {
    fn default() -> IOLimiter {
        IOLimiter::new(
            DEFAULT_SNAP_MIN_BYTES_PER_TIME,
            DEFAULT_SNAP_MAX_BYTES_PER_TIME,
            DEFAULT_SNAP_MAX_BYTES_PER_SEC,
        )
    }
}

pub struct LimitWriter<'a, T: Write + 'a> {
    pub limiter: Arc<IOLimiter>,
    pub writer: &'a mut T,
}

impl<'a, T: Write + 'a> Write for LimitWriter<'a, T> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let total = buf.len();
        let single = self.limiter.get_max_bytes_per_time() as usize;
        if single > 0 {
            let mut curr = 0;
            let mut end;
            while curr < total {
                if curr + single >= total {
                    end = total;
                } else {
                    end = curr + single;
                }
                self.limiter.request((end - curr) as i64);
                self.writer.write_all(&buf[curr..end])?;
                curr = end;
            }
        } else {
            self.writer.write_all(buf)?;
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

    use super::{IOLimiter, LimitWriter, DEFAULT_SNAP_MAX_BYTES_PER_SEC,
                DEFAULT_SNAP_MAX_BYTES_PER_TIME, DEFAULT_SNAP_MIN_BYTES_PER_TIME};

    #[test]
    fn test_default_io_limiter() {
        let limiter = IOLimiter::default();
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
    fn test_io_limiter() {
        let limiter = IOLimiter::new(64 * 1024, 1024 * 1024, 10 * 1024 * 1024);
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
    fn test_disabled_io_limiter() {
        let limiter = IOLimiter::new(1024, 1024 * 1024, 0);
        assert_eq!(limiter.get_min_bytes_per_time(), 0);
        assert_eq!(limiter.get_max_bytes_per_time(), 0);

        assert_eq!(limiter.get_bytes_per_second(), 0);

        limiter.request(1024 * 1024);
        assert_eq!(limiter.get_total_bytes_through(), 0);

        assert_eq!(limiter.get_total_requests(), 0);
    }

    #[test]
    fn test_limit_writer() {
        let mut file = File::create("./test_limiter_writer.txt").unwrap();
        let mut limit_writer = LimitWriter {
            limiter: Arc::new(IOLimiter::default()),
            writer: &mut file,
        };
        limit_writer.write_all(b"Hello, World!").unwrap();
        limit_writer.flush().unwrap();

        let mut file = File::open("./test_limiter_writer.txt").unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "Hello, World!");
        fs::remove_file("./test_limiter_writer.txt").unwrap();
    }
}

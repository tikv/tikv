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

use rocksdb::RateLimiter;

const PRIORITY_HIGH: u8 = 1;
const REFILL_PERIOD: i64 = 100 * 1000;
const FARENESS: i32 = 10;
const SNAP_MAX_BYTES_PER_TIME: i64 = 4 * 1024 * 1024;
pub const DEFAULT_SNAP_MAX_BYTES_PER_SEC: u64 = 30 * 1024 * 1024;

pub struct IOLimiter {
    inner: RateLimiter,
}

impl IOLimiter {
    pub fn new(bytes_per_sec: u64) -> IOLimiter {
        IOLimiter {
            inner: RateLimiter::new(bytes_per_sec as i64, REFILL_PERIOD, FARENESS),
        }
    }

    pub fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        self.inner.set_bytes_per_second(bytes_per_sec)
    }

    pub fn request(&self, bytes: i64) {
        self.inner.request(bytes, PRIORITY_HIGH)
    }

    pub fn get_max_bytes_per_time(&self) -> i64 {
        if self.inner.get_singleburst_bytes() > SNAP_MAX_BYTES_PER_TIME {
            SNAP_MAX_BYTES_PER_TIME
        } else {
            self.inner.get_singleburst_bytes()
        }
    }

    pub fn get_total_bytes_through(&self) -> i64 {
        self.inner.get_total_bytes_through(PRIORITY_HIGH)
    }

    pub fn get_bytes_per_second(&self) -> i64 {
        self.inner.get_bytes_per_second()
    }

    pub fn get_total_requests(&self) -> i64 {
        self.inner.get_total_requests(PRIORITY_HIGH)
    }
}

#[cfg(test)]
mod test {
    use super::{IOLimiter, DEFAULT_SNAP_MAX_BYTES_PER_SEC, SNAP_MAX_BYTES_PER_TIME};

    #[test]
    fn test_io_limiter() {
        let limiter = IOLimiter::new(10 * 1024 * 1024);
        assert!(limiter.get_max_bytes_per_time() <= SNAP_MAX_BYTES_PER_TIME);

        limiter.set_bytes_per_second(20 * 1024 * 1024);
        assert_eq!(limiter.get_bytes_per_second(), 20 * 1024 * 1024);

        assert_eq!(limiter.get_total_bytes_through(), 0);

        limiter.request(1024 * 1024);
        assert_eq!(limiter.get_total_bytes_through(), 1024 * 1024);

        assert_eq!(limiter.get_total_requests(), 1);
    }
}

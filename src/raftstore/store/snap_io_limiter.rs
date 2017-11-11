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

use util::io_limiter::IOLimiter;

use rocksdb::RateLimiter;

pub struct SnapshotIOLimiter {
    inner: RateLimiter,
    max_bytes_per_time: i64,
}

impl IOLimiter for SnapshotIOLimiter {
    fn new(bytes_per_time: i64, bytes_per_sec: i64) -> SnapshotIOLimiter {
        SnapshotIOLimiter {
            inner: RateLimiter::new(bytes_per_sec, 100 * 1000, 10),
            max_bytes_per_time: bytes_per_time,
        }
    }

    fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        self.inner.set_bytes_per_second(bytes_per_sec);
    }

    fn request(&self, bytes: i64) {
        self.inner.request(bytes, 1);
    }

    fn get_singleburst_bytes(&self) -> i64 {
        let single = self.inner.get_singleburst_bytes();
        if single > self.max_bytes_per_time {
            self.max_bytes_per_time
        } else {
            single
        }
    }

    fn get_total_bytes_through(&self) -> i64 {
        self.inner.get_total_bytes_through(1)
    }

    fn get_bytes_per_second(&self) -> i64 {
        self.inner.get_bytes_per_second()
    }

    fn get_total_requests(&self) -> i64 {
        self.inner.get_total_requests(1)
    }
}

#[cfg(test)]
mod test {
    use util::io_limiter::IOLimiter;
    use super::SnapshotIOLimiter;

    #[test]
    fn test_snapshot_io_limiter() {
        let limiter = SnapshotIOLimiter::new(10 * 1024 * 1024, 1024 * 1024);
        assert_eq!(limiter.get_singleburst_bytes(), 1024 * 1024);

        limiter.set_bytes_per_second(20 * 1024 * 1024);
        assert_eq!(limiter.get_bytes_per_second(), 20 * 1024 * 1024);

        assert_eq!(limiter.get_total_bytes_through(), 0);

        limiter.request(1024 * 1024);
        assert_eq!(limiter.get_total_bytes_through(), 1024 * 1024);

        assert_eq!(limiter.get_total_requests(), 1);
    }
}

// Copyright 2016 PingCAP, Inc.
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

// `raw_now` returns the monotonic time since some unspecified starting point.
pub use self::inner::raw_now;

#[cfg(not(target_os = "linux"))]
mod inner {
    use time::{self, Timespec};

    const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;

    pub fn raw_now() -> Timespec {
        // TODO Add monotonic raw clock time impl for macos and windows
        // Currently use `time::get_precise_ns()` instead.
        let ns = time::precise_time_ns();
        let s = ns / NANOSECONDS_PER_SECOND;
        let ns = ns % NANOSECONDS_PER_SECOND;
        Timespec::new(s as i64, ns as i32)
    }
}

#[cfg(target_os = "linux")]
mod inner {
    use std::io;
    use time::Timespec;
    use libc;

    pub fn raw_now() -> Timespec {
        let mut t = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let res = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut t) };
        if res != 0 {
            panic!("failed to get monotonic raw locktime, err {}",
                   io::Error::last_os_error());
        }
        Timespec::new(t.tv_sec, t.tv_nsec as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_monotonic_raw() {
        let early_time = raw_now();
        let late_time = raw_now();
        // The monotonic raw clocktime must be strictly monotonic increasing.
        assert!(late_time > early_time);
    }
}

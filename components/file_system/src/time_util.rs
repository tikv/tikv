// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

pub use inner::{monotonic_now, monotonic_raw_now};

#[cfg(not(target_os = "linux"))]
mod inner {
    const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;
    use time::{self, Timespec};

    #[inline]
    #[allow(dead_code)]
    pub fn monotonic_raw_now() -> Timespec {
        // TODO Add monotonic raw clock time impl for macos and windows
        // Currently use `time::get_precise_ns()` instead.
        let ns = time::precise_time_ns();
        let s = ns / NANOSECONDS_PER_SECOND;
        let ns = ns % NANOSECONDS_PER_SECOND;
        Timespec::new(s as i64, ns as i32)
    }

    #[inline]
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn monotonic_raw_now() -> Timespec {
        get_time(libc::CLOCK_MONOTONIC_RAW)
    }

    #[inline]
    #[allow(dead_code)]
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

pub fn checked_sub(a: time::Timespec, b: time::Timespec) -> Duration {
    if a >= b {
        (a - b).to_std().unwrap()
    } else {
        Duration::from_secs(0)
    }
}

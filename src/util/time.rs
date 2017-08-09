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

use std::time::{SystemTime, Duration};
use std::thread::{self, JoinHandle, Builder};
use std::sync::mpsc::{self, Sender};
use std::ops::{Add, Sub, AddAssign, SubAssign};

use time::{Timespec, Duration as TimeDuration};

/// Convert Duration to milliseconds.
#[inline]
pub fn duration_to_ms(d: Duration) -> u64 {
    let nanos = d.subsec_nanos() as u64;
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() * 1_000 + (nanos / 1_000_000)
}

/// Convert Duration to seconds.
#[inline]
pub fn duration_to_sec(d: Duration) -> f64 {
    let nanos = d.subsec_nanos() as f64;
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}

/// Convert Duration to nanoseconds.
#[inline]
pub fn duration_to_nanos(d: Duration) -> u64 {
    let nanos = d.subsec_nanos() as u64;
    // Most of case, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() * 1_000_000_000 + nanos
}

pub struct SlowTimer {
    slow_time: Duration,
    t: Instant,
}

impl SlowTimer {
    pub fn new() -> SlowTimer {
        SlowTimer::default()
    }

    pub fn from(slow_time: Duration) -> SlowTimer {
        SlowTimer {
            slow_time: slow_time,
            t: Instant::now(),
        }
    }

    pub fn from_secs(secs: u64) -> SlowTimer {
        SlowTimer::from(Duration::from_secs(secs))
    }

    pub fn from_millis(millis: u64) -> SlowTimer {
        SlowTimer::from(Duration::from_millis(millis))
    }

    pub fn elapsed(&self) -> Duration {
        self.t.elapsed()
    }

    pub fn is_slow(&self) -> bool {
        self.elapsed() >= self.slow_time
    }
}

const DEFAULT_SLOW_SECS: u64 = 1;

impl Default for SlowTimer {
    fn default() -> SlowTimer {
        SlowTimer::from_secs(DEFAULT_SLOW_SECS)
    }
}

const DEFAULT_WAIT_MS: u64 = 100;

pub struct Monitor {
    tx: Sender<bool>,
    handle: Option<JoinHandle<()>>,
}

impl Monitor {
    pub fn new<D, N>(on_jumped: D, now: N) -> Monitor
        where D: Fn() + Send + 'static,
              N: Fn() -> SystemTime + Send + 'static
    {
        let (tx, rx) = mpsc::channel();
        let h = Builder::new()
            .name(thd_name!("time-monitor-worker"))
            .spawn(move || {
                while let Err(_) = rx.try_recv() {
                    let before = now();
                    thread::sleep(Duration::from_millis(DEFAULT_WAIT_MS));

                    let after = now();
                    if let Err(e) = after.duration_since(before) {
                        error!("system time jumped back, {:?} -> {:?}, err {:?}",
                               before,
                               after,
                               e);
                        on_jumped()
                    }
                }
            })
            .unwrap();

        Monitor {
            tx: tx,
            handle: Some(h),
        }
    }
}

impl Default for Monitor {
    fn default() -> Monitor {
        Monitor::new(|| {}, SystemTime::now)
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }

        if let Err(e) = self.tx.send(true) {
            error!("send quit message for time monitor worker failed {:?}", e);
            return;
        }

        if let Err(e) = h.unwrap().join() {
            error!("join time monitor worker failed {:?}", e);
            return;
        }
    }
}

fn elapsed_duration(later: Timespec, earlier: Timespec) -> Duration {
    if later >= earlier {
        Duration::new((later.sec - earlier.sec) as u64,
                      (later.nsec - earlier.nsec) as u32)
    } else {
        panic!("system time jumped back, {:.9} -> {:.9}",
               earlier.sec as f64 + earlier.nsec as f64 / NANOSECONDS_PER_SECOND as f64,
               later.sec as f64 + later.nsec as f64 / NANOSECONDS_PER_SECOND as f64);
    }
}

/// `raw_now` returns the monotonic time since some unspecified starting point.
pub use self::inner::raw_now;
use self::inner::coarse_now;

const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;

#[cfg(not(target_os = "linux"))]
mod inner {
    use time::{self, Timespec};

    pub fn raw_now() -> Timespec {
        // TODO Add monotonic raw clock time impl for macos and windows
        // Currently use `time::get_precise_ns()` instead.
        let ns = time::precise_time_ns();
        let s = ns / NANOSECONDS_PER_SECOND;
        let ns = ns % NANOSECONDS_PER_SECOND;
        Timespec::new(s as i64, ns as i32)
    }

    pub fn coarse_now() -> Timespec {
        // TODO Add monotonic coarse clock time impl for macos and windows
        raw_now()
    }
}

#[cfg(target_os = "linux")]
mod inner {
    use std::io;
    use time::Timespec;
    use libc;

    pub fn raw_now() -> Timespec {
        get_time(libc::CLOCK_MONOTONIC_RAW as _)
    }

    pub fn coarse_now() -> Timespec {
        get_time(libc::CLOCK_MONOTONIC_COARSE as _)
    }

    fn get_time(clock: libc::clock_t) -> Timespec {
        let mut t = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let errno = unsafe { libc::clock_gettime(clock as _, &mut t) };
        if errno != 0 {
            panic!("failed to get monotonic raw locktime, err {}",
                   io::Error::last_os_error());
        }
        Timespec::new(t.tv_sec, t.tv_nsec as _)
    }
}

/// A measurement of a monotonically increasing clock.
/// It's similar and meat to replace `std::time::Instant`,
/// for providing extra features.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Instant {
    Monotonic(Timespec),
    MonotonicCoarse(Timespec),
}

impl Instant {
    pub fn now() -> Instant {
        Instant::Monotonic(raw_now())
    }

    pub fn now_coarse() -> Instant {
        Instant::MonotonicCoarse(coarse_now())
    }

    pub fn elapsed(&self) -> Duration {
        match *self {
            Instant::Monotonic(t) => {
                let now = raw_now();
                elapsed_duration(now, t)
            }
            Instant::MonotonicCoarse(t) => {
                let now = coarse_now();
                elapsed_duration(now, t)
            }
        }
    }

    pub fn duration_since(&self, earlier: Instant) -> Duration {
        let later = match *self {
            Instant::Monotonic(t) |
            Instant::MonotonicCoarse(t) => t,
        };
        let earlier = match earlier {
            Instant::Monotonic(t) |
            Instant::MonotonicCoarse(t) => t,
        };
        elapsed_duration(later, earlier)
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, other: Duration) -> Instant {
        match self {
            Instant::Monotonic(t) => Instant::Monotonic(t + TimeDuration::from_std(other).unwrap()),
            Instant::MonotonicCoarse(t) => {
                Instant::MonotonicCoarse(t + TimeDuration::from_std(other).unwrap())
            }
        }
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, other: Duration) {
        *self = *self + other;
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;

    fn sub(self, other: Duration) -> Instant {
        match self {
            Instant::Monotonic(t) => Instant::Monotonic(t - TimeDuration::from_std(other).unwrap()),
            Instant::MonotonicCoarse(t) => {
                Instant::MonotonicCoarse(t - TimeDuration::from_std(other).unwrap())
            }
        }
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, other: Duration) {
        *self = *self - other;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, other: Instant) -> Duration {
        self.duration_since(other)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, Duration};
    use std::thread;
    use std::ops::Sub;
    use std::f64;
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_time_monitor() {
        let jumped = Arc::new(AtomicBool::new(false));
        let triggered = AtomicBool::new(false);
        let now = move || {
            if !triggered.load(Ordering::SeqCst) {
                triggered.store(true, Ordering::SeqCst);
                SystemTime::now()
            } else {
                SystemTime::now().sub(Duration::from_secs(2))
            }
        };

        let jumped2 = jumped.clone();
        let on_jumped = move || {
            jumped2.store(true, Ordering::SeqCst);
        };

        let _m = Monitor::new(on_jumped, now);
        thread::sleep(Duration::from_secs(1));

        assert_eq!(jumped.load(Ordering::SeqCst), true);
    }

    #[test]
    fn test_duration_to() {
        let tbl = vec![0, 100, 1_000, 5_000, 9999, 1_000_000, 1_000_000_000];
        for ms in tbl {
            let d = Duration::from_millis(ms);
            assert_eq!(ms, duration_to_ms(d));
            let exp_sec = ms as f64 / 1000.0;
            let act_sec = duration_to_sec(d);
            assert!((act_sec - exp_sec).abs() < f64::EPSILON);
            assert_eq!(ms * 1_000_000, duration_to_nanos(d));
        }
    }

    #[test]
    fn test_now() {
        let early_time = raw_now();
        let late_time = raw_now();
        // The monotonic raw clocktime must be strictly monotonic increasing.
        assert!(late_time >= early_time,
                "expect late time {:?} >= early time {:?}",
                late_time,
                early_time);

        let early_time = coarse_now();
        let late_time = coarse_now();
        // The monotonic coarse clocktime must be strictly monotonic increasing.
        assert!(late_time >= early_time,
                "expect late time {:?} >= early time {:?}",
                late_time,
                early_time);
    }

    #[test]
    fn test_instant() {
        Instant::now().elapsed();
        let early_raw = Instant::now();
        let late_raw = Instant::now();
        assert!(late_raw >= early_raw,
                "expect late time {:?} >= early time {:?}",
                late_raw,
                early_raw);
        assert_eq!(early_raw, early_raw);
        assert!(early_raw <= late_raw);

        Instant::now_coarse().elapsed();
        let early_coarse = Instant::now_coarse();
        let late_coarse = Instant::now_coarse();
        assert!(late_coarse >= early_coarse,
                "expect late time {:?} >= early time {:?}",
                late_coarse,
                early_coarse);
        assert_eq!(early_coarse, early_coarse);
        assert!(early_coarse <= late_coarse);

        assert!(early_raw <= late_coarse);

        let zero = Duration::new(0, 0);
        // Sub Instant.
        assert!(late_raw - early_raw > zero);
        assert!(late_coarse - early_coarse >= zero);
        assert!(late_coarse - early_raw >= zero);

        // Sub Duration.
        assert_eq!(late_raw - zero, late_raw);
        assert_eq!(late_coarse - zero, late_coarse);

        // Add Duration.
        assert_eq!(late_raw + zero, late_raw);
        assert_eq!(late_coarse + zero, late_coarse);

        // SubAssign Duration
        let mut early_raw_ = early_raw;
        early_raw_ -= zero;

        // AddAssign Duration
        let mut late_raw_ = late_raw;
        late_raw_ += zero;
        assert!(late_raw_ > early_raw_);
    }
}

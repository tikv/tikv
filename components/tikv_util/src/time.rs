// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// Re-export duration.
pub use std::time::Duration;
use std::{
    cell::RefCell,
    cmp::Ordering,
    ops::{Add, AddAssign, Sub, SubAssign},
    sync::mpsc::{self, Sender},
    thread::{self, Builder, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
};

use async_speed_limit::clock::{BlockingClock, Clock, StandardClock};
use time::{Duration as TimeDuration, Timespec};

/// Converts Duration to milliseconds.
#[inline]
pub fn duration_to_ms(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    // If Duration is too large, the result may be overflow.
    d.as_secs() * 1_000 + (nanos / 1_000_000)
}

/// Converts Duration to seconds.
#[inline]
pub fn duration_to_sec(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos());
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}

/// Converts Duration to microseconds.
#[inline]
pub fn duration_to_us(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    // If Duration is too large, the result may be overflow.
    d.as_secs() * 1_000_000 + (nanos / 1_000)
}

/// Converts TimeSpec to nanoseconds
#[inline]
pub fn timespec_to_ns(t: Timespec) -> u64 {
    (t.sec as u64) * NANOSECONDS_PER_SECOND + t.nsec as u64
}

/// Converts Duration to nanoseconds.
#[inline]
pub fn duration_to_ns(d: Duration) -> u64 {
    let nanos = u64::from(d.subsec_nanos());
    // If Duration is too large, the result may be overflow.
    d.as_secs() * 1_000_000_000 + nanos
}

pub trait InstantExt {
    fn saturating_elapsed(&self) -> Duration;
}

impl InstantExt for std::time::Instant {
    #[inline]
    fn saturating_elapsed(&self) -> Duration {
        std::time::Instant::now().saturating_duration_since(*self)
    }
}

/// A time in seconds since the start of the Unix epoch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UnixSecs(u64);

impl UnixSecs {
    pub fn now() -> UnixSecs {
        UnixSecs(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
    }

    pub fn zero() -> UnixSecs {
        UnixSecs(0)
    }

    pub fn into_inner(self) -> u64 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
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
            slow_time,
            t: Instant::now_coarse(),
        }
    }

    pub fn from_secs(secs: u64) -> SlowTimer {
        SlowTimer::from(Duration::from_secs(secs))
    }

    pub fn from_millis(millis: u64) -> SlowTimer {
        SlowTimer::from(Duration::from_millis(millis))
    }

    pub fn saturating_elapsed(&self) -> Duration {
        self.t.saturating_elapsed()
    }

    pub fn is_slow(&self) -> bool {
        self.saturating_elapsed() >= self.slow_time
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
    where
        D: Fn() + Send + 'static,
        N: Fn() -> SystemTime + Send + 'static,
    {
        let props = crate::thread_group::current_properties();
        let (tx, rx) = mpsc::channel();
        let h = Builder::new()
            .name(thd_name!("time-monitor"))
            .spawn_wrapper(move || {
                crate::thread_group::set_properties(props);
                tikv_alloc::add_thread_memory_accessor();
                while rx.try_recv().is_err() {
                    let before = now();
                    thread::sleep(Duration::from_millis(DEFAULT_WAIT_MS));

                    let after = now();
                    if let Err(e) = after.duration_since(before) {
                        error!(
                            "system time jumped back";
                            "before" => ?before,
                            "after" => ?after,
                            "err" => ?e,
                        );
                        on_jumped()
                    }
                }
                tikv_alloc::remove_thread_memory_accessor();
            })
            .unwrap();

        Monitor {
            tx,
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
            error!("send quit message for time monitor worker failed"; "err" => ?e);
            return;
        }

        if let Err(e) = h.unwrap().join() {
            error!("join time monitor worker failed"; "err" => ?e);
        }
    }
}

use self::inner::monotonic_coarse_now;
pub use self::inner::monotonic_now;
/// Returns the monotonic raw time since some unspecified starting point.
pub use self::inner::monotonic_raw_now;
use crate::sys::thread::StdThreadBuildWrapper;

const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;
const MILLISECOND_PER_SECOND: i64 = 1_000;
const NANOSECONDS_PER_MILLISECOND: i64 = 1_000_000;

#[cfg(not(target_os = "linux"))]
mod inner {
    use time::{self, Timespec};

    use super::NANOSECONDS_PER_SECOND;

    pub fn monotonic_raw_now() -> Timespec {
        // TODO Add monotonic raw clock time impl for macos and windows
        // Currently use `time::get_precise_ns()` instead.
        let ns = time::precise_time_ns();
        let s = ns / NANOSECONDS_PER_SECOND;
        let ns = ns % NANOSECONDS_PER_SECOND;
        Timespec::new(s as i64, ns as i32)
    }

    pub fn monotonic_now() -> Timespec {
        // TODO Add monotonic clock time impl for macos and windows
        monotonic_raw_now()
    }

    pub fn monotonic_coarse_now() -> Timespec {
        // TODO Add monotonic coarse clock time impl for macos and windows
        monotonic_raw_now()
    }
}

#[cfg(target_os = "linux")]
mod inner {
    use std::io;

    use time::Timespec;

    #[inline]
    pub fn monotonic_raw_now() -> Timespec {
        get_time(libc::CLOCK_MONOTONIC_RAW)
    }

    #[inline]
    pub fn monotonic_now() -> Timespec {
        get_time(libc::CLOCK_MONOTONIC)
    }

    #[inline]
    pub fn monotonic_coarse_now() -> Timespec {
        get_time(libc::CLOCK_MONOTONIC_COARSE)
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

/// A measurement of a monotonically increasing clock.
/// It's similar and meant to replace `std::time::Instant`,
/// for providing extra features.
#[derive(Copy, Clone, Debug)]
pub enum Instant {
    Monotonic(Timespec),
    MonotonicCoarse(Timespec),
}

impl Instant {
    pub fn now() -> Instant {
        Instant::Monotonic(monotonic_now())
    }

    pub fn now_coarse() -> Instant {
        Instant::MonotonicCoarse(monotonic_coarse_now())
    }

    pub fn saturating_elapsed(&self) -> Duration {
        match *self {
            Instant::Monotonic(t) => {
                let now = monotonic_now();
                Instant::saturating_elapsed_duration(now, t)
            }
            Instant::MonotonicCoarse(t) => {
                let now = monotonic_coarse_now();
                Instant::saturating_elapsed_duration_coarse(now, t)
            }
        }
    }

    // This function may panic if the current time is earlier than this
    // instant. Deprecated.
    // pub fn elapsed_secs(&self) -> f64;

    pub fn saturating_elapsed_secs(&self) -> f64 {
        duration_to_sec(self.saturating_elapsed())
    }

    pub fn duration_since(&self, earlier: Instant) -> Duration {
        match (*self, earlier) {
            (Instant::Monotonic(later), Instant::Monotonic(earlier)) => {
                Instant::elapsed_duration(later, earlier)
            }
            (Instant::MonotonicCoarse(later), Instant::MonotonicCoarse(earlier)) => {
                Instant::saturating_elapsed_duration_coarse(later, earlier)
            }
            _ => {
                panic!("duration between different types of Instants");
            }
        }
    }

    pub fn saturating_duration_since(&self, earlier: Instant) -> Duration {
        match (*self, earlier) {
            (Instant::Monotonic(later), Instant::Monotonic(earlier)) => {
                Instant::saturating_elapsed_duration(later, earlier)
            }
            (Instant::MonotonicCoarse(later), Instant::MonotonicCoarse(earlier)) => {
                Instant::saturating_elapsed_duration_coarse(later, earlier)
            }
            _ => {
                panic!("duration between different types of Instants");
            }
        }
    }

    /// It is similar to `duration_since`, but it won't panic when `self` is less than `other`,
    /// and `None` will be returned in this case.
    ///
    /// Callers need to ensure that `self` and `other` are same type of Instants.
    pub fn checked_sub(&self, other: Instant) -> Option<Duration> {
        if *self >= other {
            Some(self.duration_since(other))
        } else {
            None
        }
    }

    pub(crate) fn elapsed_duration(later: Timespec, earlier: Timespec) -> Duration {
        if later >= earlier {
            (later - earlier).to_std().unwrap()
        } else {
            panic!(
                "monotonic time jumped back, {:.9} -> {:.9}",
                earlier.sec as f64 + f64::from(earlier.nsec) / NANOSECONDS_PER_SECOND as f64,
                later.sec as f64 + f64::from(later.nsec) / NANOSECONDS_PER_SECOND as f64
            );
        }
    }

    pub(crate) fn saturating_elapsed_duration(later: Timespec, earlier: Timespec) -> Duration {
        if later >= earlier {
            (later - earlier).to_std().unwrap()
        } else {
            error!(
                "monotonic time jumped back, {:.3} -> {:.3}",
                earlier.sec as f64 + f64::from(earlier.nsec) / NANOSECONDS_PER_SECOND as f64,
                later.sec as f64 + f64::from(later.nsec) / NANOSECONDS_PER_SECOND as f64
            );
            Duration::from_millis(0)
        }
    }

    // It is different from `elapsed_duration`, the resolution here is millisecond.
    // The processors in an SMP system do not start all at exactly the same time
    // and therefore the timer registers are typically running at an offset.
    // Use millisecond resolution for ignoring the error.
    // See more: https://linux.die.net/man/2/clock_gettime
    pub(crate) fn saturating_elapsed_duration_coarse(
        later: Timespec,
        earlier: Timespec,
    ) -> Duration {
        let later_ms = later.sec * MILLISECOND_PER_SECOND
            + i64::from(later.nsec) / NANOSECONDS_PER_MILLISECOND;
        let earlier_ms = earlier.sec * MILLISECOND_PER_SECOND
            + i64::from(earlier.nsec) / NANOSECONDS_PER_MILLISECOND;
        let dur = later_ms - earlier_ms;
        if dur >= 0 {
            Duration::from_millis(dur as u64)
        } else {
            debug!(
                "coarse time jumped back, {:.3} -> {:.3}",
                earlier.sec as f64 + f64::from(earlier.nsec) / NANOSECONDS_PER_SECOND as f64,
                later.sec as f64 + f64::from(later.nsec) / NANOSECONDS_PER_SECOND as f64
            );
            Duration::from_millis(0)
        }
    }
}

impl PartialEq for Instant {
    fn eq(&self, other: &Instant) -> bool {
        match (*self, *other) {
            (Instant::Monotonic(this), Instant::Monotonic(other))
            | (Instant::MonotonicCoarse(this), Instant::MonotonicCoarse(other)) => this.eq(&other),
            _ => false,
        }
    }
}

impl PartialOrd for Instant {
    fn partial_cmp(&self, other: &Instant) -> Option<Ordering> {
        match (*self, *other) {
            (Instant::Monotonic(this), Instant::Monotonic(other))
            | (Instant::MonotonicCoarse(this), Instant::MonotonicCoarse(other)) => {
                this.partial_cmp(&other)
            }
            // The Order of different types of Instants is meaningless.
            _ => None,
        }
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
    fn add_assign(&mut self, rhs: Duration) {
        *self = self.add(rhs)
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
    fn sub_assign(&mut self, rhs: Duration) {
        *self = self.sub(rhs)
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    // TODO: For safety in production code, `sub` actually does saturating_sub.
    // We should remove this operator from public scope.
    fn sub(self, other: Instant) -> Duration {
        self.saturating_duration_since(other)
    }
}

/// A coarse clock for `async_speed_limit`.
#[derive(Copy, Clone, Default, Debug)]
pub struct CoarseClock;

impl Clock for CoarseClock {
    type Instant = Instant;
    type Delay = <StandardClock as Clock>::Delay;

    fn now(&self) -> Self::Instant {
        Instant::now_coarse()
    }

    fn sleep(&self, dur: Duration) -> Self::Delay {
        StandardClock.sleep(dur)
    }
}

impl BlockingClock for CoarseClock {
    fn blocking_sleep(&self, dur: Duration) {
        StandardClock.blocking_sleep(dur);
    }
}

/// A limiter which uses the coarse clock for measurement.
pub type Limiter = async_speed_limit::Limiter<CoarseClock>;
pub type Consume = async_speed_limit::limiter::Consume<CoarseClock, ()>;

/// ReadId to judge whether the read requests come from the same GRPC stream.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct ThreadReadId {
    sequence: u64,
    pub create_time: Timespec,
}

thread_local!(static READ_SEQUENCE: RefCell<u64> = RefCell::new(0));

impl ThreadReadId {
    pub fn new() -> ThreadReadId {
        let sequence = READ_SEQUENCE.with(|s| {
            let seq = *s.borrow() + 1;
            *s.borrow_mut() = seq;
            seq
        });
        ThreadReadId {
            sequence,
            create_time: monotonic_raw_now(),
        }
    }
}

impl Default for ThreadReadId {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ops::Sub,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::{Duration, SystemTime},
    };

    use super::*;

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

        let jumped2 = Arc::clone(&jumped);
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
            assert_eq!(ms * 1_000, duration_to_us(d));
            assert_eq!(ms * 1_000_000, duration_to_ns(d));
        }
    }

    #[test]
    fn test_now() {
        let pairs = vec![
            (monotonic_raw_now(), monotonic_raw_now()),
            (monotonic_now(), monotonic_now()),
            (monotonic_coarse_now(), monotonic_coarse_now()),
        ];
        for (early_time, late_time) in pairs {
            // The monotonic clocktime must be strictly monotonic increasing.
            assert!(
                late_time >= early_time,
                "expect late time {:?} >= early time {:?}",
                late_time,
                early_time
            );
        }
    }

    #[test]
    #[allow(clippy::eq_op)]
    fn test_instant() {
        Instant::now().saturating_elapsed();
        Instant::now_coarse().saturating_elapsed();

        // Ordering.
        let early_raw = Instant::now();
        let late_raw = Instant::now();
        assert!(early_raw <= late_raw);
        assert!(late_raw >= early_raw);

        assert_eq!(early_raw, early_raw);
        assert!(early_raw >= early_raw);
        assert!(early_raw <= early_raw);

        let early_coarse = Instant::now_coarse();
        let late_coarse = Instant::now_coarse();
        assert!(late_coarse >= early_coarse);
        assert!(early_coarse <= late_coarse);

        assert_eq!(early_coarse, early_coarse);
        assert!(early_coarse >= early_coarse);
        assert!(early_coarse <= early_coarse);

        let zero = Duration::new(0, 0);
        // Sub Instant.
        assert!(late_raw.duration_since(early_raw) >= zero);
        assert!(late_coarse.duration_since(early_coarse) >= zero);

        // Sub Duration.
        assert_eq!(late_raw - zero, late_raw);
        assert_eq!(late_coarse - zero, late_coarse);

        // Sub assign Duration
        let mut tmp_late_row = late_raw;
        tmp_late_row -= zero;
        assert_eq!(tmp_late_row, late_raw);

        // checked_sub Duration.
        assert!(late_raw.checked_sub(early_raw).unwrap() >= zero);
        // It's either `None` or `Some(zero)`(if they are equal).
        assert_eq!(early_raw.checked_sub(late_raw).unwrap_or(zero), zero);

        let mut tmp_late_coarse = late_coarse;
        tmp_late_coarse -= zero;
        assert_eq!(tmp_late_coarse, late_coarse);

        // Add Duration.
        assert_eq!(late_raw + zero, late_raw);
        assert_eq!(late_coarse + zero, late_coarse);

        // add assign
        let mut tmp_late_row = late_raw;
        tmp_late_row += zero;
        assert_eq!(tmp_late_row, late_raw);

        let mut tmp_coarse = late_coarse;
        tmp_coarse += zero;
        assert_eq!(tmp_coarse, late_coarse);

        // PartialEq and PartialOrd
        let ts = Timespec::new(1, 1);
        let now1 = Instant::Monotonic(ts);
        let now2 = Instant::MonotonicCoarse(ts);
        assert_ne!(now1, now2);
        assert_eq!(now1.partial_cmp(&now2), None);
    }

    #[test]
    fn test_coarse_instant_on_smp() {
        let zero = Duration::from_millis(0);
        for i in 0..1_000_000 {
            let now = Instant::now();
            let now_coarse = Instant::now_coarse();
            if i % 100 == 0 {
                thread::yield_now();
            }
            assert!(now.saturating_elapsed() >= zero);
            assert!(now_coarse.saturating_elapsed() >= zero);
        }
    }
}

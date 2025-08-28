// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::Add,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

pub struct AtomicDuration {
    pub(crate) unit: TimeUnit,
    pub(crate) dur: AtomicU64,
}

impl AtomicDuration {
    pub fn new(dur: Duration, unit: TimeUnit) -> Self {
        Self {
            unit,
            dur: AtomicU64::new(Self::parse_duration_number(dur, unit)),
        }
    }

    fn parse_duration_number(dur: Duration, unit: TimeUnit) -> u64 {
        match unit {
            TimeUnit::Second => dur.as_secs(),
            TimeUnit::Millisecond => dur.as_millis() as u64,
            TimeUnit::Microsecond => dur.as_micros() as u64,
            TimeUnit::Nanosecond => dur.as_nanos() as u64,
        }
    }
    fn from_duration_number(dur: u64, unit: TimeUnit) -> Duration {
        match unit {
            TimeUnit::Second => Duration::from_secs(dur),
            TimeUnit::Millisecond => Duration::from_millis(dur),
            TimeUnit::Microsecond => Duration::from_micros(dur),
            TimeUnit::Nanosecond => Duration::from_nanos(dur),
        }
    }

    pub fn load(&self) -> Duration {
        Self::from_duration_number(self.dur.load(Relaxed), self.unit)
    }

    pub fn store(&self, dur: Duration) {
        self.dur
            .store(Self::parse_duration_number(dur, self.unit), Relaxed)
    }

    pub fn compare_exchange(&self, old: Duration, new: Duration) -> Option<Duration> /* current duration */
    {
        match self.dur.compare_exchange(
            Self::parse_duration_number(old, self.unit),
            Self::parse_duration_number(new, self.unit),
            Relaxed,
            Relaxed,
        ) {
            Ok(_) => None, // Updated successful.
            Err(current) => {
                // Update failed, return the current duration.
                Some(Self::from_duration_number(current, self.unit))
            }
        }
    }
}

pub struct AtomicTime {
    pub(crate) start: Instant,
    pub(crate) offset: AtomicDuration,
}

impl AtomicTime {
    pub fn new(start: Instant, unit: TimeUnit) -> Self {
        Self {
            offset: AtomicDuration::new(start.elapsed(), unit),
            start,
        }
    }

    fn parse_offset(&self, instant: Instant) -> Duration {
        instant.duration_since(self.start)
    }

    fn to_instant(&self, offset: Duration) -> Instant {
        self.start.add(offset)
    }

    pub fn load(&self) -> Instant {
        self.to_instant(self.offset.load())
    }

    pub fn store(&self, instant: Instant) {
        self.offset.store(self.parse_offset(instant))
    }

    pub fn compare_exchange(&self, old: Instant, new: Instant) -> Option<Instant> /* current time */
    {
        self.offset
            .compare_exchange(self.parse_offset(old), self.parse_offset(new))
            .map(|current| self.to_instant(current))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        thread::sleep,
        time::{Duration, Instant},
    };

    use crate::*;

    #[test]
    fn test_atomic_duration() {
        test_util::init_log_for_test();
        let cases = vec![
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];
        for unit in cases {
            let dur = Duration::from_secs(1);
            let atomic_duration = AtomicDuration::new(dur, unit);
            assert_eq!(atomic_duration.load(), dur);
            let new_dur = Duration::from_secs(2);
            atomic_duration.store(new_dur);
            assert_eq!(atomic_duration.load(), new_dur);
        }
    }

    #[test]
    fn test_atomic_time() {
        test_util::init_log_for_test();
        let start = Instant::now();
        let atomic_time = AtomicTime::new(start, TimeUnit::Nanosecond);
        sleep(Duration::from_millis(10));
        let now = Instant::now();
        atomic_time.store(now);
        assert_eq!(atomic_time.load(), now);
        sleep(Duration::from_millis(20));
        let new_now = Instant::now();
        assert!(atomic_time.compare_exchange(now, new_now).is_none());
        assert_eq!(atomic_time.load(), new_now);
    }
}

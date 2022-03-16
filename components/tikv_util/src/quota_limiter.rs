// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use super::config::ReadableSize;
use super::time::Instant;
use super::time::Limiter;
use cpu_time::ThreadTime;
use std::time::Duration;

// The cpu time is not a real statistics, only part of the processing logic is
// taken into account, so it needs to be multiplied by a factor.
// transfer milli cpu to micro cpu
//
// TODO: Don't adjusted based on experience.
const CPU_TIME_FACTOR: f64 = 0.9;

const MAX_QUOTA_DELAY: Duration = Duration::from_secs(1);

// Quota limiter allows users to obtain stable performance by increasing the
// completion time of tasks through restrictions of different metrics.
#[derive(Debug)]
pub struct QuotaLimiter {
    cputime_limiter: Limiter,
    write_bandwidth_limiter: Limiter,
    read_bandwidth_limiter: Limiter,
}

impl Default for QuotaLimiter {
    fn default() -> Self {
        Self {
            cputime_limiter: Limiter::new(f64::INFINITY),
            write_bandwidth_limiter: Limiter::new(f64::INFINITY),
            read_bandwidth_limiter: Limiter::new(f64::INFINITY),
        }
    }
}

impl QuotaLimiter {
    // 1000 millicpu equals to 1vCPU, 0 means unlimited
    pub fn new(
        cpu_quota: usize,
        write_bandwidth: ReadableSize,
        read_bandwidth: ReadableSize,
    ) -> Self {
        let cputime_limiter = if cpu_quota == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            Limiter::new(cpu_quota as f64 * CPU_TIME_FACTOR * 1000_f64)
        };

        let write_bandwidth_limiter = if write_bandwidth.0 == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            Limiter::new(write_bandwidth.0 as f64)
        };

        let read_bandwidth_limiter = if read_bandwidth.0 == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            Limiter::new(read_bandwidth.0 as f64)
        };

        Self {
            cputime_limiter,
            write_bandwidth_limiter,
            read_bandwidth_limiter,
        }
    }

    // record info after write requests finished and return the suggested delay duration.
    pub fn consume_write(&self, bytes: usize, cpu_time: Duration) -> Duration {
        let cpu_dur = self
            .cputime_limiter
            .consume_duration(cpu_time.as_micros() as usize);

        let bw_dur = if bytes > 0 {
            self.write_bandwidth_limiter.consume_duration(bytes)
        } else {
            Duration::ZERO
        };

        let max_dur = std::cmp::max(cpu_dur, bw_dur);
        let should_delay = if max_dur > cpu_time {
            max_dur - cpu_time
        } else {
            Duration::ZERO
        };
        std::cmp::min(MAX_QUOTA_DELAY, should_delay)
    }

    // record info after read requests finished and return the suggested delay duration.
    pub fn consume_read(&self, bytes: usize, cpu_time: Duration) -> Duration {
        let cpu_dur = self
            .cputime_limiter
            .consume_duration(cpu_time.as_micros() as usize);

        let bw_dur = if bytes > 0 {
            self.read_bandwidth_limiter.consume_duration(bytes)
        } else {
            Duration::ZERO
        };

        let max_dur = std::cmp::max(cpu_dur, bw_dur);
        let should_delay = if max_dur > cpu_time {
            max_dur - cpu_time
        } else {
            Duration::ZERO
        };
        std::cmp::min(MAX_QUOTA_DELAY, should_delay)
    }

    // If `cputime_limiter` is set to INFINITY, use `CLOCK_MONOTONIC_COARSE` to save cost.
    pub fn get_now_timer(&self) -> Box<dyn Timer> {
        if self.cputime_limiter.speed_limit().is_infinite() {
            Box::new(Instant::now_coarse())
        } else {
            Box::new(ThreadTime::now())
        }
    }
}

pub trait Timer {
    fn elapsed(&self) -> Duration;
}

impl Timer for Instant {
    fn elapsed(&self) -> Duration {
        self.saturating_elapsed()
    }
}

impl Timer for ThreadTime {
    fn elapsed(&self) -> Duration {
        self.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_limiter() {
        // consume write
        let quota_limiter = QuotaLimiter::new(
            1100, /*1.1vCPU*/
            ReadableSize::kb(1),
            ReadableSize::kb(1),
        );

        let thread_start_time = quota_limiter.get_now_timer();

        // delay = 495 / (1100 * CPU_TIME_FACTOR) * 1 sec = 500ms, and 500 - 495 = 5
        let delay = quota_limiter.consume_write(0, Duration::from_millis(495));
        assert_eq!(delay, Duration::from_millis(5));

        // only bytes take effect
        let delay =
            quota_limiter.consume_write(ReadableSize::kb(1).0 as usize, Duration::from_millis(99));
        assert_eq!(delay, Duration::from_millis(901));

        // when all set to zero, only cpu time limiter take effect
        let delay = quota_limiter.consume_write(0, Duration::ZERO);
        assert!(delay <= Duration::from_millis(600));
        assert!(delay > Duration::ZERO);

        // need to sleep to refresh cpu time limiter
        std::thread::sleep(Duration::from_millis(600));

        // refill is 0.1
        let delay = quota_limiter.consume_read(0, Duration::from_micros(98999));
        assert_eq!(delay, Duration::from_secs(0));

        // `get_now_timer` must return ThreadTime so elapsed time is not long.
        assert!(thread_start_time.elapsed() < Duration::from_millis(500));
    }
}

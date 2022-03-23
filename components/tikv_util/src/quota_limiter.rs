// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use super::config::{ReadableDuration, ReadableSize};
use super::time::Limiter;
use super::timer::GLOBAL_TIMER_HANDLE;

use cpu_time::ThreadTime;
use futures::compat::Future01CompatExt;

const CPU_LIMITER_REFILL_DURATION: Duration = Duration::from_millis(50);

// Quota limiter allows users to obtain stable performance by increasing the
// completion time of tasks through restrictions of different metrics.
#[derive(Debug)]
pub struct QuotaLimiter {
    cputime_limiter: Limiter,
    write_bandwidth_limiter: Limiter,
    read_bandwidth_limiter: Limiter,
    max_delay_duration: Duration,
}

// Throttle must be consumed in quota limiter.
pub struct Sample {
    read_bytes: usize,
    write_bytes: usize,
    cpu_time: Duration,
    enable_cpu_limit: bool,
}

impl<'a> Sample {
    pub fn add_read_bytes(&mut self, bytes: usize) {
        self.read_bytes += bytes;
    }

    pub fn add_write_bytes(&mut self, bytes: usize) {
        self.write_bytes += bytes;
    }

    // Record the cpu time in the lifetime. Use this function inside code block.
    // If `cputime_limiter` is not enabled, guard will do nothing when dropped.
    pub fn observe_cpu(&'a mut self) -> CpuObserveGuard<'a> {
        if self.enable_cpu_limit {
            CpuObserveGuard {
                timer: Some(ThreadTime::now()),
                sample: self,
            }
        } else {
            CpuObserveGuard {
                timer: None,
                sample: self,
            }
        }
    }

    fn add_cpu_time(&mut self, time: Duration) {
        self.cpu_time += time;
    }
}

pub struct CpuObserveGuard<'a> {
    timer: Option<ThreadTime>,
    sample: &'a mut Sample,
}

impl<'a> Drop for CpuObserveGuard<'a> {
    fn drop(&mut self) {
        if let Some(timer) = self.timer {
            self.sample.add_cpu_time(timer.elapsed());
        }
    }
}

impl Default for QuotaLimiter {
    fn default() -> Self {
        Self {
            cputime_limiter: Limiter::new(f64::INFINITY),
            write_bandwidth_limiter: Limiter::new(f64::INFINITY),
            read_bandwidth_limiter: Limiter::new(f64::INFINITY),
            max_delay_duration: Duration::ZERO,
        }
    }
}

impl QuotaLimiter {
    // 1000 millicpu equals to 1vCPU, 0 means unlimited
    pub fn new(
        cpu_quota: usize,
        write_bandwidth: ReadableSize,
        read_bandwidth: ReadableSize,
        max_delay_duration: ReadableDuration,
    ) -> Self {
        let cputime_limiter = if cpu_quota == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            Limiter::builder(cpu_quota as f64 * 1000_f64)
                .refill(CPU_LIMITER_REFILL_DURATION)
                .build()
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

        let max_delay_duration = max_delay_duration.0;

        Self {
            cputime_limiter,
            write_bandwidth_limiter,
            read_bandwidth_limiter,
            max_delay_duration,
        }
    }

    // To generate a sampler.
    pub fn new_sample(&self) -> Sample {
        Sample {
            read_bytes: 0,
            write_bytes: 0,
            cpu_time: Duration::ZERO,
            enable_cpu_limit: !self.cputime_limiter.speed_limit().is_infinite(),
        }
    }

    // To consume a sampler and return delayed duration.
    // If the sampler is null, the speed limiter will just return ZERO.
    pub async fn async_consume(&self, sample: Sample) -> Duration {
        let cpu_dur = if sample.cpu_time > Duration::ZERO {
            self.cputime_limiter
                .consume_duration(sample.cpu_time.as_micros() as usize)
        } else {
            Duration::ZERO
        };

        let w_bw_dur = if sample.write_bytes > 0 {
            self.write_bandwidth_limiter
                .consume_duration(sample.write_bytes)
        } else {
            Duration::ZERO
        };

        let r_bw_dur = if sample.read_bytes > 0 {
            self.read_bandwidth_limiter
                .consume_duration(sample.read_bytes)
        } else {
            Duration::ZERO
        };

        let mut exec_delay = std::cmp::max(cpu_dur, std::cmp::max(w_bw_dur, r_bw_dur));
        if !self.max_delay_duration.is_zero() {
            exec_delay = std::cmp::min(self.max_delay_duration, exec_delay);
        };

        if !exec_delay.is_zero() {
            GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + exec_delay)
                .compat()
                .await
                .unwrap();
        }

        exec_delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn test_quota_limiter() {
        // refill duration = 50ms
        // bucket capacity = 50
        let quota_limiter = QuotaLimiter::new(
            1000,
            ReadableSize::kb(1),
            ReadableSize::kb(1),
            ReadableDuration::millis(0),
        );

        let thread_start_time = ThreadTime::now();

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(20));
        let should_delay = block_on(quota_limiter.async_consume(sample));
        assert_eq!(should_delay, Duration::ZERO);

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(30));
        let should_delay = block_on(quota_limiter.async_consume(sample));
        assert_eq!(should_delay, Duration::from_millis(50));

        std::thread::sleep(Duration::from_millis(10));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(30));
        let should_delay = block_on(quota_limiter.async_consume(sample));
        // should less 20+30+30
        assert!(should_delay < Duration::from_millis(80));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(200));
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.async_consume(sample));
        assert_eq!(should_delay, Duration::from_millis(250));

        // ThreadTime elapsed time is not long.
        assert!(thread_start_time.elapsed() < Duration::from_millis(50));
    }
}

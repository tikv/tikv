// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use super::config::ReadableSize;
use super::time::Limiter;
use super::timer::GLOBAL_TIMER_HANDLE;

use cpu_time::ThreadTime;
use futures::compat::Future01CompatExt;

// The cpu time is not a real statistics, only part of the processing logic is
// taken into account, so it needs to be multiplied by a factor.
// transfer milli cpu to micro cpu
//
// TODO: Don't adjusted based on experience.
const CPU_TIME_FACTOR: f64 = 0.8;

// To avoid long tail latency.
const MAX_QUOTA_DELAY: Duration = Duration::from_secs(1);

// Quota limiter allows users to obtain stable performance by increasing the
// completion time of tasks through restrictions of different metrics.
#[derive(Debug)]
pub struct QuotaLimiter {
    cputime_limiter: Limiter,
    write_bandwidth_limiter: Limiter,
    read_bandwidth_limiter: Limiter,
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

        let max_dur = std::cmp::max(cpu_dur, std::cmp::max(w_bw_dur, r_bw_dur));
        let should_delay = if max_dur > sample.cpu_time {
            max_dur - sample.cpu_time
        } else {
            Duration::ZERO
        };
        let exec_delay = std::cmp::min(MAX_QUOTA_DELAY, should_delay);

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
        // consume write
        let quota_limiter = QuotaLimiter::new(
            1250, /*1.1vCPU*/
            ReadableSize::kb(1),
            ReadableSize::kb(1),
        );

        let thread_start_time = ThreadTime::now();

        // (1250 * CPU_TIME_FACTOR) * 1 sec = 1000 millis
        let mut th = quota_limiter.new_sample();
        th.add_cpu_time(Duration::from_millis(200));
        let begin_instant = std::time::Instant::now();
        block_on(quota_limiter.async_consume(th));
        // 50ms represents fast
        assert!(begin_instant.elapsed() < Duration::from_millis(50));

        // only bytes take effect (1000ms - 300ms used by cpu)
        // (1250 * CPU_TIME_FACTOR) * 1 sec = 1000 millis
        let mut th = quota_limiter.new_sample();
        th.add_cpu_time(Duration::from_millis(300));
        th.add_write_bytes(ReadableSize::kb(1).0 as usize);
        let begin_instant = std::time::Instant::now();
        block_on(quota_limiter.async_consume(th));
        assert!(begin_instant.elapsed() > Duration::from_millis(700));
        assert!(begin_instant.elapsed() < Duration::from_millis(800));

        // test max delay
        let mut th = quota_limiter.new_sample();
        th.add_cpu_time(Duration::from_millis(100));
        th.add_read_bytes(ReadableSize::kb(100).0 as usize);
        let begin_instant = std::time::Instant::now();
        block_on(quota_limiter.async_consume(th));
        assert!(begin_instant.elapsed() < Duration::from_millis(1100));
        assert!(begin_instant.elapsed() > Duration::from_millis(1000));

        // ThreadTime elapsed time is not long.
        assert!(thread_start_time.elapsed() < Duration::from_millis(100));
    }
}

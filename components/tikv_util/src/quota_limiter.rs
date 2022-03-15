// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use super::config::ReadableSize;
use super::time::Limiter;
use cpu_time::ThreadTime;
use std::time::Duration;
use std::time::Instant;

const CPU_TIME_FACTOR: f64 = 0.9;

// Quota limiter allows users to obtain stable performance by increasing the
// completion time of tasks through restrictions of different metrics.
#[derive(Debug)]
pub struct QuotaLimiter {
    cputime_limiter: Limiter,
    write_kvs_limiter: Limiter,
    write_bandwidth_limiter: Limiter,
    read_bandwidth_limiter: Limiter,
    req_rate_limiter: Limiter,
}

impl Default for QuotaLimiter {
    fn default() -> Self {
        Self {
            cputime_limiter: Limiter::new(f64::INFINITY),
            write_kvs_limiter: Limiter::new(f64::INFINITY),
            write_bandwidth_limiter: Limiter::new(f64::INFINITY),
            read_bandwidth_limiter: Limiter::new(f64::INFINITY),
            req_rate_limiter: Limiter::new(f64::INFINITY),
        }
    }
}

impl QuotaLimiter {
    // 1000 millicpu equals to 1vCPU, 0 means unlimited
    pub fn new(
        cpu_quota: usize,
        req_rate: usize,
        write_kvs: usize,
        write_bandwidth: ReadableSize,
        read_bandwidth: ReadableSize,
    ) -> Self {
        let cputime_limiter = if cpu_quota == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            // The cpu time is not a real statistics, only part of the processing logic is
            // taken into account, so it needs to be multiplied by a factor.
            // transfer milli cpu to micro cpu
            Limiter::new(cpu_quota as f64 * CPU_TIME_FACTOR * 1000_f64)
        };

        let write_kvs_limiter = if write_kvs == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            Limiter::new(write_kvs as f64)
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

        let total_qps_limiter = if req_rate == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            Limiter::new(req_rate as f64)
        };

        Self {
            cputime_limiter,
            write_kvs_limiter,
            write_bandwidth_limiter,
            read_bandwidth_limiter,
            req_rate_limiter: total_qps_limiter,
        }
    }

    pub fn consume_write(
        &self,
        req_cnt: usize,
        kv_cnt: usize,
        bytes: usize,
        cpu_micro_secs: usize,
    ) -> Duration {
        let cpu_dur = self.cputime_limiter.consume_duration(cpu_micro_secs);

        let kv_dur = if kv_cnt > 0 {
            self.write_kvs_limiter.consume_duration(kv_cnt)
        } else {
            Duration::ZERO
        };

        let bw_dur = if bytes > 0 {
            self.write_bandwidth_limiter.consume_duration(bytes)
        } else {
            Duration::ZERO
        };

        let req_rate_dur = if req_cnt > 0 {
            self.req_rate_limiter.consume_duration(req_cnt)
        } else {
            Duration::ZERO
        };

        std::cmp::max(
            req_rate_dur,
            std::cmp::max(std::cmp::max(cpu_dur, kv_dur), bw_dur),
        )
    }

    pub fn consume_read(
        &self,
        req_cnt: usize,
        read_bytes: usize,
        cpu_micro_secs: usize,
    ) -> Duration {
        let cpu_dur = self.cputime_limiter.consume_duration(cpu_micro_secs);

        let bw_dur = if read_bytes > 0 {
            self.read_bandwidth_limiter.consume_duration(read_bytes)
        } else {
            Duration::ZERO
        };

        let req_rate_dur = self.req_rate_limiter.consume_duration(req_cnt);

        std::cmp::max(req_rate_dur, std::cmp::max(cpu_dur, bw_dur))
    }

    // If `cputime_limiter` is set to INFINITY, use `CLOCK_MONOTONIC` to save cost.
    pub fn get_now_time(&self) -> Box<dyn Timer> {
        if self.cputime_limiter.speed_limit().is_infinite() {
            Box::new(Instant::now())
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
        self.elapsed()
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
            64,
            1024,
            ReadableSize::kb(1),
            ReadableSize::kb(1),
        );

        let thread_start_time = quota_limiter.get_now_time();

        // delay = 495 / (1100 * CPU_TIME_FACTOR) * 1 sec = 500ms
        let delay = quota_limiter.consume_write(0, 0, 0, 495000);
        assert_eq!(delay, Duration::from_millis(500));
        // only write_kvs_limiter take effect
        let delay = quota_limiter.consume_write(32, 1536, 0, 0);
        assert_eq!(delay, Duration::from_millis(1500));
        let delay = quota_limiter.consume_write(0, 0, 0, 0);
        // when all set to zero, only cpu time limiter take effect
        assert_eq!(delay, Duration::from_millis(500));

        // need to sleep to refresh cpu time limiter
        std::thread::sleep(Duration::from_millis(500));

        // refill is 0.1 so 98999 will not trigger delay (98999 < 990000/10)
        let delay = quota_limiter.consume_read(0, 0, 98999);
        assert_eq!(delay, Duration::from_secs(0));

        let delay = quota_limiter.consume_read(0, 128, 0);
        assert_eq!(delay, Duration::from_millis(125));

        // `get_now_time` must return ThreadTime so elapsed time is not long.
        assert!(thread_start_time.elapsed() < Duration::from_millis(500));
    }
}

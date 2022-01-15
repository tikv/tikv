// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use super::config::ReadableSize;
use super::time::Limiter;
use std::time::Duration;

pub enum QType {
    KvGet,
    CoprScan,
    Others,
}

pub struct QuotaLimiter {
    cputime_limiter: Limiter,
    write_kvs_limiter: Limiter,
    write_bandwidth_limiter: Limiter,
    read_bandwidth_limiter: Limiter,
}

impl Default for QuotaLimiter {
    fn default() -> Self {
        Self {
            cputime_limiter: Limiter::new(f64::INFINITY),
            write_kvs_limiter: Limiter::new(f64::INFINITY),
            write_bandwidth_limiter: Limiter::new(f64::INFINITY),
            read_bandwidth_limiter: Limiter::new(f64::INFINITY),
        }
    }
}

impl QuotaLimiter {
    // 1000 millicpu equals to 1vCPU, 0 means unlimited
    pub fn new(
        cpu_quota: usize,
        write_kvs: usize,
        write_bandwidth: ReadableSize,
        read_bandwidth: ReadableSize,
    ) -> Self {
        let cputime_limiter = if cpu_quota == 0 {
            Limiter::new(f64::INFINITY)
        } else {
            // transfer milli cpu to micro cpu
            Limiter::new(cpu_quota as f64 * 1000_f64)
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

        Self {
            cputime_limiter,
            write_kvs_limiter,
            write_bandwidth_limiter,
            read_bandwidth_limiter,
        }
    }

    pub fn consume_write(&self, req_cnt: usize, kv_cnt: usize, bytes: usize) -> Duration {
        let cost_micro_cpu: usize = req_cnt * 200 + kv_cnt * 50;
        let cpu_dur = self.cputime_limiter.consume_duration(cost_micro_cpu);

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

        cpu_dur + kv_dur + bw_dur
    }

    pub fn consume_read(
        &self,
        time_micro_secs: usize,
        bytes: usize,
        query_type: QType,
    ) -> Duration {
        let cpu_dur = match query_type {
            QType::KvGet => self
                .cputime_limiter
                .consume_duration(time_micro_secs as usize + 100 /*request overhead*/),
            QType::CoprScan => self
                .cputime_limiter
                .consume_duration(time_micro_secs as usize + 50),
            _ => self
                .cputime_limiter
                .consume_duration(time_micro_secs as usize),
        };
        let bw_dur = if bytes > 0 {
            self.read_bandwidth_limiter.consume_duration(bytes)
        } else {
            Duration::ZERO
        };
        cpu_dur + bw_dur
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_limiter() {
        // consume write
        let quota_limiter = QuotaLimiter::new(
            1000, /*1vCPU*/
            1024,
            ReadableSize::kb(1),
            ReadableSize::kb(1),
        );
        let delay = quota_limiter.consume_write(1, 1, 1024);
        assert_eq!(delay, Duration::from_secs(1));

        // 10K write requests will cost more than 1vCPU
        let quota_limiter = QuotaLimiter::new(
            1000, /*1vCPU*/
            1024,
            ReadableSize::kb(1),
            ReadableSize::kb(1),
        );
        let delay = quota_limiter.consume_write(5000, 0, 0);
        assert_eq!(delay, Duration::from_secs(1));

        // consume read
        let quota_limiter = QuotaLimiter::new(
            1000, /*1vCPU*/
            1024,
            ReadableSize::kb(1),
            ReadableSize::kb(1),
        );
        let delay = quota_limiter.consume_read(1_000_000, 0, QType::Others);
        assert_eq!(delay, Duration::from_secs(1));

        let quota_limiter = QuotaLimiter::new(
            1000, /*1vCPU*/
            1024,
            ReadableSize::kb(1),
            ReadableSize::kb(1),
        );
        let delay = quota_limiter.consume_read(0, 1024, QType::Others);
        assert_eq!(delay, Duration::from_secs(1));
    }
}

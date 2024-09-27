// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use file_system::IoBytes;
use futures::compat::Future01CompatExt;
use prometheus::Histogram;
use strum::EnumCount;
use tikv_util::{resource_control::TaskPriority, time::Limiter, timer::GLOBAL_TIMER_HANDLE};

use crate::metrics::PRIORITY_WAIT_DURATION_VEC;

#[derive(Clone, Copy, Eq, PartialEq, EnumCount)]
#[repr(usize)]
pub enum ResourceType {
    Cpu,
    Io,
}

impl ResourceType {
    pub fn as_str(&self) -> &str {
        match *self {
            ResourceType::Cpu => "cpu",
            ResourceType::Io => "io",
        }
    }
}

impl fmt::Debug for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub struct ResourceLimiter {
    _name: String,
    version: u64,
    limiters: [QuotaLimiter; ResourceType::COUNT],
    // whether the resource limiter is a background limiter or priority limiter.
    is_background: bool,
    // the wait duration histogram for prioitry limiter.
    wait_histogram: Option<Histogram>,
}

impl std::fmt::Debug for ResourceLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResourceLimiter(...)")
    }
}

impl ResourceLimiter {
    pub fn new(
        name: String,
        cpu_limit: f64,
        io_limit: f64,
        version: u64,
        is_background: bool,
    ) -> Self {
        let cpu_limiter = QuotaLimiter::new(cpu_limit);
        let io_limiter = QuotaLimiter::new(io_limit);
        // high priority tasks does not triggers wait, so no need to generate an empty
        // metrics.
        let wait_histogram = if !is_background && name != TaskPriority::High.as_str() {
            Some(
                PRIORITY_WAIT_DURATION_VEC
                    .get_metric_with_label_values(&[&name])
                    .unwrap(),
            )
        } else {
            None
        };
        Self {
            _name: name,
            version,
            limiters: [cpu_limiter, io_limiter],
            is_background,
            wait_histogram,
        }
    }

    pub fn is_background(&self) -> bool {
        self.is_background
    }

    pub fn consume(&self, cpu_time: Duration, io_bytes: IoBytes, wait: bool) -> Duration {
        let cpu_dur =
            self.limiters[ResourceType::Cpu as usize].consume(cpu_time.as_micros() as u64, wait);
        let io_dur = self.limiters[ResourceType::Io as usize].consume_io(io_bytes, wait);
        let wait_dur = cpu_dur.max(io_dur);
        if !wait_dur.is_zero()
            && let Some(h) = &self.wait_histogram
        {
            h.observe(wait_dur.as_secs_f64());
        }
        wait_dur
    }

    pub async fn async_consume(&self, cpu_time: Duration, io_bytes: IoBytes) -> Duration {
        let dur = self.consume(cpu_time, io_bytes, true);
        if !dur.is_zero() {
            _ = GLOBAL_TIMER_HANDLE
                .delay(Instant::now() + dur)
                .compat()
                .await;
        }
        dur
    }

    #[inline]
    pub(crate) fn get_limiter(&self, ty: ResourceType) -> &QuotaLimiter {
        &self.limiters[ty as usize]
    }

    pub(crate) fn get_limit_statistics(&self, ty: ResourceType) -> GroupStatistics {
        let (total_consumed, total_wait_dur_us, read_consumed, write_consumed, request_count) =
            self.limiters[ty as usize].get_statistics();
        GroupStatistics {
            version: self.version,
            total_consumed,
            total_wait_dur_us,
            read_consumed,
            write_consumed,
            request_count,
        }
    }
}

pub(crate) struct QuotaLimiter {
    limiter: Limiter,
    // total waiting duration in us
    total_wait_dur_us: AtomicU64,
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    req_count: AtomicU64,
}

impl QuotaLimiter {
    fn new(limit: f64) -> Self {
        Self {
            // we use 1s refill and 1ms min_wait duration to avoid trigger wait too frequently.
            // NOTE: the parameter `refill` mainly impact the capacity of token bucket but not
            // refill interval.
            limiter: Limiter::builder(limit)
                .refill(Duration::from_millis(1000))
                .min_wait(Duration::from_millis(1))
                .build(),
            total_wait_dur_us: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            req_count: AtomicU64::new(0),
        }
    }

    pub(crate) fn get_rate_limit(&self) -> f64 {
        self.limiter.speed_limit()
    }

    pub(crate) fn set_rate_limit(&self, mut limit: f64) {
        // treat 0 as infinity.
        if limit <= f64::EPSILON {
            limit = f64::INFINITY;
        }
        self.limiter.set_speed_limit(limit);
    }

    fn get_statistics(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.limiter.total_bytes_consumed() as u64,
            self.total_wait_dur_us.load(Ordering::Relaxed),
            self.read_bytes.load(Ordering::Relaxed),
            self.write_bytes.load(Ordering::Relaxed),
            self.req_count.load(Ordering::Relaxed),
        )
    }

    fn consume(&self, value: u64, wait: bool) -> Duration {
        if value == 0 && self.limiter.speed_limit().is_infinite() {
            return Duration::ZERO;
        }
        let mut dur = self.limiter.consume_duration(value as usize);
        if !wait {
            dur = Duration::ZERO;
        } else if dur != Duration::ZERO {
            self.total_wait_dur_us
                .fetch_add(dur.as_micros() as u64, Ordering::Relaxed);
        }
        self.req_count.fetch_add(1, Ordering::Relaxed);
        dur
    }

    fn consume_io(&self, value: IoBytes, wait: bool) -> Duration {
        self.read_bytes.fetch_add(value.read, Ordering::Relaxed);
        self.write_bytes.fetch_add(value.write, Ordering::Relaxed);

        let value = value.read + value.write;
        if value == 0 && self.limiter.speed_limit().is_infinite() {
            return Duration::ZERO;
        }
        let mut dur = self.limiter.consume_duration(value as usize);
        if !wait {
            dur = Duration::ZERO;
        } else if dur != Duration::ZERO {
            self.total_wait_dur_us
                .fetch_add(dur.as_micros() as u64, Ordering::Relaxed);
        }
        self.req_count.fetch_add(1, Ordering::Relaxed);
        dur
    }
}

#[derive(Default, Clone, PartialEq, Eq, Copy, Debug)]
pub struct GroupStatistics {
    pub version: u64,
    pub total_consumed: u64,
    pub total_wait_dur_us: u64,
    pub read_consumed: u64,
    pub write_consumed: u64,
    pub request_count: u64,
}

impl std::ops::Sub for GroupStatistics {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            version: self.version,
            total_consumed: self.total_consumed.saturating_sub(rhs.total_consumed),
            total_wait_dur_us: self.total_wait_dur_us.saturating_sub(rhs.total_wait_dur_us),
            read_consumed: self.read_consumed.saturating_sub(rhs.read_consumed),
            write_consumed: self.write_consumed.saturating_sub(rhs.write_consumed),
            request_count: self.request_count.saturating_sub(rhs.request_count),
        }
    }
}

impl std::ops::Div<f64> for GroupStatistics {
    type Output = Self;

    fn div(self, rhs: f64) -> Self::Output {
        Self {
            version: self.version,
            total_consumed: (self.total_consumed as f64 / rhs) as u64,
            total_wait_dur_us: (self.total_wait_dur_us as f64 / rhs) as u64,
            read_consumed: (self.read_consumed as f64 / rhs) as u64,
            write_consumed: (self.write_consumed as f64 / rhs) as u64,
            request_count: (self.request_count as f64 / rhs) as u64,
        }
    }
}

// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use strum::EnumCount;
use tikv_util::time::Limiter;

#[derive(Clone, Copy, Eq, PartialEq, EnumCount)]
#[repr(usize)]
pub enum ResourceType {
    Cpu,
    Io,
}

impl fmt::Debug for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ResourceType::Cpu => write!(f, "cpu"),
            ResourceType::Io => write!(f, "io"),
        }
    }
}

pub struct ResourceLimiter {
    limiters: [QuotaLimiter; ResourceType::COUNT],
}

impl std::fmt::Debug for ResourceLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResourceLimiter(...)")
    }
}

impl ResourceLimiter {
    pub fn new(cpu_limit: f64, io_limit: f64) -> Self {
        let cpu_limiter = QuotaLimiter::new(cpu_limit);
        let io_limiter = QuotaLimiter::new(io_limit);
        Self {
            limiters: [cpu_limiter, io_limiter],
        }
    }

    pub fn consume(&self, cpu_time: Duration, io_bytes: u64) -> Duration {
        let cpu_dur =
            self.limiters[ResourceType::Cpu as usize].consume(cpu_time.as_micros() as u64);
        let io_dur = self.limiters[ResourceType::Io as usize].consume(io_bytes);
        cpu_dur.max(io_dur)
    }

    #[inline]
    pub(crate) fn get_limiter(&self, ty: ResourceType) -> &QuotaLimiter {
        &self.limiters[ty as usize]
    }
}

pub(crate) struct QuotaLimiter {
    limiter: Limiter,
    // total waiting duration in us
    total_wait_dur_us: AtomicU64,
}

impl QuotaLimiter {
    fn new(limit: f64) -> Self {
        Self {
            limiter: Limiter::new(limit),
            total_wait_dur_us: AtomicU64::new(0),
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

    pub fn get_statistics(&self) -> GroupStatistics {
        GroupStatistics {
            total_consumed: self.limiter.total_bytes_consumed() as u64,
            total_wait_dur_us: self.total_wait_dur_us.load(Ordering::Relaxed),
        }
    }

    fn consume(&self, value: u64) -> Duration {
        if value == 0 {
            return Duration::ZERO;
        }
        let dur = self.limiter.consume_duration(value as usize);
        if dur != Duration::ZERO {
            self.total_wait_dur_us
                .fetch_add(dur.as_micros() as u64, Ordering::Relaxed);
        }
        dur
    }
}

#[derive(Default, Clone, PartialEq, Eq, Copy, Debug)]
pub struct GroupStatistics {
    pub total_consumed: u64,
    pub total_wait_dur_us: u64,
}

impl std::ops::Sub for GroupStatistics {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            total_consumed: self.total_consumed.saturating_sub(rhs.total_consumed),
            total_wait_dur_us: self.total_wait_dur_us.saturating_sub(rhs.total_wait_dur_us),
        }
    }
}

impl std::ops::Div<f64> for GroupStatistics {
    type Output = Self;

    fn div(self, rhs: f64) -> Self::Output {
        Self {
            total_consumed: (self.total_consumed as f64 / rhs) as u64,
            total_wait_dur_us: (self.total_wait_dur_us as f64 / rhs) as u64,
        }
    }
}

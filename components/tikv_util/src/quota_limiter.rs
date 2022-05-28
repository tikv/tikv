// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use cpu_time::ThreadTime;
use futures::compat::Future01CompatExt;
use online_config::{ConfigChange, ConfigManager};

use super::{
    config::{ReadableDuration, ReadableSize},
    time::Limiter,
    timer::GLOBAL_TIMER_HANDLE,
};

// TODO: This value is fixed based on experience of AWS 4vCPU TPC-C bench test.
// It's better to use a universal approach.
const CPU_LIMITER_REFILL_DURATION: Duration = Duration::from_millis(100);

// Quota limiter allows users to obtain stable performance by increasing the
// completion time of tasks through restrictions of different metrics.
#[derive(Debug)]
pub struct QuotaLimiter {
    foreground_cputime_limiter: Limiter,
    foreground_write_bandwidth_limiter: Limiter,
    foreground_read_bandwidth_limiter: Limiter,
    background_cputime_limiter: Limiter,
    background_write_bandwidth_limiter: Limiter,
    background_read_bandwidth_limiter: Limiter,
    // max delay nano seconds
    max_delay_duration: AtomicU64,
    // if supports auto tune
    support_auto_tune: AtomicBool,
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
            foreground_cputime_limiter: Limiter::new(f64::INFINITY),
            foreground_write_bandwidth_limiter: Limiter::new(f64::INFINITY),
            foreground_read_bandwidth_limiter: Limiter::new(f64::INFINITY),
            background_cputime_limiter: Limiter::new(f64::INFINITY),
            background_write_bandwidth_limiter: Limiter::new(f64::INFINITY),
            background_read_bandwidth_limiter: Limiter::new(f64::INFINITY),
            max_delay_duration: AtomicU64::new(0),
            support_auto_tune: AtomicBool::new(false),
        }
    }
}

impl QuotaLimiter {
    // 1000 millicpu equals to 1vCPU, 0 means unlimited
    pub fn new(
        foreground_cpu_quota: usize,
        foreground_write_bandwidth: ReadableSize,
        foreground_read_bandwidth: ReadableSize,
        background_cpu_quota: usize,
        background_write_bandwidth: ReadableSize,
        background_read_bandwidth: ReadableSize,
        max_delay_duration: ReadableDuration,
        support_auto_tune: bool,
    ) -> Self {
        let foreground_cputime_limiter =
            Limiter::builder(Self::speed_limit(foreground_cpu_quota as f64 * 1000_f64))
                .refill(CPU_LIMITER_REFILL_DURATION)
                .build();

        let foreground_write_bandwidth_limiter =
            Limiter::new(Self::speed_limit(foreground_write_bandwidth.0 as f64));

        let foreground_read_bandwidth_limiter =
            Limiter::new(Self::speed_limit(foreground_read_bandwidth.0 as f64));

        let background_cputime_limiter =
            Limiter::builder(Self::speed_limit(background_cpu_quota as f64 * 1000_f64))
                .refill(CPU_LIMITER_REFILL_DURATION)
                .build();

        let background_write_bandwidth_limiter =
            Limiter::new(Self::speed_limit(background_write_bandwidth.0 as f64));

        let background_read_bandwidth_limiter =
            Limiter::new(Self::speed_limit(background_read_bandwidth.0 as f64));

        let max_delay_duration = AtomicU64::new(max_delay_duration.0.as_nanos() as u64);
        let support_auto_tune = AtomicBool::new(support_auto_tune);

        Self {
            foreground_cputime_limiter,
            foreground_write_bandwidth_limiter,
            foreground_read_bandwidth_limiter,
            background_cputime_limiter,
            background_write_bandwidth_limiter,
            background_read_bandwidth_limiter,
            max_delay_duration,
            support_auto_tune,
        }
    }

    fn speed_limit(quota: f64) -> f64 {
        if quota < f64::EPSILON {
            f64::INFINITY
        } else {
            quota
        }
    }

    pub fn set_foreground_cpu_time_limit(&self, quota_limit: usize) {
        self.foreground_cputime_limiter
            .set_speed_limit(Self::speed_limit(quota_limit as f64 * 1000_f64));
    }

    pub fn set_foreground_write_bandwidth_limit(&self, write_bandwidth: ReadableSize) {
        self.foreground_write_bandwidth_limiter
            .set_speed_limit(Self::speed_limit(write_bandwidth.0 as f64));
    }

    pub fn set_foreground_read_bandwidth_limit(&self, read_bandwidth: ReadableSize) {
        self.foreground_read_bandwidth_limiter
            .set_speed_limit(Self::speed_limit(read_bandwidth.0 as f64));
    }

    pub fn set_background_cpu_time_limit(&self, quota_limit: usize) {
        self.background_cputime_limiter
            .set_speed_limit(Self::speed_limit(quota_limit as f64 * 1000_f64));
    }

    pub fn set_background_write_bandwidth_limit(&self, write_bandwidth: ReadableSize) {
        self.background_write_bandwidth_limiter
            .set_speed_limit(Self::speed_limit(write_bandwidth.0 as f64));
    }

    pub fn set_background_read_bandwidth_limit(&self, read_bandwidth: ReadableSize) {
        self.background_read_bandwidth_limiter
            .set_speed_limit(Self::speed_limit(read_bandwidth.0 as f64));
    }

    pub fn set_max_delay_duration(&self, duration: ReadableDuration) {
        self.max_delay_duration
            .store(duration.0.as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn foreground_cputime_limiter(&self) -> f64 {
        self.foreground_cputime_limiter.speed_limit()
    }

    pub fn background_cputime_limiter(&self) -> f64 {
        self.background_cputime_limiter.speed_limit()
    }

    fn max_delay_duration(&self) -> Duration {
        Duration::from_nanos(self.max_delay_duration.load(Ordering::Relaxed))
    }

    pub fn supports_auto_tune(&self) -> bool {
        self.support_auto_tune.load(Ordering::Relaxed)
    }

    // To generate a sampler.
    pub fn new_sample(&self) -> Sample {
        Sample {
            read_bytes: 0,
            write_bytes: 0,
            cpu_time: Duration::ZERO,
            enable_cpu_limit: !self.foreground_cputime_limiter.speed_limit().is_infinite()
                || !self.background_cputime_limiter.speed_limit().is_infinite(),
        }
    }

    // To consume a sampler and return delayed duration for foreground request.
    // If the sampler is null, the speed limiter will just return ZERO.
    pub async fn async_foreground_consume(&self, sample: Sample) -> Duration {
        let cpu_dur = if sample.cpu_time > Duration::ZERO {
            self.foreground_cputime_limiter
                .consume_duration(sample.cpu_time.as_micros() as usize)
        } else {
            Duration::ZERO
        };

        let w_bw_dur = if sample.write_bytes > 0 {
            self.foreground_write_bandwidth_limiter
                .consume_duration(sample.write_bytes)
        } else {
            Duration::ZERO
        };

        let r_bw_dur = if sample.read_bytes > 0 {
            self.foreground_read_bandwidth_limiter
                .consume_duration(sample.read_bytes)
        } else {
            Duration::ZERO
        };

        let mut exec_delay = std::cmp::max(cpu_dur, std::cmp::max(w_bw_dur, r_bw_dur));
        let delay_duration = self.max_delay_duration();
        if !delay_duration.is_zero() {
            exec_delay = std::cmp::min(delay_duration, exec_delay);
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

    // To consume a sampler and return delayed duration for background request.
    // If the sampler is null, the speed limiter will just return ZERO.
    pub async fn async_background_consume(&self, sample: Sample) -> Duration {
        let cpu_dur = if sample.cpu_time > Duration::ZERO {
            self.background_cputime_limiter
                .consume_duration(sample.cpu_time.as_micros() as usize)
        } else {
            Duration::ZERO
        };

        let w_bw_dur = if sample.write_bytes > 0 {
            self.background_write_bandwidth_limiter
                .consume_duration(sample.write_bytes)
        } else {
            Duration::ZERO
        };

        let r_bw_dur = if sample.read_bytes > 0 {
            self.background_read_bandwidth_limiter
                .consume_duration(sample.read_bytes)
        } else {
            Duration::ZERO
        };

        let mut exec_delay = std::cmp::max(cpu_dur, std::cmp::max(w_bw_dur, r_bw_dur));
        let delay_duration = self.max_delay_duration();
        if !delay_duration.is_zero() {
            exec_delay = std::cmp::min(delay_duration, exec_delay);
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

pub struct QuotaLimitConfigManager {
    quota_limiter: Arc<QuotaLimiter>,
}

impl QuotaLimitConfigManager {
    pub fn new(quota_limiter: Arc<QuotaLimiter>) -> Self {
        Self { quota_limiter }
    }
}

impl ConfigManager for QuotaLimitConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if let Some(cpu_limit) = change.get("foreground_cpu_time") {
            self.quota_limiter
                .set_foreground_cpu_time_limit(cpu_limit.into());
        }
        if let Some(write_bandwidth) = change.get("foreground_write_bandwidth") {
            self.quota_limiter
                .set_foreground_write_bandwidth_limit(write_bandwidth.clone().into())
        }
        if let Some(read_bandwidth) = change.get("foreground_read_bandwidth") {
            self.quota_limiter
                .set_foreground_write_bandwidth_limit(read_bandwidth.clone().into());
        }
        if let Some(duration) = change.get("max_delay_duration") {
            let delay_dur: ReadableDuration = duration.clone().into();
            self.quota_limiter
                .max_delay_duration
                .store(delay_dur.0.as_nanos() as u64, Ordering::Relaxed);
        }

        if let Some(cpu_limit) = change.get("background_cpu_time") {
            self.quota_limiter
                .set_background_cpu_time_limit(cpu_limit.into());
        }
        if let Some(write_bandwidth) = change.get("background_write_bandwidth") {
            self.quota_limiter
                .set_background_write_bandwidth_limit(write_bandwidth.clone().into())
        }
        if let Some(read_bandwidth) = change.get("background_read_bandwidth") {
            self.quota_limiter
                .set_background_read_bandwidth_limit(read_bandwidth.clone().into());
        }
        if let Some(duration) = change.get("background_max_delay_duration") {
            let delay_dur: ReadableDuration = duration.clone().into();
            self.quota_limiter
                .max_delay_duration
                .store(delay_dur.0.as_nanos() as u64, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn test_quota_limiter() {
        // refill duration = 100ms
        // bucket capacity = 100
        let quota_limiter = QuotaLimiter::new(
            1000,
            ReadableSize::kb(1),
            ReadableSize::kb(1),
            1000,
            ReadableSize::kb(1),
            ReadableSize::kb(1),
            ReadableDuration::millis(0),
            false,
        );

        let thread_start_time = ThreadTime::now();

        let check_duration = |actual: Duration, expected: Duration| {
            // time delay may be less than expected because the cpu time is keep-going.
            assert!(
                actual >= expected.saturating_sub(Duration::from_millis(5)) && actual <= expected
            );
        };

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(60));
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::ZERO);

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(50));
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::from_millis(110));

        std::thread::sleep(Duration::from_millis(10));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(20));
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        // should less 60+50+20
        assert!(should_delay < Duration::from_millis(130));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(200));
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::from_millis(250));

        // ThreadTime elapsed time is not long.
        assert!(thread_start_time.elapsed() < Duration::from_millis(50));

        quota_limiter.set_foreground_cpu_time_limit(2000);
        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(200));
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::from_millis(100));

        quota_limiter.set_foreground_read_bandwidth_limit(ReadableSize(512));
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(128);
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::from_millis(250));

        quota_limiter.set_foreground_write_bandwidth_limit(ReadableSize::kb(2));
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::from_millis(125));

        quota_limiter.set_max_delay_duration(ReadableDuration::millis(40));
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(256);
        sample.add_write_bytes(512);
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::from_millis(40));

        // test change limiter to 0
        quota_limiter.set_foreground_cpu_time_limit(0);
        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(100));
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::ZERO);

        quota_limiter.set_foreground_write_bandwidth_limit(ReadableSize::kb(0));
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::ZERO);

        quota_limiter.set_foreground_read_bandwidth_limit(ReadableSize::kb(0));
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(256);
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::ZERO);

        // set bandwidth back
        quota_limiter.set_foreground_write_bandwidth_limit(ReadableSize::kb(1));
        quota_limiter.set_max_delay_duration(ReadableDuration::millis(0));
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(128);
        let should_delay = block_on(quota_limiter.async_foreground_consume(sample));
        check_duration(should_delay, Duration::from_millis(125));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(60));
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::ZERO);

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(50));
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::from_millis(110));

        std::thread::sleep(Duration::from_millis(10));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(20));
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        // should less 60+50+20
        assert!(should_delay < Duration::from_millis(130));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(200));
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::from_millis(250));

        // ThreadTime elapsed time is not long.
        assert!(thread_start_time.elapsed() < Duration::from_millis(50));

        quota_limiter.set_background_cpu_time_limit(2000);
        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(200));
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::from_millis(100));

        quota_limiter.set_background_read_bandwidth_limit(ReadableSize(512));
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(128);
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::from_millis(250));

        quota_limiter.set_background_write_bandwidth_limit(ReadableSize::kb(2));
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::from_millis(125));

        quota_limiter.set_max_delay_duration(ReadableDuration::millis(40));
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(256);
        sample.add_write_bytes(512);
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::from_millis(40));

        // test change limiter to 0
        quota_limiter.set_background_cpu_time_limit(0);
        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(100));
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::ZERO);

        quota_limiter.set_background_write_bandwidth_limit(ReadableSize::kb(0));
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::ZERO);

        quota_limiter.set_background_read_bandwidth_limit(ReadableSize::kb(0));
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(256);
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::ZERO);

        // set bandwidth back
        quota_limiter.set_background_write_bandwidth_limit(ReadableSize::kb(1));
        quota_limiter.set_max_delay_duration(ReadableDuration::millis(0));
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(128);
        let should_delay = block_on(quota_limiter.async_background_consume(sample));
        check_duration(should_delay, Duration::from_millis(125));
    }
}

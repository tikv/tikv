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

// Limter can be issued to cpu, write and read bandwidth
#[derive(Debug)]
pub struct LimiterItems {
    cputime_limiter: Limiter,
    write_bandwidth_limiter: Limiter,
    read_bandwidth_limiter: Limiter,
}

impl LimiterItems {
    pub fn new(
        cpu_quota: usize,
        write_bandwidth: ReadableSize,
        read_bandwidth: ReadableSize,
    ) -> Self {
        let cputime_limiter =
            Limiter::builder(QuotaLimiter::speed_limit(cpu_quota as f64 * 1000_f64))
                .refill(CPU_LIMITER_REFILL_DURATION)
                .build();

        let write_bandwidth_limiter =
            Limiter::new(QuotaLimiter::speed_limit(write_bandwidth.0 as f64));

        let read_bandwidth_limiter =
            Limiter::new(QuotaLimiter::speed_limit(read_bandwidth.0 as f64));

        Self {
            cputime_limiter,
            write_bandwidth_limiter,
            read_bandwidth_limiter,
        }
    }
}

impl Default for LimiterItems {
    fn default() -> Self {
        Self {
            cputime_limiter: Limiter::new(f64::INFINITY),
            write_bandwidth_limiter: Limiter::new(f64::INFINITY),
            read_bandwidth_limiter: Limiter::new(f64::INFINITY),
        }
    }
}

// Quota limiter allows users to obtain stable performance by increasing the
// completion time of tasks through restrictions of different metrics.
#[derive(Debug)]
pub struct QuotaLimiter {
    foreground_limiters: LimiterItems,
    background_limiters: LimiterItems,
    // max delay nano seconds
    max_delay_duration: AtomicU64,
    // if auto tune is enabled
    enable_auto_tune: AtomicBool,
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
        let foreground_limiters = LimiterItems::default();
        let background_limiters = LimiterItems::default();
        Self {
            foreground_limiters,
            background_limiters,
            max_delay_duration: AtomicU64::new(0),
            enable_auto_tune: AtomicBool::new(false),
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
        enable_auto_tune: bool,
    ) -> Self {
        let foreground_limiters = LimiterItems::new(
            foreground_cpu_quota,
            foreground_write_bandwidth,
            foreground_read_bandwidth,
        );
        let background_limiters = LimiterItems::new(
            background_cpu_quota,
            background_write_bandwidth,
            background_read_bandwidth,
        );
        let max_delay_duration = AtomicU64::new(max_delay_duration.0.as_nanos() as u64);
        let enable_auto_tune = AtomicBool::new(enable_auto_tune);

        Self {
            foreground_limiters,
            background_limiters,
            max_delay_duration,
            enable_auto_tune,
        }
    }

    fn speed_limit(quota: f64) -> f64 {
        if quota < f64::EPSILON {
            f64::INFINITY
        } else {
            quota
        }
    }

    #[inline]
    fn get_limiters(&self, is_foreground: bool) -> &LimiterItems {
        if is_foreground {
            &self.foreground_limiters
        } else {
            &self.background_limiters
        }
    }

    pub fn set_cpu_time_limit(&self, quota_limit: usize, is_foreground: bool) {
        self.get_limiters(is_foreground)
            .cputime_limiter
            .set_speed_limit(Self::speed_limit(quota_limit as f64 * 1000_f64));
    }

    pub fn set_write_bandwidth_limit(&self, write_bandwidth: ReadableSize, is_foreground: bool) {
        self.get_limiters(is_foreground)
            .write_bandwidth_limiter
            .set_speed_limit(Self::speed_limit(write_bandwidth.0 as f64));
    }

    pub fn set_read_bandwidth_limit(&self, read_bandwidth: ReadableSize, is_foreground: bool) {
        self.get_limiters(is_foreground)
            .read_bandwidth_limiter
            .set_speed_limit(Self::speed_limit(read_bandwidth.0 as f64));
    }

    pub fn set_max_delay_duration(&self, duration: ReadableDuration) {
        self.max_delay_duration
            .store(duration.0.as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn set_enable_auto_tune(&self, enable_auto_tune: bool) {
        self.enable_auto_tune
            .store(enable_auto_tune, Ordering::Relaxed);
    }

    pub fn cputime_limiter(&self, is_foreground: bool) -> f64 {
        self.get_limiters(is_foreground)
            .cputime_limiter
            .speed_limit()
    }

    fn max_delay_duration(&self) -> Duration {
        Duration::from_nanos(self.max_delay_duration.load(Ordering::Relaxed))
    }

    pub fn auto_tune_enabled(&self) -> bool {
        self.enable_auto_tune.load(Ordering::Relaxed)
    }

    // To generate a sampler.
    pub fn new_sample(&self) -> Sample {
        Sample {
            read_bytes: 0,
            write_bytes: 0,
            cpu_time: Duration::ZERO,
            enable_cpu_limit: !self
                .foreground_limiters
                .cputime_limiter
                .speed_limit()
                .is_infinite()
                || !self
                    .background_limiters
                    .cputime_limiter
                    .speed_limit()
                    .is_infinite(),
        }
    }

    // To consume a sampler and return delayed duration.
    // If the sampler is null, the speed limiter will just return ZERO.
    pub async fn consume_sample(&self, sample: Sample, is_foreground: bool) -> Duration {
        let limiters = self.get_limiters(is_foreground);

        let cpu_dur = if sample.cpu_time > Duration::ZERO {
            limiters
                .cputime_limiter
                .consume_duration(sample.cpu_time.as_micros() as usize)
        } else {
            Duration::ZERO
        };

        let w_bw_dur = if sample.write_bytes > 0 {
            limiters
                .write_bandwidth_limiter
                .consume_duration(sample.write_bytes)
        } else {
            Duration::ZERO
        };

        let r_bw_dur = if sample.read_bytes > 0 {
            limiters
                .read_bandwidth_limiter
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
                .set_cpu_time_limit(cpu_limit.into(), true);
        }

        if let Some(write_bandwidth) = change.get("foreground_write_bandwidth") {
            self.quota_limiter
                .set_write_bandwidth_limit(write_bandwidth.clone().into(), true);
        }

        if let Some(read_bandwidth) = change.get("foreground_read_bandwidth") {
            self.quota_limiter
                .set_read_bandwidth_limit(read_bandwidth.clone().into(), true);
        }

        if let Some(cpu_limit) = change.get("background_cpu_time") {
            self.quota_limiter
                .set_cpu_time_limit(cpu_limit.into(), false);
        }

        if let Some(write_bandwidth) = change.get("background_write_bandwidth") {
            self.quota_limiter
                .set_write_bandwidth_limit(write_bandwidth.clone().into(), false);
        }

        if let Some(read_bandwidth) = change.get("background_read_bandwidth") {
            self.quota_limiter
                .set_read_bandwidth_limit(read_bandwidth.clone().into(), false);
        }

        if let Some(duration) = change.get("max_delay_duration") {
            let delay_dur: ReadableDuration = duration.clone().into();
            self.quota_limiter
                .max_delay_duration
                .store(delay_dur.0.as_nanos() as u64, Ordering::Relaxed);
        }

        if let Some(enable_auto_tune) = change.get("enable_auto_tune") {
            self.quota_limiter
                .set_enable_auto_tune(enable_auto_tune.clone().into());
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
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::ZERO);

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(50));
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::from_millis(110));

        std::thread::sleep(Duration::from_millis(10));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(20));
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        // should less 60+50+20
        assert!(should_delay < Duration::from_millis(130));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(200));
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::from_millis(250));

        // ThreadTime elapsed time is not long.
        assert!(thread_start_time.elapsed() < Duration::from_millis(50));

        quota_limiter.set_cpu_time_limit(2000, true);
        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(200));
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::from_millis(100));

        quota_limiter.set_read_bandwidth_limit(ReadableSize(512), true);
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(128);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::from_millis(250));

        quota_limiter.set_write_bandwidth_limit(ReadableSize::kb(2), true);
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::from_millis(125));

        quota_limiter.set_max_delay_duration(ReadableDuration::millis(40));
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(256);
        sample.add_write_bytes(512);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::from_millis(40));

        // test change limiter to 0
        quota_limiter.set_cpu_time_limit(0, true);
        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(100));
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::ZERO);

        quota_limiter.set_write_bandwidth_limit(ReadableSize::kb(0), true);
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::ZERO);

        quota_limiter.set_read_bandwidth_limit(ReadableSize::kb(0), true);
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(256);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::ZERO);

        // set bandwidth back
        quota_limiter.set_write_bandwidth_limit(ReadableSize::kb(1), true);
        quota_limiter.set_max_delay_duration(ReadableDuration::millis(0));
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(128);
        let should_delay = block_on(quota_limiter.consume_sample(sample, true));
        check_duration(should_delay, Duration::from_millis(125));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(60));
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::ZERO);

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(50));
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::from_millis(110));

        std::thread::sleep(Duration::from_millis(10));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(20));
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        // should less 60+50+20
        assert!(should_delay < Duration::from_millis(130));

        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(200));
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::from_millis(250));

        // ThreadTime elapsed time is not long.
        assert!(thread_start_time.elapsed() < Duration::from_millis(50));

        quota_limiter.set_cpu_time_limit(2000, false);
        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(200));
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::from_millis(100));

        quota_limiter.set_read_bandwidth_limit(ReadableSize(512), false);
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(128);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::from_millis(250));

        quota_limiter.set_write_bandwidth_limit(ReadableSize::kb(2), false);
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::from_millis(125));

        quota_limiter.set_max_delay_duration(ReadableDuration::millis(40));
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(256);
        sample.add_write_bytes(512);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::from_millis(40));

        // test change limiter to 0
        quota_limiter.set_cpu_time_limit(0, false);
        let mut sample = quota_limiter.new_sample();
        sample.add_cpu_time(Duration::from_millis(100));
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::ZERO);

        quota_limiter.set_write_bandwidth_limit(ReadableSize::kb(0), false);
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(256);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::ZERO);

        quota_limiter.set_read_bandwidth_limit(ReadableSize::kb(0), false);
        let mut sample = quota_limiter.new_sample();
        sample.add_read_bytes(256);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::ZERO);

        // set bandwidth back
        quota_limiter.set_write_bandwidth_limit(ReadableSize::kb(1), false);
        quota_limiter.set_max_delay_duration(ReadableDuration::millis(0));
        let mut sample = quota_limiter.new_sample();
        sample.add_write_bytes(128);
        let should_delay = block_on(quota_limiter.consume_sample(sample, false));
        check_duration(should_delay, Duration::from_millis(125));
    }
}

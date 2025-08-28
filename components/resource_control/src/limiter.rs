// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Add, Deref, Sub},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use futures::compat::Future01CompatExt;
use tikv_util::{
    error, info, resource_control::DEFAULT_RESOURCE_GROUP_NAME, timer::GLOBAL_TIMER_HANDLE,
};

use crate::{ACTIVE_RESOURCE_GROUP_READ_BYTES, AtomicDuration, AtomicTime, Config, TimeUnit};

const MIN_WAIT_TIME_INTERVAL: Duration = Duration::from_millis(1);

pub const MAX_WAIT_TIME: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum Action {
    None,
    ScaleOut,
    Throttle,
    Reject,
}

#[derive(Clone)]
pub struct ReadLimiter {
    pub(crate) core: Arc<ReadLimiterCore>,
}

pub struct ReadLimiterCore {
    pub(crate) enabled: AtomicBool,
    pub(crate) timeout: AtomicDuration,
    pub(crate) stats_interval: AtomicDuration,
    pub(crate) max_wait_time: AtomicDuration,
    pub(crate) resource_group_limiters: DashMap<String, (Instant, ResourceGroupReadLimiter)>,
}

impl Deref for ReadLimiter {
    type Target = ReadLimiterCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl ReadLimiterCore {
    pub fn new(config: Config) -> Self {
        let timeout = config.limiter_timeout.0;
        let stats_interval = config.limiter_stats_interval.0;
        Self {
            enabled: AtomicBool::from(config.enabled),
            timeout: AtomicDuration::new(timeout, TimeUnit::Millisecond),
            stats_interval: AtomicDuration::new(stats_interval, TimeUnit::Millisecond),
            max_wait_time: AtomicDuration::new(MAX_WAIT_TIME, TimeUnit::Millisecond),
            resource_group_limiters: DashMap::new(),
        }
    }
}

impl ReadLimiter {
    pub fn new(config: Config) -> Self {
        Self {
            core: Arc::new(ReadLimiterCore::new(config)),
        }
    }

    pub(crate) fn update_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Relaxed);
        if !enabled {
            self.clear_all_limiter();
        }
    }

    pub(crate) fn get_enabled(&self) -> bool {
        self.enabled.load(Relaxed)
    }

    pub(crate) fn update_timeout(&self, timeout: Duration) {
        self.timeout.store(timeout);
    }

    pub(crate) fn update_stats_interval(&self, stats_interval: Duration) {
        self.stats_interval.store(stats_interval);
    }

    pub(crate) fn update_max_wait_time(&self, max_wait_time: Duration) {
        self.max_wait_time.store(max_wait_time);
    }

    pub(crate) fn clear_all_limiter(&self) {
        self.resource_group_limiters.iter().for_each(|group_limiter_ref|{
            let name = group_limiter_ref.key();
            let _ =  ACTIVE_RESOURCE_GROUP_READ_BYTES.remove_label_values(&[name.as_str()]).map_err(
                |err| error!("failed to remove active resource group read bytes metric"; "resource_group" => name, "err" => %err),
            );
        });
        self.resource_group_limiters.clear();
    }

    pub(crate) fn update_limit(
        &self,
        name: &str,
        req_speed_limit: Option<f64>,
        bytes_speed_limit: Option<f64>,
        instant: Instant,
    ) {
        if !self.get_enabled() {
            return;
        }
        self.resource_group_limiters
            .entry(name.to_string())
            .and_modify(|(ts, read_limiter)| {
                *ts = instant;
                read_limiter.set_speed_limit(req_speed_limit, bytes_speed_limit);
                read_limiter.update_stats_interval(self.stats_interval.load());
                read_limiter.update_max_wait_time(self.max_wait_time.load());
            })
            .or_insert_with(|| {
                let group_read_limiter = ResourceGroupReadLimiter::new(
                    name.to_string(),
                    self.stats_interval.load(),
                    self.max_wait_time.load(),
                );
                group_read_limiter.set_speed_limit(req_speed_limit, bytes_speed_limit);
                (instant, group_read_limiter)
            });
    }

    pub fn get_limiter(&self, name: &str) -> Option<ResourceGroupReadLimiter> {
        if !self.get_enabled() {
            return None;
        }
        let timeout = self.timeout.load();
        if let Some(entry) = self.resource_group_limiters.get(name) {
            let (ts, read_limiter) = entry.value();
            if ts.elapsed() < timeout {
                Some(read_limiter.clone())
            } else {
                drop(entry);
                self.remove_limiter(name);
                None
            }
        } else {
            None
        }
    }

    fn remove_limiter(&self, name: &str) {
        self.resource_group_limiters.remove(name);
        let _ =  ACTIVE_RESOURCE_GROUP_READ_BYTES.remove_label_values(&[name]).map_err(
            |err| error!("failed to remove active resource group read bytes metric"; "resource_group" => name, "err" => %err),
        );
    }
}

#[derive(Clone)]
pub struct ResourceGroupReadLimiter {
    core: Arc<ResourceGroupReadLimiterCore>,
}

pub struct ResourceGroupReadLimiterCore {
    name: String,
    req_limiter: tikv_util::time::Limiter,
    bytes_limiter: tikv_util::time::Limiter,
    allowed_time: AtomicTime, // Requests after the allowed time do not need to wait.
    last_time: AtomicTime,
    stats_interval: AtomicDuration,
    max_wait_time: AtomicDuration,
}

impl Deref for ResourceGroupReadLimiter {
    type Target = ResourceGroupReadLimiterCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl ResourceGroupReadLimiterCore {
    pub fn new(name: String, stats_interval: Duration, max_wait_time: Duration) -> Self {
        let req_limiter = <tikv_util::time::Limiter>::builder(f64::INFINITY).build();
        let bytes_limiter = <tikv_util::time::Limiter>::builder(f64::INFINITY).build();
        let start = Instant::now();
        Self {
            name,
            req_limiter,
            bytes_limiter,
            allowed_time: AtomicTime::new(start, TimeUnit::Microsecond),
            last_time: AtomicTime::new(start, TimeUnit::Millisecond),
            stats_interval: AtomicDuration::new(stats_interval, TimeUnit::Millisecond),
            max_wait_time: AtomicDuration::new(max_wait_time, TimeUnit::Millisecond),
        }
    }
}

impl Default for ResourceGroupReadLimiter {
    fn default() -> Self {
        Self::new(
            DEFAULT_RESOURCE_GROUP_NAME.to_string(),
            Duration::from_secs(1),
            MAX_WAIT_TIME,
        )
    }
}

impl ResourceGroupReadLimiter {
    pub fn new(name: String, stats_interval: Duration, max_wait_time: Duration) -> Self {
        Self {
            core: Arc::new(ResourceGroupReadLimiterCore::new(
                name,
                stats_interval,
                max_wait_time,
            )),
        }
    }

    pub(crate) fn update_allowed_time<F>(&self, update: F) -> Duration
    where
        F: Fn(Instant) -> (Option<Instant>, Duration),
    {
        let mut allowed_time = self.allowed_time.load();
        loop {
            let (new_allowed_time, dur) = update(allowed_time);
            let Some(new_allowed_time) = new_allowed_time else {
                return dur;
            };
            match self
                .allowed_time
                .compare_exchange(allowed_time, new_allowed_time)
            {
                Some(current) => {
                    allowed_time = current;
                }
                None => {
                    return dur;
                }
            }
        }
    }

    pub fn take_wait_time(&self) -> Duration {
        let update = |allowed_time: Instant| {
            let now = Instant::now();
            let dur = allowed_time.duration_since(now);
            if dur < MIN_WAIT_TIME_INTERVAL {
                return (None, dur);
            }
            let new_allowed_time = now;
            (Some(new_allowed_time), dur)
        };
        self.update_allowed_time(update)
    }

    pub fn wait_time(&self) -> Duration {
        let update = |allowed_time: Instant| {
            let now = Instant::now();
            let dur = allowed_time.duration_since(now);
            if dur < MIN_WAIT_TIME_INTERVAL {
                return (None, dur);
            };
            let new_allowed_time = allowed_time.sub(MIN_WAIT_TIME_INTERVAL);
            (Some(new_allowed_time), MIN_WAIT_TIME_INTERVAL)
        };
        self.update_allowed_time(update)
    }

    pub async fn wait(&self) -> Duration {
        let mut wait_time = Duration::default();
        loop {
            let dur = self.wait_time();
            if dur.is_zero() {
                break;
            }
            let _ = GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + dur)
                .compat()
                .await;
            // tokio::time::sleep(dur).await;
            wait_time = wait_time.add(dur);
            if wait_time >= self.max_wait_time.load() {
                break;
            }
        }
        wait_time
    }

    pub fn consume(&self, bytes: usize) {
        let dur = self
            .req_limiter
            .consume_duration(1)
            .max(self.bytes_limiter.consume_duration(bytes));

        let update = |allowed_time: Instant| {
            let now = Instant::now();
            let new_allowed_time = if allowed_time.duration_since(now).is_zero() {
                now.add(dur)
            } else {
                allowed_time.add(dur)
            };
            (Some(new_allowed_time), dur)
        };
        self.update_allowed_time(update);
    }

    pub fn unconsume(&self, bytes: usize) {
        self.req_limiter.unconsume(1);
        self.bytes_limiter.unconsume(bytes);
    }

    pub fn is_unlimited(&self) -> bool {
        self.req_limiter.speed_limit() == f64::INFINITY
            && self.bytes_limiter.speed_limit() == f64::INFINITY
    }

    pub fn total_consumed(&self) -> (usize /* req */, usize /* bytes */) {
        (
            self.req_limiter.total_bytes_consumed(),
            self.bytes_limiter.total_bytes_consumed(),
        )
    }

    pub fn speed_limit(&self) -> (f64 /* req */, f64 /* bytes */) {
        (
            self.req_limiter.speed_limit(),
            self.bytes_limiter.speed_limit(),
        )
    }

    pub fn set_speed_limit(&self, req_speed_limit: Option<f64>, bytes_speed_limit: Option<f64>) {
        if let Some(req_speed_limit) = req_speed_limit {
            if req_speed_limit > 0.0 {
                self.req_limiter.set_speed_limit(req_speed_limit);
            } else {
                self.req_limiter.set_speed_limit(f64::INFINITY);
            }
        }
        if let Some(bytes_speed_limit) = bytes_speed_limit {
            if bytes_speed_limit > 0.0 {
                self.bytes_limiter.set_speed_limit(bytes_speed_limit);
            } else {
                self.bytes_limiter.set_speed_limit(f64::INFINITY);
            }
        }
        self.update_statistics();
    }

    fn update_statistics(&self) {
        let last_time = self.last_time.load();
        let dur = last_time.elapsed();
        if dur < self.stats_interval.load() {
            return;
        }
        self.last_time.store(Instant::now());
        let total_requests = self.req_limiter.total_bytes_consumed() as f64;
        let total_bytes = self.bytes_limiter.total_bytes_consumed() as f64;
        self.req_limiter.reset_statistics();
        self.bytes_limiter.reset_statistics();
        if total_requests == 0.0 {
            return;
        }
        let qps = total_requests / dur.as_secs_f64();
        let bytes_rate = total_bytes / dur.as_secs_f64();
        let bytes_per_req = total_bytes / total_requests;
        info!("resource control update_statistics dur {:?}", dur;
            "resource_group" => &self.name,
            "total_requests" => total_requests,
            "total_bytes" => total_bytes,
            "qps" => qps,
            "bytes_rate" => bytes_rate,
            "bytes_per_req" => bytes_per_req,
        );
    }

    fn update_stats_interval(&self, stats_interval: Duration) {
        self.stats_interval.store(stats_interval);
    }

    fn update_max_wait_time(&self, max_wait_time: Duration) {
        self.max_wait_time.store(max_wait_time);
    }
}

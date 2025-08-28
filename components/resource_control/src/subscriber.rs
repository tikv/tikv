// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use tikv_util::{info, sys::SysQuota};

use crate::{
    ACTIVE_RESOURCE_GROUP_READ_BYTES, Config, ReadLimiter, ResourceEvent, Severity,
    SeverityThreshold,
    usage::{ResourceGroupInstantUsages, Usages},
};

#[async_trait::async_trait]
pub trait ResourceSubscriber: Send + Sync {
    fn on_event(&mut self, event: &ResourceEvent);
}

pub struct ReadSubscriberData {
    pub(crate) config: Config,
    pub(crate) max_cpu_time: f64,
    pub(crate) max_read_bytes: u64,
    pub(crate) severity_threshold: SeverityThreshold,
    pub(crate) cpu_severity: Severity,
    pub(crate) usages: ResourceGroupInstantUsages,
    pub(crate) read_limiter: ReadLimiter,
}

impl ReadSubscriberData {
    pub fn new(read_limiter: ReadLimiter, config: Config) -> Self {
        let mut subscriber = ReadSubscriberData {
            config: config.clone(),
            max_cpu_time: Default::default(),
            max_read_bytes: Default::default(),
            severity_threshold: Default::default(),
            cpu_severity: Default::default(),
            usages: Default::default(),
            read_limiter,
        };
        subscriber.update_config(&config);
        subscriber
    }

    pub fn update_config(&mut self, new_config: &Config) {
        let window_secs = new_config.window_size.as_secs_f64();
        let cpu_cores = SysQuota::cpu_cores_quota().max(1.0);
        self.max_cpu_time = cpu_cores * new_config.max_read_cpu_ratio * window_secs;
        self.max_read_bytes = (new_config.max_read_bytes_per_core_per_second.0 as f64
            * cpu_cores
            * window_secs) as u64;
        let severity_threshold = SeverityThreshold {
            stressed: new_config.severity_threshold_stressed,
            critical: new_config.severity_threshold_critical,
            exhausted: new_config.severity_threshold_exhausted,
        };
        self.severity_threshold = severity_threshold;
        if self.config.enabled != new_config.enabled {
            self.read_limiter.update_enabled(new_config.enabled);
        }
        if self.config.limiter_timeout != new_config.limiter_timeout {
            self.read_limiter
                .update_timeout(new_config.limiter_timeout.0);
        }
        if self.config.limiter_stats_interval != new_config.limiter_stats_interval {
            self.read_limiter
                .update_stats_interval(new_config.limiter_stats_interval.0);
        }
        if self.config.max_read_wait_time != new_config.max_read_wait_time {
            self.read_limiter
                .update_max_wait_time(new_config.max_read_wait_time.0);
        }
        self.config = new_config.clone()
    }

    pub fn on_usages(&mut self, usages: &Usages) {
        match usages {
            Usages::Global { usages } => {
                let cpu_time = usages.cpu_time();

                let read_pool_cpu_ratio = cpu_time / self.max_cpu_time;
                self.cpu_severity =
                    Severity::get_severity(read_pool_cpu_ratio, self.severity_threshold);
                info!(
                    "read subscriber on_usages Global {:?} {:?} cpu_time {} max_cpu_time {} read_pool_cpu_ratio {} cpu_severity {:?}",
                    usages.resource_type,
                    usages.total(),
                    cpu_time,
                    self.max_cpu_time,
                    read_pool_cpu_ratio,
                    self.cpu_severity,
                );
            }
            Usages::ResourceGroup { usages } => {
                self.usages = usages.clone();
                info!(
                    "read subscriber on_usages ResoureGroup {:?} {:?} cpu_severity {:?}",
                    usages.resource_type,
                    usages.total(),
                    self.cpu_severity
                );
                if !self.cpu_severity.is_abnormal() || self.usages.total() == 0 {
                    return;
                }

                let bytes_usage = usages.bytes();
                let heavy_usage = bytes_usage / 100;
                let mut group_usages = usages
                    .usages
                    .iter()
                    .map(|(group, usages)| (group.clone(), usages.bytes(), usages.qps()))
                    .clone()
                    .collect::<Vec<_>>();
                group_usages.retain(|(_, bytes, _)| *bytes > heavy_usage);
                group_usages.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));
                let max_read_bytes = self.max_read_bytes.max(bytes_usage) as f64;
                let active_quota_read_bytes =
                    (max_read_bytes * self.config.active_quota_ratio) as u64;
                let min_read_bytes_quota = (max_read_bytes * self.config.min_quota_ratio) as u64;
                let mut active_bytes = 0u64;
                let mut active_pos = 0usize;
                for (name, bytes, _) in group_usages.iter() {
                    ACTIVE_RESOURCE_GROUP_READ_BYTES
                        .with_label_values(&[name])
                        .inc_by(*bytes);
                    active_bytes += *bytes;
                    active_pos += 1;
                    if active_bytes > active_quota_read_bytes {
                        break;
                    }
                }
                info!(
                    "read subscriber active read quota, active_bytes:{} active_pos:{}",
                    active_bytes, active_pos
                );
                let now = Instant::now();
                let factor = match self.cpu_severity {
                    Severity::Normal => {
                        return;
                    }
                    Severity::Stressed => {
                        let factor = 1.1 * self.config.severity_stressed_factor;
                        if group_usages.is_empty() {
                            return;
                        }
                        let (name, _, qps) = &group_usages[0];
                        self.read_limiter.update_limit(
                            name,
                            Some(*qps as f64 * factor),
                            Some(f64::INFINITY),
                            now,
                        );
                        info!(
                            "read subscriber active read quota, resource_group: {}, qps: {}",
                            name, qps
                        );
                        return;
                    }
                    Severity::Critical => 0.9 * self.config.severity_critical_factor,
                    Severity::Exhausted => 0.5 * self.config.severity_exhausted_factor,
                };
                let group_read_bytes_quota = active_bytes / active_pos as u64;
                let window_secs = self.config.window_size.as_secs_f64();
                for (idx, (group, bytes, qps)) in group_usages.iter().enumerate() {
                    if idx > active_pos {
                        break;
                    }
                    let mut target_bytes = (*bytes as f64 * self.config.smoothing_factor
                        + group_read_bytes_quota as f64 * (1.0 - self.config.smoothing_factor))
                        / window_secs;
                    let qps = *qps as f64 * factor;
                    target_bytes =
                        target_bytes.max(min_read_bytes_quota as f64 / window_secs) * factor;
                    info!(
                        "read subscriber active read quota, resource_group: {}, qps: {}, target_bytes: {}",
                        group, qps, target_bytes
                    );
                    self.read_limiter
                        .update_limit(group, Some(qps), Some(target_bytes), now);
                }
            }
        };
    }
}

#[derive(Clone)]
pub struct ReadSubscriber {
    pub(crate) data: Arc<Mutex<ReadSubscriberData>>,
}

impl ReadSubscriber {
    pub fn new(read_limiter: ReadLimiter, config: Config) -> Self {
        let mut subscriber = ReadSubscriber {
            data: Arc::new(Mutex::new(ReadSubscriberData::new(
                read_limiter,
                config.clone(),
            ))),
        };
        subscriber.update_config(&config);
        subscriber
    }

    pub fn update_config(&mut self, config: &Config) {
        let mut data = self.data.lock().unwrap();
        data.update_config(config);
    }

    pub fn on_usages(&mut self, usages: &Usages) {
        let mut data = self.data.lock().unwrap();
        data.on_usages(usages);
    }
}

#[async_trait::async_trait]
impl ResourceSubscriber for ReadSubscriber {
    fn on_event(&mut self, event: &ResourceEvent) {
        match event {
            ResourceEvent::DispatchUsages(usages) => {
                self.on_usages(usages);
            }
            ResourceEvent::UpdateConfig(config) => self.update_config(config),
            _ => {}
        }
    }
}

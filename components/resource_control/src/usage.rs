// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use tikv_util::info;

use crate::{Config, Metric, ResourceType, ResourceValue, Scope, SeverityThreshold, Usage};

#[derive(Clone)]
pub enum Usages {
    Global { usages: GlobalInstantUsages },
    ResourceGroup { usages: ResourceGroupInstantUsages },
}

#[derive(Clone, Default)]
pub struct InstantUsages {
    pub(crate) usages: Vec<InstantUsage>,
}

impl InstantUsages {
    pub(crate) fn total(&self) -> usize {
        self.usages.len()
    }

    pub(crate) fn duration(&self) -> Duration {
        let total = self.total();
        if total > 1 {
            let oldest_instant_usage = &self.usages[0];
            let latest_instant_usage = &self.usages[total - 1];
            {
                return latest_instant_usage
                    .instant
                    .duration_since(oldest_instant_usage.instant);
            }
        }
        Duration::default()
    }

    pub(crate) fn qps(&self) -> usize {
        let dur = self.duration();
        if !dur.is_zero() {
            return self.total() / dur.as_secs() as usize;
        }
        0
    }

    pub(crate) fn cpu_time(&self) -> f64 {
        let total = self.total();
        if total > 1 {
            let oldest_instant_usage = &self.usages[0];
            let latest_instant_usage = &self.usages[total - 1];
            {
                return latest_instant_usage
                    .usage
                    .subtract(&oldest_instant_usage.usage)
                    .cpu_time();
            }
        }
        0.0
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.usages.iter().map(|v| v.usage.bytes()).sum::<u64>()
    }

    #[allow(dead_code)]
    pub(crate) fn latest_bytes(&self) -> u64 {
        let total = self.total();
        if total > 0 {
            return self.usages[total - 1].usage.bytes();
        }
        0
    }
}

#[derive(Clone, Default)]
pub struct GlobalInstantUsages {
    pub(crate) resource_type: ResourceType,
    pub(crate) usages: InstantUsages,
}

impl GlobalInstantUsages {
    #[allow(dead_code)]
    pub(crate) fn total(&self) -> usize {
        self.usages.total()
    }

    pub(crate) fn cpu_time(&self) -> f64 {
        self.usages.cpu_time()
    }

    #[allow(dead_code)]
    pub(crate) fn latest_bytes(&self) -> u64 {
        self.usages.latest_bytes()
    }
}

#[derive(Clone, Default)]
pub struct ResourceGroupInstantUsages {
    pub(crate) resource_type: ResourceType,
    pub(crate) usages: Vec<(String, InstantUsages)>,
}

impl ResourceGroupInstantUsages {
    pub(crate) fn total(&self) -> usize {
        self.usages
            .iter()
            .map(|(_, usages)| usages.total())
            .sum::<usize>()
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.usages
            .iter()
            .map(|(_, usages)| usages.bytes())
            .sum::<u64>()
    }
}

#[derive(Clone)]
pub struct InstantUsage {
    pub(crate) instant: Instant,
    pub(crate) usage: ResourceValue,
}

#[derive(Clone, Default)]
pub struct WindowUsages {
    pub(crate) resource_type: ResourceType,
    pub(crate) queue: VecDeque<InstantUsage>,
}

impl WindowUsages {
    fn total(&self) -> usize {
        self.queue.len()
    }

    fn usage(&self) -> GlobalInstantUsages {
        let usages = self.queue.iter().cloned().collect();
        GlobalInstantUsages {
            resource_type: self.resource_type,
            usages: InstantUsages { usages },
        }
    }

    fn evict_old(&mut self, window_size: Duration, now: Instant) {
        while let Some(instant_usage) = self.queue.front() {
            if now.duration_since(instant_usage.instant) > window_size {
                self.queue.pop_front();
            } else {
                break;
            }
        }
    }

    fn append(&mut self, window_size: Duration, now: Instant, usage: Usage) {
        if self.resource_type.is_unknown() {
            self.resource_type = usage.resource_type;
        }
        self.evict_old(window_size, now);
        self.queue.push_back(InstantUsage {
            instant: now,
            usage: usage.resource_value,
        });
    }
}

#[derive(Default)]
pub struct GlobalUsages {
    pub(crate) usages: HashMap<ResourceType, WindowUsages>,
}

impl GlobalUsages {
    fn total(&self) -> usize {
        self.usages.values().map(|v| v.total()).sum()
    }

    pub(crate) fn usage(&self) -> Vec<GlobalInstantUsages> {
        self.usages
            .values()
            .map(|window_usages| window_usages.usage())
            .collect()
    }

    fn evict_old(&mut self, window_size: Duration, now: Instant) {
        self.usages.retain(|_, window_usages| {
            window_usages.evict_old(window_size, now);
            window_usages.total() > 0
        })
    }

    fn append(&mut self, window_size: Duration, now: Instant, usage: Usage) {
        let window_usages = self.usages.entry(usage.resource_type).or_default();
        window_usages.append(window_size, now, usage);
    }
}

#[derive(Default)]
pub struct ResourceGroupUsages {
    pub(crate) usages: HashMap<ResourceType, HashMap<String, WindowUsages>>,
}

impl ResourceGroupUsages {
    fn total(&self) -> usize {
        self.usages
            .values()
            .map(|group_usages| {
                group_usages
                    .values()
                    .map(|usages| usages.total())
                    .sum::<usize>()
            })
            .sum::<usize>()
    }

    pub(crate) fn usage(&self) -> Vec<ResourceGroupInstantUsages> {
        self.usages
            .iter()
            .map(|(resource_type, keyspace_usages)| {
                let usages = keyspace_usages
                    .iter()
                    .map(|(name, usages)| (name.clone(), usages.usage().usages))
                    .collect();
                ResourceGroupInstantUsages {
                    resource_type: *resource_type,
                    usages,
                }
            })
            .collect()
    }

    fn evict_old(&mut self, window_size: Duration, now: Instant) {
        self.usages.retain(|_, keyspace_usages| {
            keyspace_usages.retain(|_, usages| {
                usages.evict_old(window_size, now);
                usages.total() > 0
            });
            !keyspace_usages.is_empty()
        })
    }

    fn append(&mut self, group_name: &str, window_size: Duration, now: Instant, usage: Usage) {
        let keyspaces = self.usages.entry(usage.resource_type).or_default();
        let window_usages = keyspaces.entry(group_name.to_string()).or_default();
        window_usages.append(window_size, now, usage);
    }
}

pub struct ResourceUsage {
    pub(crate) global_usages: GlobalUsages,
    pub(crate) resource_group_usages: ResourceGroupUsages,
    pub(crate) window_size: Duration,
    pub(crate) smoothing_factor: f64, /* The `smoothing_factor` is used to control the weight
                                       * ratio between the
                                       * old value and the new target value */
    pub(crate) active_quota_ratio: f64, /* The `active_quota_ratio` represents the proportion of
                                         * the
                                         * active usage to the total quota. */
    pub(crate) min_quota_ratio: f64, /* The `min_quota_ratio` represents the proportion of the
                                      * min usage to the total quota. */
    pub(crate) severity_threshold: SeverityThreshold,
    pub(crate) severity_stressed_factor: f64,
    pub(crate) severity_critical_factor: f64,
    pub(crate) severity_exhausted_factor: f64,
}

impl ResourceUsage {
    pub(crate) fn new(config: Config) -> Self {
        let severity_threshold = SeverityThreshold {
            stressed: config.severity_threshold_stressed,
            critical: config.severity_threshold_critical,
            exhausted: config.severity_threshold_exhausted,
        };
        info!(
            "resource control config:{:?}, severity threshold {:?}",
            config, severity_threshold
        );
        Self {
            global_usages: GlobalUsages::default(),
            resource_group_usages: ResourceGroupUsages::default(),
            window_size: config.window_size.0,
            smoothing_factor: config.smoothing_factor,
            active_quota_ratio: config.active_quota_ratio,
            min_quota_ratio: config.min_quota_ratio,
            severity_threshold,
            severity_stressed_factor: config.severity_stressed_factor,
            severity_critical_factor: config.severity_critical_factor,
            severity_exhausted_factor: config.severity_exhausted_factor,
        }
    }

    pub(crate) fn update_window_size(&mut self, window_size: Duration) {
        self.window_size = window_size;
    }

    pub(crate) fn update_smoothing_factor(&mut self, smoothing_factor: f64) {
        self.smoothing_factor = smoothing_factor;
    }

    pub(crate) fn update_active_quota_ratio(&mut self, active_quota_ratio: f64) {
        self.active_quota_ratio = active_quota_ratio;
    }

    pub(crate) fn update_min_quota_ratio(&mut self, min_quota_ratio: f64) {
        self.min_quota_ratio = min_quota_ratio;
    }

    pub(crate) fn get_resource_group_usage(
        &self,
        name: &str,
        resource_type: ResourceType,
    ) -> Option<GlobalInstantUsages> {
        self.resource_group_usages
            .usages
            .get(&resource_type)
            .and_then(|usages| usages.get(name).map(|window_usages| window_usages.usage()))
    }

    pub(crate) fn total(&self) -> usize {
        self.resource_group_usages.total() + self.global_usages.total()
    }

    pub(crate) fn usage(&self) -> (Vec<GlobalInstantUsages>, Vec<ResourceGroupInstantUsages>) {
        let global_usages = self.global_usages.usage();
        let keyspace_usages = self.resource_group_usages.usage();
        (global_usages, keyspace_usages)
    }

    fn evict_old(&mut self) {
        let window_size = self.window_size;
        let now = Instant::now();
        self.global_usages.evict_old(window_size, now);
        self.resource_group_usages.evict_old(window_size, now);
    }

    pub(crate) fn on_metric(&mut self, metric: &Metric) {
        let now = Instant::now();
        match &metric.scope {
            Scope::Global => self.on_global_metric(metric, now),
            Scope::ResourceGroup { name } => {
                self.on_resource_group_metric(name, metric, now);
            }
        };
    }

    fn on_global_metric(&mut self, metric: &Metric, now: Instant) {
        let window_size = self.window_size;
        let resource_usage = metric.resource.resource_usage();
        self.global_usages.append(window_size, now, resource_usage);
    }

    fn on_resource_group_metric(&mut self, name: &str, metric: &Metric, now: Instant) {
        let window_size = self.window_size;
        let resource_usage = metric.resource.resource_usage();
        self.resource_group_usages
            .append(name, window_size, now, resource_usage);
    }

    pub(crate) fn on_tick(&mut self) {
        info!("resource control on tick metrics {}", self.total());
        self.evict_old();
    }
}

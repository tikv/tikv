// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{error::Error, fmt, sync::Arc, time::Duration};

use online_config::{ConfigChange, ConfigValue, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::{
    config::{ReadableDuration, ReadableSize, VersionTrack},
    info,
};

use crate::{ResourceEvent, ResourcePublisher, SeverityThreshold, limiter::MAX_WAIT_TIME};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,
    pub priority_ctl_strategy: PriorityCtlStrategy,

    // following are instance level resource control parameters.
    pub dry_run: bool,
    pub debug: bool,
    pub window_size: ReadableDuration,
    pub stats_interval: ReadableDuration,
    pub limiter_timeout: ReadableDuration,
    pub limiter_stats_interval: ReadableDuration,
    pub smoothing_factor: f64,
    pub active_quota_ratio: f64,
    pub min_quota_ratio: f64,
    pub severity_stressed_factor: f64,
    pub severity_critical_factor: f64,
    pub severity_exhausted_factor: f64,
    pub severity_threshold_stressed: f64,
    pub severity_threshold_critical: f64,
    pub severity_threshold_exhausted: f64,
    pub ignore_resource_groups: String,
    pub max_read_cpu_ratio: f64,
    pub max_read_bytes_factor: f64,
    pub max_read_bytes_per_core_per_second: ReadableSize,
    pub max_read_wait_time: ReadableDuration,
}

impl Default for Config {
    fn default() -> Self {
        let severity_threshold = SeverityThreshold::default();
        Self {
            enabled: true,
            priority_ctl_strategy: PriorityCtlStrategy::Moderate,

            dry_run: false,
            debug: false,
            window_size: ReadableDuration::minutes(1),
            stats_interval: ReadableDuration::secs(1),
            limiter_timeout: ReadableDuration::secs(5),
            limiter_stats_interval: ReadableDuration::secs(1),
            smoothing_factor: 0.9,
            active_quota_ratio: 0.8,
            min_quota_ratio: 0.01,
            severity_stressed_factor: 1.0,
            severity_critical_factor: 1.0,
            severity_exhausted_factor: 1.0,
            severity_threshold_stressed: severity_threshold.stressed,
            severity_threshold_critical: severity_threshold.critical,
            severity_threshold_exhausted: severity_threshold.exhausted,
            ignore_resource_groups: "[]".to_string(),
            max_read_cpu_ratio: 0.6,
            max_read_bytes_factor: 1.0,
            max_read_bytes_per_core_per_second: ReadableSize::mb(64),
            max_read_wait_time: ReadableDuration::millis(MAX_WAIT_TIME.as_millis() as u64),
        }
    }
}

impl Config {
    /// Check whether the configuration is legal.
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.window_size.0 < Duration::from_millis(1) {
            return Err("window-size cannot be less than 1 milliseconds".into());
        }
        if self.stats_interval.0 < Duration::from_millis(1) {
            return Err("stats-interval cannot be less than 1 milliseconds".into());
        }
        if self.limiter_timeout.0 < Duration::from_millis(1) {
            return Err("limiter-timeout cannot be less than 1 milliseconds".into());
        }
        if self.limiter_stats_interval.0 < Duration::from_millis(1) {
            return Err("limiter-stats-interval cannot be less than 1 milliseconds".into());
        }
        if self.smoothing_factor <= 0.0 || self.smoothing_factor >= 1.0 {
            return Err("smoothing-factor must be a float in the range (0.0, 1.0).".into());
        }
        if self.active_quota_ratio <= 0.0 || self.active_quota_ratio >= 1.0 {
            return Err("active-quota-ratio must be a float in the range (0.0, 1.0).".into());
        }
        if self.min_quota_ratio <= 0.0 || self.min_quota_ratio >= 1.0 {
            return Err("min-quota-ratio must be a float in the range (0.0, 1.0).".into());
        }
        if self.severity_threshold_stressed <= 0.0
            || self.severity_threshold_stressed >= self.severity_threshold_critical
        {
            return Err("severity-threshold-stressed must be a float in the range (0.0, severity-threshold-critical).".into());
        }
        if self.severity_threshold_critical <= self.severity_threshold_stressed
            || self.severity_threshold_critical >= self.severity_threshold_exhausted
        {
            return Err("severity-threshold-critical must be a float in the range (severity-threshold-stressed, severity-threshold-exhausted).".into());
        }
        if self.severity_threshold_exhausted <= self.severity_threshold_critical
            || self.severity_threshold_exhausted >= 1.0
        {
            return Err("severity-threshold-exhausted must be a float in the range (severity-threshold-critical, 1.0).".into());
        }
        if self.max_read_cpu_ratio <= 0.0 || self.max_read_cpu_ratio >= 1.0 {
            return Err("max-read-cpu-ratio must be a float in the range (0.0, 1.0).".into());
        }
        if self.max_read_bytes_per_core_per_second.0 == 0 {
            return Err("max-read-bytes-per-core-per-second cannot be 0".into());
        }
        if self.max_read_wait_time.0 < Duration::from_millis(1) {
            return Err("max-read-wait-time cannot be less than 1 milliseconds".into());
        }
        if let Err(e) = serde_json::from_str::<Vec<String>>(&self.ignore_resource_groups) {
            return Err(format!(
                "parse ignore-resource-groups value '{}' failed with: {}",
                &self.ignore_resource_groups, e
            )
            .into());
        }
        Ok(())
    }
}

/// PriorityCtlStrategy controls how  resource quota is granted  to low-priority
/// tasks.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PriorityCtlStrategy {
    /// Prioritize the throughput and latency of high-priority tasks, result in
    /// low-priority tasks running much slower.
    Aggressive,
    #[default]
    /// Try to balance between the latency of high-prioirty tasks and throughput
    /// of low-priority tasks.
    Moderate,
    /// Prioritize of overall throughput, the latency of high-priority tasks may
    /// be significantly impacted when the overall load is high.
    Conservative,
}

impl PriorityCtlStrategy {
    pub fn to_resource_util_percentage(self) -> f64 {
        match self {
            Self::Aggressive => 0.5,
            Self::Moderate => 0.7,
            Self::Conservative => 0.9,
        }
    }
}

impl fmt::Display for PriorityCtlStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str_value = match *self {
            Self::Aggressive => "aggressive",
            Self::Moderate => "moderate",
            Self::Conservative => "conservative",
        };
        f.write_str(str_value)
    }
}

impl From<PriorityCtlStrategy> for ConfigValue {
    fn from(v: PriorityCtlStrategy) -> Self {
        ConfigValue::String(format!("{}", v))
    }
}

impl TryFrom<ConfigValue> for PriorityCtlStrategy {
    type Error = String;
    fn try_from(v: ConfigValue) -> Result<Self, Self::Error> {
        if let ConfigValue::String(s) = v {
            match s.as_str() {
                "aggressive" => Ok(Self::Aggressive),
                "moderate" => Ok(Self::Moderate),
                "conservative" => Ok(Self::Conservative),
                s => Err(format!("invalid config value: {}", s)),
            }
        } else {
            panic!("expect ConfigValue::String, got: {:?}", v);
        }
    }
}

pub struct ResourceContrlCfgMgr {
    config: Arc<VersionTrack<Config>>,
    pub(crate) event_publisher: Arc<dyn ResourcePublisher>,
}

impl ResourceContrlCfgMgr {
    pub fn new(
        config: Arc<VersionTrack<Config>>,
        event_publisher: Arc<dyn ResourcePublisher>,
    ) -> Self {
        Self {
            config,
            event_publisher,
        }
    }
}

impl online_config::ConfigManager for ResourceContrlCfgMgr {
    fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
        let cfg_str = format!("{:?}", change);
        let res = self.config.update(|c| c.update(change));
        if res.is_ok() {
            let new_config = self.config.value().clone();
            self.event_publisher
                .publish(ResourceEvent::UpdateConfig(new_config));
            info!("update resource control config"; "change" => cfg_str);
        }
        res
    }
}

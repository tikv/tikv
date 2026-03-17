// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{error::Error, fmt, sync::Arc, time::Duration};

use online_config::{ConfigManager, ConfigValue, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::{
    config::{ReadableDuration, VersionTrack},
    info,
};

use crate::cpu_config::CpuThrottleConfig;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CpuThrottleSettings {
    pub enabled: bool,
    pub estimated_cpu_per_request_us: u64,
    pub resource_group_estimated_cpu_per_request_us: String,
    pub enable_adaptive_estimated_cpu_per_request_us: bool,
    pub stats_interval: ReadableDuration,
    pub refill_interval: ReadableDuration,
    pub enable_dynamic_adjustment: bool,
    pub high_watermark: f64,
    pub low_watermark: f64,
    pub enable_fair_allocation: bool,
    pub fair_allocation_threshold: f64,
    pub enable_burst: bool,
    pub burst_threshold: f64,
    pub enable_runtime_token_management: bool,
    pub runtime_check_interval_us: u64,
    pub additional_allocation_threshold: f64,
    pub per_allocation_us: u64,
    pub throttle_default_group: bool,
    pub default_group_weight: Option<u64>,
    pub debug: bool,
}

impl Default for CpuThrottleSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            estimated_cpu_per_request_us: 1_000,
            resource_group_estimated_cpu_per_request_us: String::new(),
            enable_adaptive_estimated_cpu_per_request_us: true,
            stats_interval: ReadableDuration::secs(1),
            refill_interval: ReadableDuration::millis(100),
            enable_dynamic_adjustment: false,
            high_watermark: 0.8,
            low_watermark: 0.5,
            enable_fair_allocation: false,
            fair_allocation_threshold: 0.7,
            enable_burst: false,
            burst_threshold: 0.9,
            enable_runtime_token_management: true,
            runtime_check_interval_us: 2_000,
            additional_allocation_threshold: 0.5,
            per_allocation_us: 4_000,
            throttle_default_group: false,
            default_group_weight: None,
            debug: false,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[online_config(skip)]
    pub enabled: bool,
    pub priority_ctl_strategy: PriorityCtlStrategy,
    pub max_read_cpu_ratio: f64,
    #[online_config(submodule)]
    pub cpu_throttle: CpuThrottleSettings,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            priority_ctl_strategy: PriorityCtlStrategy::Moderate,
            max_read_cpu_ratio: 0.6,
            cpu_throttle: CpuThrottleSettings::default(),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.max_read_cpu_ratio <= 0.0 || self.max_read_cpu_ratio > 1.0 {
            return Err("resource-control.max-read-cpu-ratio must be in range (0.0, 1.0]".into());
        }
        if self.cpu_throttle.enabled {
            if self.cpu_throttle.refill_interval.0 < Duration::from_millis(1) {
                return Err(
                    "resource-control.cpu-throttle.refill-interval must be at least 1ms".into(),
                );
            }
            if self.cpu_throttle.stats_interval.0 < Duration::from_millis(1) {
                return Err(
                    "resource-control.cpu-throttle.stats-interval must be at least 1ms".into(),
                );
            }
            if self.cpu_throttle.estimated_cpu_per_request_us == 0 {
                return Err(
                    "resource-control.cpu-throttle.estimated-cpu-per-request-us cannot be 0".into(),
                );
            }
            if self.cpu_throttle.enable_runtime_token_management {
                if self.cpu_throttle.runtime_check_interval_us == 0 {
                    return Err(
                        "resource-control.cpu-throttle.runtime-check-interval-us cannot be 0"
                            .into(),
                    );
                }
                if self.cpu_throttle.per_allocation_us < self.cpu_throttle.runtime_check_interval_us
                {
                    return Err("resource-control.cpu-throttle.per-allocation-us must be >= runtime-check-interval-us".into());
                }
                if !(0.0..=1.0).contains(&self.cpu_throttle.additional_allocation_threshold)
                    || self.cpu_throttle.additional_allocation_threshold <= 0.0
                {
                    return Err("resource-control.cpu-throttle.additional-allocation-threshold must be in range (0.0, 1.0]".into());
                }
            }
            if !(0.0..=1.0).contains(&self.cpu_throttle.high_watermark)
                || !(0.0..=1.0).contains(&self.cpu_throttle.low_watermark)
                || self.cpu_throttle.high_watermark <= self.cpu_throttle.low_watermark
            {
                return Err("resource-control.cpu-throttle watermarks are invalid".into());
            }
            if !(0.0..=1.0).contains(&self.cpu_throttle.fair_allocation_threshold) {
                return Err("resource-control.cpu-throttle.fair-allocation-threshold must be in range [0.0, 1.0]".into());
            }
            if !(0.0..=1.0).contains(&self.cpu_throttle.burst_threshold)
                || self.cpu_throttle.burst_threshold <= 0.0
            {
                return Err(
                    "resource-control.cpu-throttle.burst-threshold must be in range (0.0, 1.0]"
                        .into(),
                );
            }
            if self.cpu_throttle.throttle_default_group
                && self.cpu_throttle.default_group_weight.unwrap_or(0) == 0
            {
                return Err("resource-control.cpu-throttle.default-group-weight must be set when throttle-default-group = true".into());
            }
        }
        Ok(())
    }

    pub fn get_cpu_throttle_enabled(&self) -> bool {
        self.enabled && self.cpu_throttle.enabled
    }

    pub fn to_cpu_throttle_config(&self) -> CpuThrottleConfig {
        CpuThrottleConfig {
            enabled: self.get_cpu_throttle_enabled(),
            max_read_cpu_ratio: self.max_read_cpu_ratio,
            estimated_cpu_per_request_us: self.cpu_throttle.estimated_cpu_per_request_us,
            resource_group_estimated_cpu_per_request_us: self
                .cpu_throttle
                .resource_group_estimated_cpu_per_request_us
                .clone(),
            enable_adaptive_estimated_cpu_per_request_us: self
                .cpu_throttle
                .enable_adaptive_estimated_cpu_per_request_us,
            stats_interval_ms: self.cpu_throttle.stats_interval.as_millis() as u64,
            refill_interval_ms: self.cpu_throttle.refill_interval.as_millis() as u64,
            enable_dynamic_adjustment: self.cpu_throttle.enable_dynamic_adjustment,
            high_watermark: self.cpu_throttle.high_watermark,
            low_watermark: self.cpu_throttle.low_watermark,
            enable_fair_allocation: self.cpu_throttle.enable_fair_allocation,
            fair_allocation_threshold: self.cpu_throttle.fair_allocation_threshold,
            enable_burst: self.cpu_throttle.enable_burst,
            burst_threshold: self.cpu_throttle.burst_threshold,
            enable_runtime_token_management: self.cpu_throttle.enable_runtime_token_management,
            runtime_check_interval_us: self.cpu_throttle.runtime_check_interval_us,
            additional_allocation_threshold: self.cpu_throttle.additional_allocation_threshold,
            per_allocation_us: self.cpu_throttle.per_allocation_us,
            throttle_default_group: self.cpu_throttle.throttle_default_group,
            default_group_weight: self.cpu_throttle.default_group_weight,
            debug: self.cpu_throttle.debug,
        }
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
}

impl ResourceContrlCfgMgr {
    pub fn new(config: Arc<VersionTrack<Config>>) -> Self {
        Self { config }
    }
}

impl ConfigManager for ResourceContrlCfgMgr {
    fn dispatch(&mut self, change: online_config::ConfigChange) -> online_config::Result<()> {
        let cfg_str = format!("{:?}", change);
        let res = self.config.update(|current| {
            let mut new_config = current.clone();
            new_config.update(change)?;
            new_config.validate().map_err(|err| err.to_string())?;
            *current = new_config;
            Ok(())
        });
        if res.is_ok() {
            info!("update resource control config"; "change" => cfg_str);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use online_config::{ConfigManager as _, OnlineConfig};
    use tikv_util::config::VersionTrack;

    use super::{Config, ResourceContrlCfgMgr};

    #[test]
    fn test_validate_max_read_cpu_ratio_bounds() {
        let mut config = Config::default();
        config.max_read_cpu_ratio = 1.0;
        assert!(config.validate().is_ok());

        config.max_read_cpu_ratio = 0.0;
        assert!(config.validate().is_err());

        config.max_read_cpu_ratio = 1.01;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_manager_rejects_invalid_default_group_weight_update() {
        let config = Arc::new(VersionTrack::new(Config::default()));
        let mut manager = ResourceContrlCfgMgr::new(config.clone());
        let mut updated = Config::default();
        updated.cpu_throttle.enabled = true;
        updated.cpu_throttle.throttle_default_group = true;

        let result = manager.dispatch(Config::default().diff(&updated));
        assert!(result.is_err());
        assert!(!config.value().cpu_throttle.enabled);
        assert!(!config.value().cpu_throttle.throttle_default_group);
    }
}

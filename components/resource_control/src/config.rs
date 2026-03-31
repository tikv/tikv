// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{error::Error, fmt, sync::Arc, time::Duration};

use online_config::{ConfigManager, ConfigValue, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::{
    config::{ReadableDuration, VersionTrack},
    info,
    worker::Worker,
};

use crate::{cpu_config::CpuThrottleConfig, start_cpu_throttle_monitor, ResourceGroupManager};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CpuThrottleSettings {
    pub enabled: bool,
    pub estimated_cpu_per_request_us: u64,
    pub resource_group_estimated_cpu_per_request_us: String,
    pub resource_group_burst_enabled: String,
    pub enable_adaptive_estimated_cpu_per_request_us: bool,
    pub stats_interval: ReadableDuration,
    pub window_size: ReadableDuration,
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
            resource_group_burst_enabled: String::new(),
            enable_adaptive_estimated_cpu_per_request_us: true,
            stats_interval: ReadableDuration::secs(1),
            window_size: ReadableDuration::minutes(1),
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
        // The throttle manager is constructed whenever resource control is
        // enabled so online config can turn CPU throttling on without a
        // restart. Keep this invariant aligned with CpuThrottleManager::new.
        if self.enabled
            && self.cpu_throttle.throttle_default_group
            && self.cpu_throttle.default_group_weight.unwrap_or(0) == 0
        {
            return Err("resource-control.cpu-throttle.default-group-weight must be set when throttle-default-group = true".into());
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
            if self.cpu_throttle.window_size.0 < Duration::from_millis(1) {
                return Err(
                    "resource-control.cpu-throttle.window-size must be at least 1ms".into(),
                );
            }
            if self.cpu_throttle.window_size.0 < self.cpu_throttle.stats_interval.0 {
                return Err(
                    "resource-control.cpu-throttle.window-size must be >= stats-interval".into(),
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
            resource_group_burst_enabled: self.cpu_throttle.resource_group_burst_enabled.clone(),
            enable_adaptive_estimated_cpu_per_request_us: self
                .cpu_throttle
                .enable_adaptive_estimated_cpu_per_request_us,
            stats_interval_ms: self.cpu_throttle.stats_interval.as_millis(),
            window_size_ms: self.cpu_throttle.window_size.as_millis(),
            refill_interval_ms: self.cpu_throttle.refill_interval.as_millis(),
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
    resource_manager: Arc<ResourceGroupManager>,
    config: Arc<VersionTrack<Config>>,
    background_worker: Option<Worker>,
}

impl ResourceContrlCfgMgr {
    pub fn new(resource_manager: Arc<ResourceGroupManager>) -> Self {
        Self {
            config: resource_manager.get_config().clone(),
            resource_manager,
            background_worker: None,
        }
    }

    pub fn with_background_worker(
        resource_manager: Arc<ResourceGroupManager>,
        background_worker: Worker,
    ) -> Self {
        Self {
            config: resource_manager.get_config().clone(),
            resource_manager,
            background_worker: Some(background_worker),
        }
    }
}

impl ConfigManager for ResourceContrlCfgMgr {
    fn dispatch(&mut self, change: online_config::ConfigChange) -> online_config::Result<()> {
        let cfg_str = format!("{:?}", change);
        let old_cpu_config = self.config.value().to_cpu_throttle_config();
        let res = self.config.update(|current| {
            let mut new_config = current.clone();
            new_config.update(change)?;
            new_config.validate().map_err(|err| err.to_string())?;
            *current = new_config;
            Ok(())
        });
        if res.is_ok() {
            let new_cpu_config = self.config.value().to_cpu_throttle_config();
            if old_cpu_config != new_cpu_config {
                self.resource_manager
                    .refresh_cpu_throttle_config(new_cpu_config.clone());
                if new_cpu_config.enabled
                    && let Some(background_worker) = self.background_worker.as_ref()
                    && let Some(cpu_throttle_manager) =
                        self.resource_manager.get_cpu_throttle_manager()
                {
                    start_cpu_throttle_monitor(background_worker, cpu_throttle_manager);
                }
            }
            info!("update resource control config"; "change" => cfg_str);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use online_config::{ConfigManager as _, OnlineConfig};
    use tikv_util::{config::ReadableDuration, worker::Builder as WorkerBuilder};

    use super::{Config, ResourceContrlCfgMgr};
    use crate::{CpuThrottleManager, ResourceGroupManager};

    #[test]
    fn test_validate_max_read_cpu_ratio_bounds() {
        let mut config = Config::default();
        config.max_read_cpu_ratio = 1.0;
        config.validate().unwrap();

        config.max_read_cpu_ratio = 0.0;
        assert!(config.validate().is_err());

        config.max_read_cpu_ratio = 1.01;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_invalid_default_group_weight_when_cpu_throttle_disabled() {
        let mut config = Config::default();
        config.cpu_throttle.throttle_default_group = true;

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_allows_invalid_default_group_weight_when_resource_control_disabled() {
        let mut config = Config::default();
        config.enabled = false;
        config.cpu_throttle.throttle_default_group = true;

        config.validate().unwrap();
    }

    #[test]
    fn test_config_manager_rejects_invalid_default_group_weight_update_with_disabled_throttle() {
        let resource_manager = Arc::new(ResourceGroupManager::new(Config::default()));
        resource_manager.set_cpu_throttle_manager(Arc::new(CpuThrottleManager::new(
            Config::default().to_cpu_throttle_config(),
        )));
        let config = resource_manager.get_config().clone();
        let mut manager = ResourceContrlCfgMgr::new(resource_manager);
        let mut updated = Config::default();
        updated.cpu_throttle.throttle_default_group = true;

        let result = manager.dispatch(Config::default().diff(&updated));
        assert!(result.is_err());
        assert!(!config.value().cpu_throttle.enabled);
        assert!(!config.value().cpu_throttle.throttle_default_group);
    }

    #[test]
    fn test_validate_rejects_window_size_smaller_than_stats_interval() {
        let mut config = Config::default();
        config.cpu_throttle.enabled = true;
        config.cpu_throttle.stats_interval = ReadableDuration::secs(5);
        config.cpu_throttle.window_size = ReadableDuration::secs(1);

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_manager_refreshes_cpu_throttle_manager() {
        let resource_manager = Arc::new(ResourceGroupManager::new(Config::default()));
        resource_manager.set_cpu_throttle_manager(Arc::new(CpuThrottleManager::new(
            Config::default().to_cpu_throttle_config(),
        )));
        let mut manager = ResourceContrlCfgMgr::new(resource_manager.clone());

        let mut updated = Config::default();
        updated.max_read_cpu_ratio = 0.3;
        updated.cpu_throttle.enabled = true;
        updated.cpu_throttle.refill_interval = ReadableDuration::millis(250);
        updated.cpu_throttle.throttle_default_group = true;
        updated.cpu_throttle.default_group_weight = Some(200);

        manager.dispatch(Config::default().diff(&updated)).unwrap();

        let cpu_throttle_manager = resource_manager.get_cpu_throttle_manager().unwrap();
        assert!(cpu_throttle_manager.is_enabled());
        assert_eq!(
            cpu_throttle_manager.stats_interval(),
            ReadableDuration::secs(1).0
        );
        assert_eq!(cpu_throttle_manager.refill_interval_ms(), 250);
        assert!(cpu_throttle_manager.global_capacity_us() > 0);
        assert!(
            cpu_throttle_manager.has_resource_group_bucket(
                tikv_util::resource_control::DEFAULT_RESOURCE_GROUP_NAME
            )
        );
    }

    #[test]
    fn test_start_cpu_throttle_monitor_skips_disabled_manager() {
        let background_worker = WorkerBuilder::new("cpu-monitor-disabled")
            .thread_count(1)
            .create();
        let cpu_throttle_manager = Arc::new(CpuThrottleManager::new(
            Config::default().to_cpu_throttle_config(),
        ));

        crate::start_cpu_throttle_monitor(&background_worker, cpu_throttle_manager.clone());

        assert!(!cpu_throttle_manager.is_cpu_monitor_running());
        background_worker.stop();
    }

    #[test]
    fn test_config_manager_starts_cpu_monitor_when_throttle_enabled_online() {
        let resource_manager = Arc::new(ResourceGroupManager::new(Config::default()));
        let cpu_throttle_manager = Arc::new(CpuThrottleManager::new(
            Config::default().to_cpu_throttle_config(),
        ));
        resource_manager.set_cpu_throttle_manager(cpu_throttle_manager.clone());
        let background_worker = WorkerBuilder::new("cpu-monitor-online-enable")
            .thread_count(1)
            .create();
        let mut manager = ResourceContrlCfgMgr::with_background_worker(
            resource_manager,
            background_worker.clone(),
        );

        let mut updated = Config::default();
        updated.cpu_throttle.enabled = true;

        manager.dispatch(Config::default().diff(&updated)).unwrap();

        assert!(cpu_throttle_manager.is_cpu_monitor_running());
        background_worker.stop();
    }

    #[test]
    fn test_config_manager_refreshes_cpu_throttle_debug_flag() {
        let resource_manager = Arc::new(ResourceGroupManager::new(Config::default()));
        resource_manager.set_cpu_throttle_manager(Arc::new(CpuThrottleManager::new(
            Config::default().to_cpu_throttle_config(),
        )));
        let mut manager = ResourceContrlCfgMgr::new(resource_manager.clone());

        let mut updated = Config::default();
        updated.cpu_throttle.debug = true;

        manager.dispatch(Config::default().diff(&updated)).unwrap();

        let cpu_throttle_manager = resource_manager.get_cpu_throttle_manager().unwrap();
        assert!(cpu_throttle_manager.is_debug_logging_enabled());
    }

    #[test]
    fn test_config_manager_refreshes_resource_group_burst_override() {
        let resource_manager = Arc::new(ResourceGroupManager::new(Config::default()));
        let cpu_throttle_manager = Arc::new(CpuThrottleManager::new(
            Config::default().to_cpu_throttle_config(),
        ));
        cpu_throttle_manager.on_resource_group_changed("rg1", 100);
        resource_manager.set_cpu_throttle_manager(cpu_throttle_manager.clone());
        let mut manager = ResourceContrlCfgMgr::new(resource_manager);

        let mut updated = Config::default();
        updated.cpu_throttle.enable_burst = true;
        updated.cpu_throttle.resource_group_burst_enabled = "rg1:false".to_owned();

        manager.dispatch(Config::default().diff(&updated)).unwrap();

        assert!(!cpu_throttle_manager.is_burst_enabled_for_group("rg1"));
        assert!(cpu_throttle_manager.is_burst_enabled_for_group("missing"));
    }
}

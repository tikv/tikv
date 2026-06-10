// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{fmt, sync::Arc};

use online_config::{ConfigManager, ConfigValue, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::config::{ReadableSize, VersionTrack};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[online_config(skip)]
    pub enabled: bool,
    pub priority_ctl_strategy: PriorityCtlStrategy,
    /// CPU utilization percentage at which background task throttling begins.
    /// Background budget scales linearly from full down to zero between this
    /// value and fg_cpu_throttle_threshold.
    pub bg_cpu_throttle_threshold: f64,
    /// CPU utilization percentage at which foreground task protection kicks in:
    /// background tasks are fully throttled to their minimum floor and the
    /// background utilization limit is capped here.
    pub fg_cpu_throttle_threshold: f64,
    /// Compaction pressure percentage at which background write IO throttling
    /// begins. Dynamically configurable at runtime.
    pub bg_compaction_pressure_threshold: f64,
    /// Maximum write IO rate allowed for background tasks when
    /// compaction pressure is lower than the threshold.
    pub bg_write_io_ceiling: ReadableSize,
    /// Minimum write IO rate that background tasks are always allowed,
    /// even under maximum compaction pressure.
    pub bg_write_io_floor: ReadableSize,
    /// When true, enables fair two-phase scheduling for reads: groups whose
    /// current-minute RU rate exceeds their historical baseline are placed in
    /// phase 1 (deprioritised in the yatp priority queue) relative to groups
    /// within their baseline (phase 0). Protects sustained workloads from
    /// sudden traffic spikes without hard-rejecting requests.
    pub enable_fair_scheduling: bool,
    /// When true, enables Tier-1 admission control for reads: high-priority
    /// read requests from groups that are over their RU baseline are shed
    /// (SchedTooBusy) when CPU exceeds fg_cpu_throttle_threshold.
    pub enable_read_admission_control: bool,
    /// When true, enables Tier-1 admission control for writes: high-priority
    /// write requests from groups that are over their RU baseline are shed
    /// (SchedTooBusy) when CPU exceeds fg_cpu_throttle_threshold.
    pub enable_write_admission_control: bool,
    /// Size of the sliding window (in minutes) used to compute per-group
    /// historical RU baselines for fair scheduling and admission control.
    /// The window is divided into 30-second buckets (2 per minute). Minimum 2,
    /// maximum 60. Not hot-reloadable; changing requires a restart.
    #[online_config(skip)]
    pub historical_usage_window_mins: u64,
    /// Percentage of headroom above the historical RU baseline before a group
    /// is considered "over baseline" for two-phase scheduling and CPU
    /// utilization throttling. For example, 20.0 means a group must exceed
    /// 1.2× its historical rate to be deprioritized or rate-limited.
    /// Default: 20.0 (20%).
    pub baseline_burst_pct: f64,
    /// Maximum number of requests that can concurrently sit in the admission
    /// control delay phase (reads and writes combined). When this limit is
    /// reached, additional over-baseline requests are rejected immediately
    /// (SchedTooBusy) rather than delayed. Set to 0 to disable the limit
    /// (unlimited delayed requests). Default: 10_000.
    pub admission_max_delayed_count: u64,
    /// Number of threads in the admission thread pool. These threads run
    /// admission-control decisions (delay/reject) and then forward requests
    /// to the read pool or scheduler. Default: 2.
    #[online_config(skip)]
    pub admission_pool_threads: usize,
    /// Maximum number of requests that can be queued in the admission pool
    /// waiting for a thread to process them. Requests beyond this limit are
    /// rejected immediately (SchedTooBusy). Default: 10_000.
    pub admission_pool_max_tasks: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            priority_ctl_strategy: PriorityCtlStrategy::Moderate,
            bg_cpu_throttle_threshold: 60.0,
            fg_cpu_throttle_threshold: 70.0,
            bg_compaction_pressure_threshold: 70.0,
            bg_write_io_ceiling: ReadableSize::gb(100),
            bg_write_io_floor: ReadableSize::mb(10),
            enable_fair_scheduling: false,
            enable_read_admission_control: false,
            enable_write_admission_control: false,
            historical_usage_window_mins: 15,
            baseline_burst_pct: 20.0,
            admission_max_delayed_count: 10_000,
            admission_pool_threads: 2,
            admission_pool_max_tasks: 10_000,
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
        let res = self.config.update(|c| c.update(change));
        if res.is_ok() {
            tikv_util::info!("update resource control config"; "change" => cfg_str);
        }
        res
    }
}

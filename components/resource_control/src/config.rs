// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{error::Error, fmt, sync::Arc, time::Duration};

use online_config::{ConfigManager, ConfigValue, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::config::{ReadableDuration, ReadableSize, VersionTrack};

use crate::{CONTROL_TICK, CONTROL_TICK_OVERLOADED};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[online_config(skip)]
    pub enabled: bool,
    pub priority_ctl_strategy: PriorityCtlStrategy,
    /// How `select_noisy_groups` decides which groups caused an overload.
    pub noisy_detection: NoisyDetection,
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
    ///
    /// Requires `readpool.unified.auto-adjust-pool-size` to be enabled, which
    /// is *not* the default. A group is deprioritised while the unified read
    /// pool is scaled in and released once the pool recovers to its configured
    /// size, so with auto-adjustment off the pool never moves and no group is
    /// ever deprioritised. This is not rejected at config load, for backward
    /// compatibility, so enabling this alone silently has no effect.
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
    /// Also sizes the unified read pool's historical CPU-usage tracker, whose
    /// average is used as a scale-down floor for the pool's thread count.
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
    /// RU per arriving request, charged at handler entry so rejections pay.
    pub request_base_cost_micros: u64,
    /// Period of both control loops -- the quota worker and the unified read
    /// pool's thread ladder -- while the node is quiet. Detection needs two
    /// ticks of evidence before a group may be named, so this is what an idle
    /// node pays to notice load arriving.
    ///
    /// Not hot-reloadable; changing requires a restart.
    #[online_config(skip)]
    pub quiet_tick: ReadableDuration,
    /// Period of both control loops while the node is loaded (foreground CPU
    /// above `fg_cpu_throttle_threshold`). This, not `quiet_tick`, sets how
    /// long a noisy group goes unblamed, and health feedback reaches the
    /// client every second, so the tick is what the client waits on.
    ///
    /// Everything counted in ticks rather than seconds moves proportionally
    /// faster under load as a result: the throttle's recovery step and the
    /// candidacy counter included.
    ///
    /// Must not exceed `quiet_tick`. Not hot-reloadable; the tick task is
    /// woken at this period, and the quiet gate can defer a wakeup but cannot
    /// create one, so changing it requires a restart.
    #[online_config(skip)]
    pub overloaded_tick: ReadableDuration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            priority_ctl_strategy: PriorityCtlStrategy::Moderate,
            noisy_detection: NoisyDetection::BaselineFallbackCurrentUsage,
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
            request_base_cost_micros: 40,
            quiet_tick: ReadableDuration(CONTROL_TICK),
            overloaded_tick: ReadableDuration(CONTROL_TICK_OVERLOADED),
        }
    }
}

const MIN_CPU_PCT: f64 = 1.0;
const MAX_CPU_PCT: f64 = 99.0;
const MIN_HISTORICAL_WINDOW_MINS: u64 = 2;
const MAX_HISTORICAL_WINDOW_MINS: u64 = 60;
const MAX_REQUEST_BASE_COST_MICROS: u64 = 10_000;
/// A tick shorter than this is pointless: `adjust_quota` refuses to measure
/// over a window under a second, so the tick would fire and do nothing.
const MIN_TICK: Duration = Duration::from_secs(1);
const MAX_TICK: Duration = Duration::from_secs(60);

fn validate_tick(name: &str, value: Duration) -> Result<(), Box<dyn Error>> {
    if !(MIN_TICK..=MAX_TICK).contains(&value) {
        return Err(format!(
            "resource-control.{} must be in [{:?}, {:?}], but got {:?}",
            name, MIN_TICK, MAX_TICK, value
        )
        .into());
    }
    Ok(())
}

fn validate_cpu_pct(name: &str, value: f64) -> Result<(), Box<dyn Error>> {
    // `!is_finite()` also rejects NaN, which would otherwise compare false
    // against every threshold and silently disable the consumer.
    if !value.is_finite() || !(MIN_CPU_PCT..=MAX_CPU_PCT).contains(&value) {
        return Err(format!(
            "resource-control.{} must be a finite percentage in [{}, {}], but got {}",
            name, MIN_CPU_PCT, MAX_CPU_PCT, value
        )
        .into());
    }
    Ok(())
}

impl Config {
    /// Whether a client should be told about noisy groups: only once fair
    /// scheduling or admission control is on. See
    /// [`ResourceGroupManager::reports_noisy_groups`].
    pub fn reports_noisy_groups(&self) -> bool {
        self.enable_fair_scheduling
            || self.enable_read_admission_control
            || self.enable_write_admission_control
    }

    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        validate_cpu_pct("bg-cpu-throttle-threshold", self.bg_cpu_throttle_threshold)?;
        validate_cpu_pct("fg-cpu-throttle-threshold", self.fg_cpu_throttle_threshold)?;
        validate_cpu_pct(
            "bg-compaction-pressure-threshold",
            self.bg_compaction_pressure_threshold,
        )?;

        if self.bg_cpu_throttle_threshold > self.fg_cpu_throttle_threshold {
            return Err(format!(
                "resource-control.bg-cpu-throttle-threshold ({}) must not exceed \
                 fg-cpu-throttle-threshold ({})",
                self.bg_cpu_throttle_threshold, self.fg_cpu_throttle_threshold
            )
            .into());
        }

        if self.bg_write_io_floor.0 > self.bg_write_io_ceiling.0 {
            return Err(format!(
                "resource-control.bg-write-io-floor ({}) must not exceed \
                 bg-write-io-ceiling ({})",
                self.bg_write_io_floor, self.bg_write_io_ceiling
            )
            .into());
        }

        if !self.baseline_burst_pct.is_finite() || self.baseline_burst_pct < 0.0 {
            return Err(format!(
                "resource-control.baseline-burst-pct must be finite and non-negative, but got {}",
                self.baseline_burst_pct
            )
            .into());
        }

        if !(MIN_HISTORICAL_WINDOW_MINS..=MAX_HISTORICAL_WINDOW_MINS)
            .contains(&self.historical_usage_window_mins)
        {
            return Err(format!(
                "resource-control.historical-usage-window-mins must be in [{}, {}], but got {}",
                MIN_HISTORICAL_WINDOW_MINS,
                MAX_HISTORICAL_WINDOW_MINS,
                self.historical_usage_window_mins
            )
            .into());
        }

        validate_tick("quiet-tick", self.quiet_tick.0)?;
        validate_tick("overloaded-tick", self.overloaded_tick.0)?;

        // Loaded must tick faster, or detection slows exactly when it matters.
        if self.overloaded_tick.0 > self.quiet_tick.0 {
            return Err(format!(
                "resource-control.overloaded-tick ({}) must not exceed quiet-tick ({})",
                self.overloaded_tick, self.quiet_tick
            )
            .into());
        }

        // A cap: arrival is a fraction of a request's cost, not a multiple.
        if self.request_base_cost_micros > MAX_REQUEST_BASE_COST_MICROS {
            return Err(format!(
                "resource-control.request-base-cost-micros must not exceed {}, but got {}",
                MAX_REQUEST_BASE_COST_MICROS, self.request_base_cost_micros
            )
            .into());
        }

        Ok(())
    }
}

/// Which signal identifies the groups responsible for an overload.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NoisyDetection {
    /// Blame the group furthest above its own baseline: the one that changed.
    /// A group with no baseline yet is not a candidate, so an overload nobody
    /// has history for goes unattributed rather than being pinned on whoever
    /// is largest.
    Baseline,
    /// As `Baseline`, except a group with no baseline is judged against zero,
    /// so any traffic counts as excess and it ranks on current usage. On a
    /// node that is never quiet long enough to take a baseline, this is
    /// `CurrentUsage` for every group. Where some groups have a baseline and
    /// some do not, a cold group's whole rate is ranked against warm groups'
    /// rise above baseline, so a newly created group outranks an established
    /// one whose rise is smaller than the new group's rate.
    #[default]
    BaselineFallbackCurrentUsage,
    /// Blame the largest consumer right now, ignoring history.
    CurrentUsage,
}

impl fmt::Display for NoisyDetection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match *self {
            Self::Baseline => "baseline",
            Self::BaselineFallbackCurrentUsage => "baseline-fallback-current-usage",
            Self::CurrentUsage => "current-usage",
        })
    }
}

impl From<NoisyDetection> for ConfigValue {
    fn from(v: NoisyDetection) -> Self {
        ConfigValue::String(format!("{}", v))
    }
}

impl TryFrom<ConfigValue> for NoisyDetection {
    type Error = String;
    fn try_from(v: ConfigValue) -> Result<Self, Self::Error> {
        if let ConfigValue::String(s) = v {
            match s.as_str() {
                "baseline" => Ok(Self::Baseline),
                "baseline-fallback-current-usage" => Ok(Self::BaselineFallbackCurrentUsage),
                "current-usage" => Ok(Self::CurrentUsage),
                s => Err(format!("invalid config value: {}", s)),
            }
        } else {
            panic!("expect ConfigValue::String, got: {:?}", v);
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
        // Validated upstream; cached copies follow on the next tick.
        let res = self.config.update(|c| c.update(change));
        if res.is_ok() {
            tikv_util::info!("update resource control config"; "change" => cfg_str);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use online_config::{ConfigChange, ConfigValue};

    use super::*;

    #[test]
    fn test_validate_accepts_defaults() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn test_noisy_detection_round_trips_through_config_value() {
        // The path an online config update takes: a missing arm here silently
        // rejects the update rather than failing to compile.
        for policy in [
            NoisyDetection::Baseline,
            NoisyDetection::BaselineFallbackCurrentUsage,
            NoisyDetection::CurrentUsage,
        ] {
            let encoded = ConfigValue::from(policy);
            assert_eq!(NoisyDetection::try_from(encoded).unwrap(), policy);
        }
        // A prefix of a real value must not be accepted.
        NoisyDetection::try_from(ConfigValue::String("baseline-fallback".to_owned())).unwrap_err();
    }

    #[test]
    fn test_validate_rejects_out_of_range_cpu_thresholds() {
        for bad in [0.0, -50.0, 100.0, 150.0, f64::NAN, f64::INFINITY] {
            let mut cfg = Config::default();
            cfg.bg_cpu_throttle_threshold = bad;
            assert!(
                cfg.validate().is_err(),
                "bg_cpu_throttle_threshold {} should be rejected",
                bad
            );

            let mut cfg = Config::default();
            cfg.fg_cpu_throttle_threshold = bad;
            assert!(
                cfg.validate().is_err(),
                "fg_cpu_throttle_threshold {} should be rejected",
                bad
            );

            let mut cfg = Config::default();
            cfg.bg_compaction_pressure_threshold = bad;
            assert!(
                cfg.validate().is_err(),
                "bg_compaction_pressure_threshold {} should be rejected",
                bad
            );
        }
    }

    #[test]
    fn test_validate_rejects_inverted_cpu_thresholds() {
        let mut cfg = Config::default();
        cfg.bg_cpu_throttle_threshold = 80.0;
        cfg.fg_cpu_throttle_threshold = 20.0;
        assert!(cfg.validate().is_err());

        // Equal is allowed: background is fully throttled exactly where
        // foreground protection takes over.
        cfg.fg_cpu_throttle_threshold = 80.0;
        cfg.validate().unwrap();
    }

    #[test]
    fn test_validate_rejects_inverted_write_io_bounds() {
        let mut cfg = Config::default();
        cfg.bg_write_io_floor = ReadableSize::gb(200);
        cfg.bg_write_io_ceiling = ReadableSize::gb(100);
        assert!(cfg.validate().is_err());

        cfg.bg_write_io_ceiling = ReadableSize::gb(200);
        cfg.validate().unwrap();
    }

    #[test]
    fn test_validate_rejects_bad_ticks() {
        for bad in [Duration::from_millis(500), Duration::from_secs(61)] {
            let mut cfg = Config::default();
            cfg.quiet_tick = ReadableDuration(bad);
            assert!(cfg.validate().is_err(), "quiet-tick {:?} accepted", bad);

            let mut cfg = Config::default();
            cfg.overloaded_tick = ReadableDuration(bad);
            assert!(
                cfg.validate().is_err(),
                "overloaded-tick {:?} accepted",
                bad
            );
        }
    }

    #[test]
    fn test_validate_rejects_inverted_ticks() {
        let mut cfg = Config::default();
        cfg.quiet_tick = ReadableDuration::secs(5);
        cfg.overloaded_tick = ReadableDuration::secs(10);
        assert!(cfg.validate().is_err());

        // Equal is allowed: one period either way, which is a node that has
        // simply opted out of ticking faster under load.
        cfg.overloaded_tick = ReadableDuration::secs(5);
        cfg.validate().unwrap();
    }

    #[test]
    fn test_validate_rejects_bad_baseline_burst_pct() {
        for bad in [-1.0, f64::NAN, f64::INFINITY] {
            let mut cfg = Config::default();
            cfg.baseline_burst_pct = bad;
            assert!(cfg.validate().is_err(), "{} should be rejected", bad);
        }

        // Zero is allowed: no headroom above the baseline.
        let mut cfg = Config::default();
        cfg.baseline_burst_pct = 0.0;
        cfg.validate().unwrap();
    }

    #[test]
    fn test_validate_rejects_out_of_range_historical_window() {
        for bad in [0, 1, 61, u64::MAX] {
            let mut cfg = Config::default();
            cfg.historical_usage_window_mins = bad;
            assert!(cfg.validate().is_err(), "{} should be rejected", bad);
        }

        for good in [2, 15, 60] {
            let mut cfg = Config::default();
            cfg.historical_usage_window_mins = good;
            cfg.validate().unwrap();
        }
    }

    #[test]
    fn test_config_manager_applies_valid_update() {
        let config = Arc::new(VersionTrack::new(Config::default()));
        let mut mgr = ResourceContrlCfgMgr::new(config.clone());

        let mut change = ConfigChange::new();
        change.insert(
            "fg_cpu_throttle_threshold".to_owned(),
            ConfigValue::F64(90.0),
        );
        mgr.dispatch(change).unwrap();

        assert_eq!(config.value().fg_cpu_throttle_threshold, 90.0);
    }

    /// An online change to the noisy-group reporting gate reaches the cached
    /// copy the coprocessor reads per request on the next tick's refresh.
    #[test]
    fn test_online_change_reaches_cached_noisy_reporting_gate() {
        let resource_ctl = crate::ResourceGroupManager::new(Config::default());
        let mut mgr = ResourceContrlCfgMgr::new(resource_ctl.get_config().clone());
        assert!(!resource_ctl.reports_noisy_groups());

        for (name, on) in [
            ("enable_read_admission_control", true),
            ("enable_read_admission_control", false),
        ] {
            let mut change = ConfigChange::new();
            change.insert(name.to_owned(), ConfigValue::Bool(on));
            mgr.dispatch(change).unwrap();
            resource_ctl.refresh_cached_config();
            assert_eq!(resource_ctl.reports_noisy_groups(), on, "{name}={on}");
        }
    }
}

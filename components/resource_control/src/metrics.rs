// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref BACKGROUND_QUOTA_LIMIT_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resource_control_background_quota_limiter",
        "The quota limit for all background tasks per resource type, in centi-cores (cores * 100) for CPU or bytes/s for IO",
        &["type"]
    )
    .unwrap();
    pub static ref BACKGROUND_RESOURCE_CONSUMPTION: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_control_background_resource_consumption",
        "Total resource consumed by all background tasks (aggregated across all background resource groups) per resource type",
        &["type"]
    )
    .unwrap();
    pub static ref PRIORITY_QUOTA_LIMIT_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resource_control_priority_quota_limit",
        "The quota limiter for each priority in resource control",
        &["priority"]
    )
    .unwrap();
    pub static ref PRIORITY_CPU_TIME_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_control_priority_task_exec_duration",
        "Total execution duration of tasks per-priority",
        &["priority"]
    )
    .unwrap();
    pub static ref PRIORITY_WAIT_DURATION_VEC: HistogramVec = register_histogram_vec!(
        "tikv_resource_control_priority_wait_duration",
        "Histogram of wait duration cause by priority quota limiter",
        &["priority"],
        exponential_buckets(1e-5, 2.0, 22).unwrap() // 10us ~ 42s
    )
    .unwrap();

    pub static ref BACKGROUND_TASK_RESOURCE_UTILIZATION_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resource_control_bg_resource_utilization",
        "The total resource consumed by background tasks, in centi-cores (cores * 100) for CPU or bytes/s for IO",
        &["type"]
    )
    .unwrap();

    pub static ref TWO_PHASE_THROTTLED_REQUESTS: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_control_two_phase_throttled_requests_total",
        "Total requests assigned to phase 1 (RU rate above 15-min baseline) per resource group",
        &["resource_group"]
    )
    .unwrap();

    pub static ref GROUP_RU_HISTORICAL_RATE: GaugeVec = register_gauge_vec!(
        "tikv_resource_control_group_ru_historical_rate",
        "Historical CPU utilization % per resource group (sliding window average)",
        &["resource_group"]
    )
    .unwrap();

    pub static ref GROUP_RU_CURRENT_RATE: GaugeVec = register_gauge_vec!(
        "tikv_resource_control_group_ru_current_rate",
        "Current CPU utilization % per resource group (latest bucket)",
        &["resource_group"]
    )
    .unwrap();

    pub static ref GROUP_RU_BASELINE: GaugeVec = register_gauge_vec!(
        "tikv_resource_control_group_ru_baseline",
        "Quiet-window baseline per resource group, as CPU utilization %. 0 means no quiet window has elapsed yet; whether that makes the group ineligible or judged on raw usage depends on resource-control.noisy-detection",
        &["resource_group"]
    )
    .unwrap();

    pub static ref GROUP_QUOTA_LIMIT_VEC: GaugeVec = register_gauge_vec!(
        "tikv_resource_control_group_quota_limit",
        "Current rate limit per resource group per resource type (CPU as utilization %, 0 means unlimited)",
        &["resource_group", "type"]
    )
    .unwrap();

    pub static ref ADMISSION_CURRENTLY_DELAYED: IntGauge = register_int_gauge!(
        "tikv_resource_control_admission_currently_delayed",
        "Current number of requests sitting in admission control delay"
    )
    .unwrap();

    pub static ref ADMISSION_DELAYED_REQUESTS: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_control_admission_delayed_requests_total",
        "Total requests delayed by admission control per resource group",
        &["resource_group"]
    )
    .unwrap();
    pub static ref ADMISSION_REJECTED_REQUESTS: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_control_admission_rejected_requests_total",
        "Total requests rejected by admission control per resource group",
        &["resource_group"]
    )
    .unwrap();
    pub static ref ADMISSION_DELAY_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_resource_control_admission_delay_duration_seconds",
        "Histogram of delay duration imposed by admission control",
        &["resource_group"],
        exponential_buckets(1e-4, 2.0, 20).unwrap() // 100us ~ 52s
    )
    .unwrap();

    pub static ref READ_POOL_CPU_VEC: GaugeVec = register_gauge_vec!(
        "tikv_resource_control_read_pool_cpu_percent",
        "Unified read pool CPU usage as a percentage of one core (100 = 1 core): historical (live sliding average), baseline (floor frozen at overload onset, 0 when not frozen), current (measured), and target (foreground-pressure-driven ceiling)",
        &["type"]
    )
    .unwrap();

    pub static ref EFFECTIVE_NOISY_DETECTION: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resource_control_effective_noisy_detection",
        "Noisy-detection policy in force right now, 1 for the active one. Tracks online config changes; whether an individual group is falling back to current usage for want of a baseline is visible in tikv_resource_control_group_ru_baseline, which reads 0 for such a group",
        &["policy"]
    )
    .unwrap();

    pub static ref RESOURCE_SCORE_VEC: GaugeVec = register_gauge_vec!(
        "tikv_resource_control_resource_score",
        "Common 0-100 resource-pressure score computed by compute_resource_scores, per resource type (cpu, io, compaction)",
        &["type"]
    )
    .unwrap();
}

/// Drops an evicted tracker's gauges, which would keep its last value.
pub fn deregister_tracker_gauges(name: &str) {
    _ = GROUP_RU_HISTORICAL_RATE.remove_label_values(&[name]);
    _ = GROUP_RU_CURRENT_RATE.remove_label_values(&[name]);
    _ = GROUP_RU_BASELINE.remove_label_values(&[name]);
    _ = GROUP_QUOTA_LIMIT_VEC.remove_label_values(&[name, "cpu"]);
}

pub fn deregister_metrics(name: &str) {
    _ = TWO_PHASE_THROTTLED_REQUESTS.remove_label_values(&[name]);
    _ = GROUP_QUOTA_LIMIT_VEC.remove_label_values(&[name, "cpu"]);
    _ = GROUP_RU_HISTORICAL_RATE.remove_label_values(&[name]);
    _ = GROUP_RU_CURRENT_RATE.remove_label_values(&[name]);
    _ = GROUP_RU_BASELINE.remove_label_values(&[name]);
    _ = ADMISSION_DELAYED_REQUESTS.remove_label_values(&[name]);
    _ = ADMISSION_REJECTED_REQUESTS.remove_label_values(&[name]);
    _ = ADMISSION_DELAY_DURATION.remove_label_values(&[name]);
    _ = ADMISSION_DELAYED_REQUESTS.remove_label_values(&["background"]);
    _ = ADMISSION_REJECTED_REQUESTS.remove_label_values(&["background"]);
    _ = ADMISSION_DELAY_DURATION.remove_label_values(&["background"]);
}

/// Publishes `policy` as the one in force, zeroing the others so a panel that
/// sums the series cannot show two policies at once after a config change.
pub fn report_effective_noisy_detection(policy: crate::config::NoisyDetection) {
    use crate::config::NoisyDetection::*;
    for candidate in [Baseline, BaselineFallbackCurrentUsage, CurrentUsage] {
        EFFECTIVE_NOISY_DETECTION
            .with_label_values(&[&candidate.to_string()])
            .set(i64::from(candidate == policy));
    }
}

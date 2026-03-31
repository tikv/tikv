// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref BACKGROUND_QUOTA_LIMIT_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resource_control_background_quota_limiter",
        "The quota limiter for all background tasks per resource type",
        &["type"]
    )
    .unwrap();
    pub static ref BACKGROUND_RESOURCE_CONSUMPTION: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_control_background_resource_consumption",
        "Total resource consumed by all background tasks (aggregated across all background resource groups) per resource type",
        &["type"]
    )
    .unwrap();
    pub static ref BACKGROUND_TASKS_WAIT_DURATION: IntCounter = register_int_counter!(
        "tikv_resource_control_background_task_wait_duration",
        "Total wait duration of all background tasks (aggregated across all background resource groups)"
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
        "The total resource utilization percentage of background tasks",
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

    pub static ref CPU_THROTTLE_ALLOCATIONS: IntCounterVec = register_int_counter_vec!(
        "tikv_cpu_throttle_allocations_total",
        "Total CPU throttle token allocations",
        &["resource_group", "result"]
    )
    .unwrap();
    pub static ref CPU_THROTTLE_GLOBAL_BUCKET_AVAILABLE: IntGauge = register_int_gauge!(
        "tikv_cpu_throttle_global_bucket_available_us",
        "Available CPU tokens in global bucket"
    )
    .unwrap();
    pub static ref CPU_THROTTLE_GLOBAL_BUCKET_CAPACITY: IntGauge = register_int_gauge!(
        "tikv_cpu_throttle_global_bucket_capacity_us",
        "Capacity of global CPU token bucket"
    )
    .unwrap();
    pub static ref CPU_THROTTLE_GLOBAL_REFILL_RATE: IntGauge = register_int_gauge!(
        "tikv_cpu_throttle_global_refill_rate_us",
        "Current refill rate of the global CPU token bucket"
    )
    .unwrap();
    pub static ref CPU_THROTTLE_GROUP_BUCKET_AVAILABLE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_cpu_throttle_group_bucket_available_us",
        "Available CPU tokens in each resource group bucket",
        &["resource_group"]
    )
    .unwrap();
    pub static ref CPU_THROTTLE_GROUP_BUCKET_CAPACITY: IntGaugeVec = register_int_gauge_vec!(
        "tikv_cpu_throttle_group_bucket_capacity_us",
        "Capacity of each resource group CPU token bucket",
        &["resource_group"]
    )
    .unwrap();
    pub static ref CPU_THROTTLE_GROUP_REFILL_RATE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_cpu_throttle_group_refill_rate_us",
        "Current refill rate of each resource group CPU token bucket",
        &["resource_group"]
    )
    .unwrap();
    pub static ref CPU_THROTTLE_REFILL_RATE_ADJUSTMENTS: IntCounterVec = register_int_counter_vec!(
        "tikv_cpu_throttle_refill_rate_adjustments_total",
        "Total CPU throttle refill rate adjustments",
        &["level", "direction"]
    )
    .unwrap();
    pub static ref CPU_THROTTLE_TOKEN_WAIT_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_cpu_throttle_token_wait_duration_seconds",
        "Histogram of CPU token allocation wait duration",
        &["resource_group", "result"],
        exponential_buckets(1e-5, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref CPU_THROTTLE_RUNTIME_TOKEN_WAIT_DURATION: HistogramVec =
        register_histogram_vec!(
            "tikv_cpu_throttle_runtime_token_wait_duration_seconds",
            "Histogram of runtime CPU token allocation wait duration",
            &["resource_group", "result"],
            exponential_buckets(1e-5, 2.0, 20).unwrap()
        )
        .unwrap();
    pub static ref CPU_THROTTLE_UNKNOWN_GROUP: IntCounter = register_int_counter!(
        "tikv_cpu_throttle_unknown_group_total",
        "Total requests throttled with unknown resource group name"
    )
    .unwrap();
    pub static ref CPU_THROTTLE_REQUEST_CPU_TIME: HistogramVec = register_histogram_vec!(
        "tikv_cpu_throttle_request_cpu_time_seconds",
        "Histogram of measured CPU time per throttled request",
        &["resource_group"],
        exponential_buckets(1e-5, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref CPU_THROTTLE_REQUEST_ACTUAL_TO_ESTIMATED_RATIO: HistogramVec =
        register_histogram_vec!(
            "tikv_cpu_throttle_request_actual_to_estimated_ratio",
            "Histogram of actual to estimated CPU time ratio",
            &["resource_group"],
            exponential_buckets(0.125, 2.0, 12).unwrap()
        )
        .unwrap();
    pub static ref CPU_USAGE_MONITOR_GLOBAL_RATIO: Gauge = register_gauge!(
        "tikv_cpu_usage_monitor_global_ratio",
        "Sliding-window CPU usage ratio for the unified read pool"
    )
    .unwrap();
    pub static ref CPU_USAGE_MONITOR_RESOURCE_GROUP_DAG_RATIO: GaugeVec =
        register_gauge_vec!(
            "tikv_cpu_usage_monitor_resource_group_dag_ratio",
            "Sliding-window DAG CPU usage ratio for each resource group",
            &["resource_group"]
        )
        .unwrap();
    pub static ref CPU_USAGE_MONITOR_COLLECT_DURATION: Histogram = register_histogram!(
        "tikv_cpu_usage_monitor_collect_duration_seconds",
        "Histogram of CPU usage monitor collection duration",
        exponential_buckets(1e-6, 2.0, 20).unwrap()
    )
    .unwrap();
}

pub fn deregister_metrics(name: &str) {
    _ = TWO_PHASE_THROTTLED_REQUESTS.remove_label_values(&[name]);
    _ = GROUP_QUOTA_LIMIT_VEC.remove_label_values(&[name, "cpu"]);
    _ = GROUP_RU_HISTORICAL_RATE.remove_label_values(&[name]);
    _ = GROUP_RU_CURRENT_RATE.remove_label_values(&[name]);
    _ = ADMISSION_DELAYED_REQUESTS.remove_label_values(&[name]);
    _ = ADMISSION_REJECTED_REQUESTS.remove_label_values(&[name]);
    _ = ADMISSION_DELAY_DURATION.remove_label_values(&[name]);
    _ = ADMISSION_DELAYED_REQUESTS.remove_label_values(&["background"]);
    _ = ADMISSION_REJECTED_REQUESTS.remove_label_values(&["background"]);
    _ = ADMISSION_DELAY_DURATION.remove_label_values(&["background"]);
}

pub fn deregister_cpu_throttle_metrics(resource_group: &str) {
    use crate::cpu_throttle::{
        CPU_THROTTLE_ALLOCATION_BURST, CPU_THROTTLE_ALLOCATION_GLOBAL_EXHAUSTED,
        CPU_THROTTLE_ALLOCATION_GLOBAL_ONLY, CPU_THROTTLE_ALLOCATION_GROUP_EXHAUSTED,
        CPU_THROTTLE_ALLOCATION_SUCCESS, CPU_THROTTLE_TOKEN_WAIT_SUCCESS,
        CPU_THROTTLE_TOKEN_WAIT_TIMEOUT,
    };

    for result in [
        CPU_THROTTLE_ALLOCATION_SUCCESS,
        CPU_THROTTLE_ALLOCATION_GLOBAL_EXHAUSTED,
        CPU_THROTTLE_ALLOCATION_GROUP_EXHAUSTED,
        CPU_THROTTLE_TOKEN_WAIT_TIMEOUT,
        CPU_THROTTLE_ALLOCATION_BURST,
        CPU_THROTTLE_ALLOCATION_GLOBAL_ONLY,
    ] {
        _ = CPU_THROTTLE_ALLOCATIONS.remove_label_values(&[resource_group, result]);
    }
    for result in [
        CPU_THROTTLE_TOKEN_WAIT_SUCCESS,
        CPU_THROTTLE_TOKEN_WAIT_TIMEOUT,
    ] {
        _ = CPU_THROTTLE_TOKEN_WAIT_DURATION.remove_label_values(&[resource_group, result]);
        _ = CPU_THROTTLE_RUNTIME_TOKEN_WAIT_DURATION.remove_label_values(&[resource_group, result]);
    }
    _ = CPU_THROTTLE_GROUP_BUCKET_AVAILABLE.remove_label_values(&[resource_group]);
    _ = CPU_THROTTLE_GROUP_BUCKET_CAPACITY.remove_label_values(&[resource_group]);
    _ = CPU_THROTTLE_GROUP_REFILL_RATE.remove_label_values(&[resource_group]);
    _ = CPU_THROTTLE_REQUEST_CPU_TIME.remove_label_values(&[resource_group]);
    _ = CPU_THROTTLE_REQUEST_ACTUAL_TO_ESTIMATED_RATIO.remove_label_values(&[resource_group]);
    _ = CPU_USAGE_MONITOR_RESOURCE_GROUP_DAG_RATIO.remove_label_values(&[resource_group]);
}

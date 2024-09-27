// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref BACKGROUND_QUOTA_LIMIT_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resource_control_background_quota_limiter",
        "The quota limiter of background resource groups per resource type",
        &["resource_group", "type"]
    )
    .unwrap();
    pub static ref BACKGROUND_RESOURCE_CONSUMPTION: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_control_background_resource_consumption",
        "Total resource consumed of background resource groups per resource type",
        &["resource_group", "type"]
    )
    .unwrap();
    pub static ref BACKGROUND_TASKS_WAIT_DURATION: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_control_background_task_wait_duration",
        "Total wait duration of background tasks per resource group",
        &["resource_group"]
    )
    .unwrap();
    pub static ref PRIORITY_QUOTA_LIMIT_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resource_control_priority_quota_limit",
        "The quota limiter for each priority in resource control",
        &["priority"]
    )
    .unwrap();
<<<<<<< HEAD
=======
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
        exponential_buckets(1e-5, 2.0, 18).unwrap() // 10us ~ 2.5s
    )
    .unwrap();

    pub static ref BACKGROUND_TASK_RESOURCE_UTILIZATION_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_resource_control_bg_resource_utilization",
        "The total resource utilization percentage of background tasks",
        &["type"]
    )
    .unwrap();
>>>>>>> 7b49098667 (resource_control: add metrics for priority resource limiter (#17590))
}

pub fn deregister_metrics(name: &str) {
    for ty in ["cpu", "io"] {
        _ = BACKGROUND_QUOTA_LIMIT_VEC.remove_label_values(&[name, ty]);
        _ = BACKGROUND_RESOURCE_CONSUMPTION.remove_label_values(&[name, ty]);
    }
    _ = BACKGROUND_TASKS_WAIT_DURATION.remove_label_values(&[name]);
}

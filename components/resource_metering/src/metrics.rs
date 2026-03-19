// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref STAT_TASK_COUNT: IntCounter = register_int_counter!(
        "tikv_resource_metering_stat_task_count",
        "Counter of times to read the stat of tasks from procfs"
    )
    .unwrap();
    pub static ref UNIFIED_READ_TAG_SAMPLE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_unified_read_tag_sample_total",
        "Unified-read-pool sampling points split by whether a resource tag is attached",
        &["state"]
    )
    .unwrap();
    pub static ref UNIFIED_READ_TAG_CPU_MILLIS_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_unified_read_tag_cpu_millis_total",
        "Unified-read-pool observed CPU time in milliseconds split by whether a resource tag is attached",
        &["state"]
    )
    .unwrap();
    pub static ref SCHED_TAG_SAMPLE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_scheduler_tag_sample_total",
        "Scheduler-pool sampling points split by whether a resource tag is attached",
        &["state"]
    )
    .unwrap();
    pub static ref SCHED_TAG_CPU_MILLIS_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_scheduler_tag_cpu_millis_total",
        "Scheduler-pool observed CPU time in milliseconds split by whether a resource tag is attached",
        &["state"]
    )
    .unwrap();
    pub static ref SCHED_POLL_STATE_SAMPLE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_scheduler_poll_state_sample_total",
        "Scheduler-pool sampling points split by tagged poll, tracked-poll-without-tag, or outside-tracked-poll state",
        &["state"]
    )
    .unwrap();
    pub static ref SCHED_POLL_STATE_CPU_MILLIS_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_scheduler_poll_state_cpu_millis_total",
        "Scheduler-pool observed CPU time in milliseconds split by tagged poll, tracked-poll-without-tag, or outside-tracked-poll state",
        &["state"]
    )
    .unwrap();
    pub static ref REPORT_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_resource_metering_report_duration_seconds",
        "Bucketed histogram of reporting time (s) to the resource metering clients"
    )
    .unwrap();
    pub static ref REPORT_DATA_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_report_data_count",
        "Total number of reporting data",
        &["type"]
    )
    .unwrap();
    pub static ref IGNORED_DATA_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_resource_metering_ignored_data",
        "Total number of ignored data",
        &["type"]
    )
    .unwrap();
}

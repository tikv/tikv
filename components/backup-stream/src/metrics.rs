// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

/// The status of a task.
/// The ordering of this imples the priority for presenting to the user.
/// max(TASK_STATUS) of all stores would be probably the state of the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Running = 0,
    Paused,
    Error,
}

pub fn update_task_status(status: TaskStatus, task: &str) {
    let g = TASK_STATUS.with_label_values(&[task]);
    // The state transform graph:
    // ┌─────────┐      ┌────────┐      ┌───────┐
    // │ RUNNING ├──────► PAUSED ├──────► ERROR │
    // └────▲────┘      └───┬────┘      └───┬───┘
    //      └───────────────┴───────────────┘
    if g.get() < status as _ || status == TaskStatus::Running {
        g.set(status as _);
    }
}

lazy_static! {
    pub static ref INTERNAL_ACTOR_MESSAGE_HANDLE_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_log_backup_interal_actor_acting_duration_sec",
        "The internal actor message handling duration.",
        &["message"],
        exponential_buckets(0.001, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref INITIAL_SCAN_REASON: IntCounterVec = register_int_counter_vec!(
        "tikv_log_backup_initial_scan_reason",
        "The reason of doing initial scanning",
        &["reason"]
    )
    .unwrap();
    pub static ref HANDLE_EVENT_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_stream_event_handle_duration_sec",
        "The duration of handling an cmd batch.",
        &["stage"],
        exponential_buckets(0.001, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref HANDLE_KV_HISTOGRAM: Histogram = register_histogram!(
        "tikv_stream_handle_kv_batch",
        "The total kv pair change handle by the stream backup",
        exponential_buckets(1.0, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref INCREMENTAL_SCAN_SIZE: Histogram = register_histogram!(
        "tikv_stream_incremental_scan_bytes",
        "The size of scanning.",
        exponential_buckets(64.0, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref SKIP_KV_COUNTER: Counter = register_counter!(
        "tikv_stream_skip_kv_count",
        "The total kv size skipped by the streaming",
    )
    .unwrap();
    pub static ref STREAM_ERROR: CounterVec = register_counter_vec!(
        "tikv_stream_errors",
        "The errors during stream backup.",
        &["type"]
    )
    .unwrap();
    pub static ref STREAM_FATAL_ERROR: CounterVec = register_counter_vec!(
        "tikv_log_backup_fatal_errors",
        "The errors during stream backup.",
        &["type"]
    )
    .unwrap();
    pub static ref HEAP_MEMORY: IntGauge = register_int_gauge!(
        "tikv_stream_heap_memory",
        "The heap memory allocating by stream backup."
    )
    .unwrap();
    pub static ref ON_EVENT_COST_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_stream_on_event_duration_seconds",
        "The time cost of handling events.",
        &["stage"],
        exponential_buckets(0.001, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref STORE_CHECKPOINT_TS: IntGaugeVec = register_int_gauge_vec!(
        "tikv_stream_store_checkpoint_ts",
        "The checkpoint ts (next backup ts) of task",
        &["task"],
    )
    .unwrap();
    pub static ref FLUSH_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_stream_flush_duration_sec",
        "The time cost of flushing a task.",
        &["stage"],
        exponential_buckets(1.0, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref FLUSH_FILE_SIZE: Histogram = register_histogram!(
        "tikv_stream_flush_file_size",
        "Some statistics of flushing of this run.",
        exponential_buckets(1024.0, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref INITIAL_SCAN_DURATION: Histogram = register_histogram!(
        "tikv_stream_initial_scan_duration_sec",
        "The duration of initial scanning.",
        exponential_buckets(0.001, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref SKIP_RETRY: IntCounterVec = register_int_counter_vec!(
        "tikv_stream_skip_retry_observe",
        "The reason of giving up observing region when meeting error.",
        &["reason"],
    )
    .unwrap();
    pub static ref INITIAL_SCAN_STAT: IntCounterVec = register_int_counter_vec!(
        "tikv_stream_initial_scan_operations",
        "The operations over rocksdb during initial scanning.",
        &["cf", "op"],
    )
    .unwrap();
    pub static ref STREAM_ENABLED: IntCounter = register_int_counter!(
        "tikv_stream_enabled",
        "When gt 0, this node enabled streaming."
    )
    .unwrap();
    pub static ref TRACK_REGION: IntCounterVec = register_int_counter_vec!(
        "tikv_stream_observed_region",
        "the region being observed by the current store.",
        &["type"],
    )
    .unwrap();
    static ref TASK_STATUS: IntGaugeVec = register_int_gauge_vec!(
        "tikv_log_backup_task_status",
        "The status of tasks",
        &["task"]
    )
    .unwrap();
}

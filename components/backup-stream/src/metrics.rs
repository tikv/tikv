// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
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
    pub static ref HEAP_MEMORY: CounterVec = register_counter_vec!(
        "tikv_stream_heap_memory",
        "The heap memory allocating by stream backup.",
        &["type"]
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
        exponential_buckets(0.001, 2.0, 16).unwrap()
    )
    .unwrap();
}

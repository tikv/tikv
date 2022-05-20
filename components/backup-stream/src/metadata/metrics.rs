// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref METADATA_OPERATION_LATENCY: HistogramVec = register_histogram_vec! {
        "tikv_stream_metadata_operation_latency",
        "metadata operation(task_get | task_step | task_fetch_all | task_progress_get | etc.) latency.",
        &["type"],
        exponential_buckets(0.001, 2.0, 16).unwrap()
    }.unwrap();

    pub static ref METADATA_EVENT_RECEIVED: IntCounterVec = register_int_counter_vec! {
        "tikv_stream_metadata_event_count",
        "metadata event(task_add, task_removed, error) count.",
        &["type"],
    }.unwrap();
}

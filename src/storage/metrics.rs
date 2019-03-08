// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use prometheus::*;

lazy_static! {
    pub static ref KV_COMMAND_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_storage_command_total",
        "Total number of commands received.",
        &["type"]
    )
    .unwrap();
    pub static ref SCHED_STAGE_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_scheduler_stage_total",
        "Total number of commands on each stage.",
        &["type", "stage"]
    )
    .unwrap();
    pub static ref SCHED_WRITING_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_writing_bytes",
        "Total number of writing kv."
    )
    .unwrap();
    pub static ref SCHED_CONTEX_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_contex_total",
        "Total number of pending commands."
    )
    .unwrap();
    pub static ref SCHED_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_command_duration_seconds",
        "Bucketed histogram of command execution",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_LATCH_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_latch_wait_duration_seconds",
        "Bucketed histogram of latch wait",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_PROCESSING_READ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_processing_read_duration_seconds",
        "Bucketed histogram of processing read duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_PROCESSING_WRITE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_processing_write_duration_seconds",
        "Bucketed histogram of processing write duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_TOO_BUSY_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_scheduler_too_busy_total",
        "Total count of scheduler too busy",
        &["type"]
    )
    .unwrap();
    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_scheduler_commands_pri_total",
        "Total count of different priority commands",
        &["priority"]
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYREAD_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_kv_command_key_read",
        "Bucketed histogram of keys read of a kv command",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KV_COMMAND_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "tikv_scheduler_kv_scan_details",
        "Bucketed counter of kv keys scan details for each cf",
        &["req", "cf", "tag"]
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYWRITE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_kv_command_key_write",
        "Bucketed histogram of keys write of a kv command",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KV_PREWRITE_MAX_READ_TS_RES_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_storage_prewrite_max_read_ts_res",
        "Whether a prewrite request returns the max read ts successfully",
        &["type"]
    )
    .unwrap();
    pub static ref KV_MAX_READ_TS_UPDATE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_storage_max_read_ts_update",
        "Statistics of max read ts updating",
        &["from", "res"]
    )
    .unwrap();
    pub static ref KV_MAX_READ_TS_FETCH_UPDATE_RETRY_HISTOGRAM: Histogram = register_histogram!(
        "tikv_storage_max_read_ts_fetch_update_retry",
        "Statics of max read ts fetch_update retry times",
        linear_buckets(1.0, 1.0, 15).unwrap()
    )
    .unwrap();
    pub static ref KV_GC_EMPTY_RANGE_COUNTER: IntCounter = register_int_counter!(
        "tikv_storage_gc_empty_range_total",
        "Total number of empty range found by gc"
    )
    .unwrap();
    pub static ref KV_GC_SKIPPED_COUNTER: IntCounter = register_int_counter!(
        "tikv_storage_gc_skipped_counter",
        "Total number of gc command skipped owing to optimization"
    )
    .unwrap();
    pub static ref GC_TASK_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_gcworker_gc_task_duration_vec",
        "Duration of gc tasks execution",
        &["task"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref GC_GCTASK_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_gcworker_gc_tasks_vec",
        "Counter of gc tasks processed by gc_worker",
        &["task"]
    )
    .unwrap();
    pub static ref GC_GCTASK_FAIL_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_gcworker_gc_task_fail_vec",
        "Counter of gc tasks that is failed",
        &["task"]
    )
    .unwrap();
    pub static ref GC_TOO_BUSY_COUNTER: IntCounter = register_int_counter!(
        "tikv_gc_worker_too_busy",
        "Counter of occurrence of gc_worker being too busy"
    )
    .unwrap();
    pub static ref GC_KEYS_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_gcworker_gc_keys",
        "Counter of keys affected during gc",
        &["cf", "tag"]
    )
    .unwrap();
    pub static ref AUTO_GC_STATUS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_gcworker_autogc_status",
        "State of the auto gc manager",
        &["state"]
    )
    .unwrap();
    pub static ref REQUEST_EXCEED_BOUND: IntCounter = register_int_counter!(
        "tikv_request_exceed_bound",
        "Counter of request exceed bound"
    )
    .unwrap();
}

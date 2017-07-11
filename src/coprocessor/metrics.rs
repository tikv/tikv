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

use prometheus::{HistogramVec, CounterVec, GaugeVec, exponential_buckets};

lazy_static! {
    pub static ref COPR_REQ_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_coprocessor_request_duration_seconds",
            "Bucketed histogram of coprocessor request duration",
            &["type", "req"]
        ).unwrap();

    pub static ref OUTDATED_REQ_WAIT_TIME: HistogramVec =
        register_histogram_vec!(
            "tikv_coprocessor_outdated_request_wait_seconds",
            "Bucketed histogram of outdated coprocessor request wait duration",
            &["type", "req"]
        ).unwrap();

    pub static ref COPR_REQ_HANDLE_TIME: HistogramVec =
        register_histogram_vec!(
            "tikv_coprocessor_request_handle_seconds",
            "Bucketed histogram of coprocessor handle request duration",
            &["type", "req"]
        ).unwrap();

    pub static ref COPR_REQ_WAIT_TIME: HistogramVec =
        register_histogram_vec!(
            "tikv_coprocessor_request_wait_seconds",
            "Bucketed histogram of coprocessor request wait duration",
            &["type", "req"]
        ).unwrap();

    pub static ref COPR_REQ_ERROR: CounterVec =
        register_counter_vec!(
            "tikv_coprocessor_request_error",
            "Total number of push down request error.",
            &["type", "reason"]
        ).unwrap();

    pub static ref COPR_PENDING_REQS: GaugeVec =
        register_gauge_vec!(
            "tikv_coprocessor_pending_request",
            "Total number of pending push down request.",
            &["type"]
        ).unwrap();

    pub static ref COPR_SCAN_KEYS: HistogramVec =
        register_histogram_vec!(
            "tikv_coprocessor_scan_keys",
            "Bucketed histogram of coprocessor per request scan keys",
            &["type", "req"],
            exponential_buckets(1.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref COPR_SCAN_STATISTICS: HistogramVec =
        register_histogram_vec!(
            "tikv_coprocessor_scan_statistics",
            "Bucketed histogram of coprocessor scan statistics for each CF",
            &["req", "cf", "tag"],
             exponential_buckets(1.0, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref COPR_EXECUTOR_COUNT: CounterVec =
        register_counter_vec!(
            "tikv_coprocessor_executor_count",
            "Total number of each executor",
            &["type"]
        ).unwrap();

    pub static ref CORP_GET_OR_SCAN_COUNT: CounterVec =
        register_counter_vec!(
            "tikv_coprocessor_get_or_scan_count",
            "Total number of rocksdb query of get or scan count",
            &["type"]
        ).unwrap();
}

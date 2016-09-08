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

use prometheus::{CounterVec, HistogramVec, exponential_buckets};

lazy_static! {
    pub static ref RAFTKV_REQUEST_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftkv_request_total",
            "Total number of requests",
            &["type", "status"]
        ).unwrap();

    pub static ref RAFTKV_REQUEST_DURATIONS_VEC: HistogramVec =
        register_histogram_vec!(
            histogram_opts!(
                "tikv_raftkv_handle_requests_duration_seconds",
                "Bucketed histogram of processing successful requests.",
                [exponential_buckets(0.0005, 2.0, 13).unwrap()]),
            &["type"]
        ).unwrap();
}

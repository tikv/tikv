// Copyright 2018 PingCAP, Inc.
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
    pub static ref COMMAND_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_readpool_command_duration_seconds",
            "Bucketed histogram of readpool command execution duration",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    // pub static ref KEYREAD_HISTOGRAM_VEC: HistogramVec =
    //     register_histogram_vec!(
    //         "tikv_readpool_key_read_duration_seconds",
    //         "Bucketed histogram of readpool key read duration",
    //         &["type"],
    //         exponential_buckets(1.0, 2.0, 21).unwrap()
    //     ).unwrap();

    pub static ref COMMAND_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_readpool_command_total",
            "Total count of commands received",
            &["type", "priority"]
        ).unwrap();

    pub static ref KV_SCAN_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_readpool_kv_scan_total",
            "Total count of kv key scans for each CF",
            &["req", "cf", "tag"]
        ).unwrap();
}

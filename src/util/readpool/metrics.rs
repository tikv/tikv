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
    /// Command durations. `cmd_type` can be `kv` or `raw`. `stage` can be `snapshot` or `process`.
    /// Failed stages are still counted. Notice that subsequent stages may not exist. For example,
    /// if `snapshot` fails, duration of a failed `snapshot` is counted, but there will be no
    /// `process` stages.
    pub static ref COMMAND_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_readpool_command_duration_seconds",
            "Bucketed histogram of readpool command execution duration",
            &["cmd", "cmd_type", "stage"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    /// Command counts, include failed ones. `priority` can be `Normal`, `High`, `Low`.
    pub static ref COMMAND_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_readpool_command_total",
            "Total count of readpool commands received",
            &["cmd", "cmd_type", "priority"]
        ).unwrap();

    /// Successful keys read, include NOT FOUND ones. Failures are not counted.
    pub static ref KEY_READ_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_readpool_key_reads",
            "Bucketed histogram of readpool key reads",
            &["cmd", "cmd_type"],
            exponential_buckets(1.0, 2.0, 21).unwrap()
        ).unwrap();

    /// Scan counts, include failed ones. Some operations doesn't involve with scans, e.g. raw_get.
    pub static ref SCAN_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_readpool_scan_total",
            "Total count of readpool key scans for each CF",
            &["cmd", "cmd_type", "cf", "tag"]
        ).unwrap();
}

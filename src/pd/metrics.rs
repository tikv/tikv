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
    pub static ref PD_REQUEST_COUNTER: CounterVec =
        register_counter_vec!(
            "tikv_pd_request_total",
            "Total number of PD requests handled",
            &["type"]
        ).unwrap();

    pub static ref PD_REQUEST_HISTOGRAM: HistogramVec =
        register_histogram_vec!(
            "tikv_pd_request_duration_seconds",
            "Bucketed histogram of PD request duration",
            &["type", "result"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

    pub static ref PD_HEARTBEAT_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_pd_heartbeat_received_total",
            "Total number of PD heartbeat received.",
            &["type"]
        ).unwrap();

    pub static ref PD_VALIDATE_PEER_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_pd_validate_peer_total",
            "Total number of pd worker validate peer task.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SIZE_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_store_size_bytes",
            "Size of storage.",
            &["type"]
        ).unwrap();
}

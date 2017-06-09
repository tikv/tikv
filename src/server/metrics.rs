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

use prometheus::{Counter, CounterVec, Histogram, HistogramVec};

lazy_static! {
    pub static ref SEND_SNAP_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_server_send_snapshot_duration_seconds",
            "Bucketed histogram of server send snapshots duration"
        ).unwrap();

    pub static ref SNAP_TASK_COUNTER: CounterVec =
        register_counter_vec!(
            "tikv_server_snapshot_task_total",
            "Total number of snapshot task",
            &["type"]
        ).unwrap();

    pub static ref GRPC_MSG_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_grpc_msg_duration_seconds",
            "Bucketed histogram of grpc server messages",
            &["type"]
        ).unwrap();

    pub static ref GRPC_MSG_FAIL_COUNTER: CounterVec =
        register_counter_vec!(
            "tikv_grpc_msg_fail_total",
            "Total number of handle grpc message failure",
            &["type"]
        ).unwrap();

    pub static ref RAFT_MESSAGE_RECV_COUNTER: Counter =
        register_counter!(
            "tikv_server_raft_message_recv_total",
            "Total number of raft messages received"
        ).unwrap();

    pub static ref RESOLVE_STORE_COUNTER: CounterVec =
        register_counter_vec!(
            "tikv_server_resolve_store_total",
            "Total number of resolving store",
            &["type"]
        ).unwrap();

    pub static ref REPORT_FAILURE_MSG_COUNTER: CounterVec =
        register_counter_vec!(
            "tikv_server_report_failure_msg_total",
            "Total number of reporting failure messages",
            &["type", "store_id"]
        ).unwrap();
}

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

use prometheus::{Gauge, Counter, CounterVec, Histogram};

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

    pub static ref RECV_MSG_COUNTER: CounterVec =
        register_counter_vec!(
            "tikv_server_receive_msg_total",
            "Total number of receiving messages",
            &["type"]
        ).unwrap();

    pub static ref RESOLVE_STORE_COUNTER: CounterVec =
        register_counter_vec!(
            "tikv_server_resolve_store_total",
            "Total number of resolving store",
            &["type"]
        ).unwrap();

    pub static ref CONNECTION_GAUGE: Gauge =
        register_gauge!(
            "tikv_server_connection_total",
            "Total number of connection"
        ).unwrap();

    pub static ref REPORT_FAILURE_MSG_COUNTER: CounterVec =
        register_counter_vec!(
            "tikv_server_report_failure_msg_total",
            "Total number of reporting failure messages",
            &["type", "store_id"]
        ).unwrap();

    pub static ref CONN_SEND_BYTES_COUNTER: Counter =
        register_counter!(
            "tikv_server_conn_send_bytes_total",
            "Total bytes of connection send data"
        ).unwrap();

   pub static ref CONN_PENDING_SEND_BYTES_COUNTER: Counter =
        register_counter!(
            "tikv_server_conn_pending_send_bytes_total",
            "Total bytes of connection pending send data"
        ).unwrap();

    pub static ref CONN_RECV_BYTES_COUNTER: Counter =
        register_counter!(
            "tikv_server_conn_recv_bytes_total",
            "Total bytes of connection receive data"
        ).unwrap();
}

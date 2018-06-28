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
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum GrpcTypeKind {
        kv_get,
        kv_scan,
        kv_prewrite,
        kv_commit,
        kv_cleanup,
        kv_batch_get,
        kv_batch_rollback,
        kv_scan_lock,
        kv_resolve_lock,
        kv_gc,
        kv_delete_range,
        raw_get,
        raw_batch_get,
        raw_scan,
        raw_batch_scan,
        raw_put,
        raw_batch_put,
        raw_delete,
        raw_delete_range,
        raw_batch_delete,
        coprocessor,
        coprocessor_stream,
        mvcc_get_by_key,
        mvcc_get_by_start_ts,
        split_region,
    }
    pub struct GrpcMsgHistogramVec: Histogram {
        "type" => GrpcTypeKind,
    }
    pub struct GrpcMsgFailCounterVec: IntCounter {
        "type" => GrpcTypeKind,
    }
}

lazy_static! {
    pub static ref SEND_SNAP_HISTOGRAM: Histogram = register_histogram!(
        "tikv_server_send_snapshot_duration_seconds",
        "Bucketed histogram of server send snapshots duration",
        exponential_buckets(0.05, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref SNAP_TASK_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_snapshot_task_total",
        "Total number of snapshot task",
        &["type"]
    ).unwrap();
    pub static ref GRPC_MSG_HISTOGRAM_VEC: GrpcMsgHistogramVec = register_static_histogram_vec!(
        GrpcMsgHistogramVec,
        "tikv_grpc_msg_duration_seconds",
        "Bucketed histogram of grpc server messages",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref GRPC_MSG_FAIL_COUNTER: GrpcMsgFailCounterVec = register_static_int_counter_vec!(
        GrpcMsgFailCounterVec,
        "tikv_grpc_msg_fail_total",
        "Total number of handle grpc message failure",
        &["type"]
    ).unwrap();
    pub static ref RAFT_MESSAGE_RECV_COUNTER: IntCounter = register_int_counter!(
        "tikv_server_raft_message_recv_total",
        "Total number of raft messages received"
    ).unwrap();
    pub static ref RESOLVE_STORE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_resolve_store_total",
        "Total number of resolving store",
        &["type"]
    ).unwrap();
    pub static ref REPORT_FAILURE_MSG_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_report_failure_msg_total",
        "Total number of reporting failure messages",
        &["type", "store_id"]
    ).unwrap();
    pub static ref RAFT_MESSAGE_FLUSH_COUNTER: IntCounter = register_int_counter!(
        "tikv_server_raft_message_flush_total",
        "Total number of raft messages flushed"
    ).unwrap();
}

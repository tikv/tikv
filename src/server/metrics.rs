// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

use prometheus::exponential_buckets;

use crate::storage::ErrorHeaderKind;

make_static_metric! {
    pub label_enum GrpcTypeKind {
        invalid,
        kv_get,
        kv_scan,
        kv_prewrite,
        kv_pessimistic_lock,
        kv_pessimistic_rollback,
        kv_commit,
        kv_cleanup,
        kv_batch_get,
        kv_batch_get_command,
        kv_batch_rollback,
        kv_txn_heart_beat,
        kv_check_txn_status,
        kv_scan_lock,
        kv_resolve_lock,
        kv_gc,
        kv_delete_range,
        raw_get,
        raw_batch_get,
        raw_batch_get_command,
        raw_scan,
        raw_batch_scan,
        raw_put,
        raw_batch_put,
        raw_delete,
        raw_delete_range,
        raw_batch_delete,
        unsafe_destroy_range,
        coprocessor,
        coprocessor_stream,
        mvcc_get_by_key,
        mvcc_get_by_start_ts,
        split_region,
        read_index,
    }

    pub label_enum GcCommandKind {
        gc,
        unsafe_destroy_range,
    }

    pub struct GrpcMsgHistogramVec: Histogram {
        "type" => GrpcTypeKind,
    }

    pub struct GrpcMsgFailCounterVec: IntCounter {
        "type" => GrpcTypeKind,
    }

    pub struct GcCommandCounterVec: IntCounter {
        "type" => GcCommandKind,
    }
}

lazy_static! {
    pub static ref SEND_SNAP_HISTOGRAM: Histogram = register_histogram!(
        "tikv_server_send_snapshot_duration_seconds",
        "Bucketed histogram of server send snapshots duration",
        exponential_buckets(0.05, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SNAP_TASK_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_snapshot_task_total",
        "Total number of snapshot task",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_MSG_HISTOGRAM_VEC: GrpcMsgHistogramVec = register_static_histogram_vec!(
        GrpcMsgHistogramVec,
        "tikv_grpc_msg_duration_seconds",
        "Bucketed histogram of grpc server messages",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref GRPC_MSG_FAIL_COUNTER: GrpcMsgFailCounterVec = register_static_int_counter_vec!(
        GrpcMsgFailCounterVec,
        "tikv_grpc_msg_fail_total",
        "Total number of handle grpc message failure",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_REQ_BATCH_COMMANDS_SIZE: Histogram = register_histogram!(
        "tikv_server_grpc_req_batch_size",
        "grpc batch size of gRPC requests",
        exponential_buckets(1f64, 2f64, 10).unwrap()
    )
    .unwrap();
    pub static ref GRPC_RESP_BATCH_COMMANDS_SIZE: Histogram = register_histogram!(
        "tikv_server_grpc_resp_batch_size",
        "grpc batch size of gRPC responses",
        exponential_buckets(1f64, 2f64, 10).unwrap()
    )
    .unwrap();
    pub static ref GC_COMMAND_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "gc_command_total",
        "Total number of GC commands received.",
        &["type"]
    )
    .unwrap();
    pub static ref GC_COMMAND_COUNTER_VEC_STATIC: GcCommandCounterVec =
        GcCommandCounterVec::from(&GC_COMMAND_COUNTER_VEC);
    pub static ref GC_EMPTY_RANGE_COUNTER: IntCounter = register_int_counter!(
        "tikv_storage_gc_empty_range_total",
        "Total number of empty range found by gc"
    )
    .unwrap();
    pub static ref GC_SKIPPED_COUNTER: IntCounter = register_int_counter!(
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
    pub static ref AUTO_GC_SAFE_POINT_GAUGE: IntGauge = register_int_gauge!(
        "tikv_gcworker_autogc_safe_point",
        "Safe point used for auto gc"
    )
    .unwrap();
    pub static ref AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_gcworker_autogc_processed_regions",
        "Processed regions by auto gc",
        &["type"]
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_RECV_COUNTER: IntCounter = register_int_counter!(
        "tikv_server_raft_message_recv_total",
        "Total number of raft messages received"
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_BATCH_SIZE: Histogram = register_histogram!(
        "tikv_server_raft_message_batch_size",
        "Raft messages batch size",
        exponential_buckets(1f64, 2f64, 10).unwrap()
    )
    .unwrap();
    pub static ref RESOLVE_STORE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_resolve_store_total",
        "Total number of resolving store",
        &["type"]
    )
    .unwrap();
    pub static ref REPORT_FAILURE_MSG_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_report_failure_msg_total",
        "Total number of reporting failure messages",
        &["type", "store_id"]
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_FLUSH_COUNTER: IntCounter = register_int_counter!(
        "tikv_server_raft_message_flush_total",
        "Total number of raft messages flushed immediately"
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_DELAY_FLUSH_COUNTER: IntCounter = register_int_counter!(
        "tikv_server_raft_message_delay_flush_total",
        "Total number of raft messages flushed delay"
    )
    .unwrap();
    pub static ref CONFIG_ROCKSDB_GAUGE: GaugeVec = register_gauge_vec!(
        "tikv_config_rocksdb",
        "Config information of rocksdb",
        &["cf", "name"]
    )
    .unwrap();
    pub static ref REQUEST_BATCH_SIZE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_server_request_batch_size",
        "Size of request batch input",
        &["type"],
        exponential_buckets(1f64, 5f64, 10).unwrap()
    )
    .unwrap();
    pub static ref REQUEST_BATCH_RATIO_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_server_request_batch_ratio",
        "Ratio of request batch output to input",
        &["type"],
        exponential_buckets(1f64, 5f64, 10).unwrap()
    )
    .unwrap();
}

make_static_metric! {
    pub label_enum RequestStatusKind {
        all,
        success,
        err_timeout,
        err_empty_request,
        err_other,
        err_io,
        err_server,
        err_invalid_resp,
        err_invalid_req,
        err_not_leader,
        err_region_not_found,
        err_key_not_in_region,
        err_epoch_not_match,
        err_server_is_busy,
        err_stale_command,
        err_store_not_match,
        err_raft_entry_too_large,
    }

    pub label_enum RequestTypeKind {
        write,
        snapshot,
    }

    pub struct AsyncRequestsCounterVec: IntCounter {
        "type" => RequestTypeKind,
        "status" => RequestStatusKind,
    }

    pub struct AsyncRequestsDurationVec: Histogram {
        "type" => RequestTypeKind,
    }
}

impl From<ErrorHeaderKind> for RequestStatusKind {
    fn from(kind: ErrorHeaderKind) -> Self {
        match kind {
            ErrorHeaderKind::NotLeader => RequestStatusKind::err_not_leader,
            ErrorHeaderKind::RegionNotFound => RequestStatusKind::err_region_not_found,
            ErrorHeaderKind::KeyNotInRegion => RequestStatusKind::err_key_not_in_region,
            ErrorHeaderKind::EpochNotMatch => RequestStatusKind::err_epoch_not_match,
            ErrorHeaderKind::ServerIsBusy => RequestStatusKind::err_server_is_busy,
            ErrorHeaderKind::StaleCommand => RequestStatusKind::err_stale_command,
            ErrorHeaderKind::StoreNotMatch => RequestStatusKind::err_store_not_match,
            ErrorHeaderKind::RaftEntryTooLarge => RequestStatusKind::err_raft_entry_too_large,
            ErrorHeaderKind::Other => RequestStatusKind::err_other,
        }
    }
}

lazy_static! {
    pub static ref ASYNC_REQUESTS_COUNTER_VEC: AsyncRequestsCounterVec = {
        register_static_int_counter_vec!(
            AsyncRequestsCounterVec,
            "tikv_storage_engine_async_request_total",
            "Total number of engine asynchronous requests",
            &["type", "status"]
        )
        .unwrap()
    };
    pub static ref ASYNC_REQUESTS_DURATIONS_VEC: AsyncRequestsDurationVec = {
        register_static_histogram_vec!(
            AsyncRequestsDurationVec,
            "tikv_storage_engine_async_request_duration_seconds",
            "Bucketed histogram of processing successful asynchronous requests.",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        )
        .unwrap()
    };
}

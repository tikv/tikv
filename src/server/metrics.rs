// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::{exponential_buckets, *};
use prometheus_static_metric::*;

pub use crate::storage::kv::metrics::{
    GcKeysCF, GcKeysCounterVec, GcKeysCounterVecInner, GcKeysDetail,
};
use crate::storage::ErrorHeaderKind;

make_auto_flush_static_metric! {
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
        kv_check_secondary_locks,
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
        raw_get_key_ttl,
        raw_compare_and_swap,
        raw_checksum,
        unsafe_destroy_range,
        physical_scan_lock,
        register_lock_observer,
        check_lock_observer,
        remove_lock_observer,
        coprocessor,
        coprocessor_stream,
        raw_coprocessor,
        mvcc_get_by_key,
        mvcc_get_by_start_ts,
        split_region,
        read_index,
        check_leader,
        batch_commands,
    }

    pub label_enum GcCommandKind {
        gc,
        gc_keys,
        unsafe_destroy_range,
        physical_scan_lock,
        validate_config,
        orphan_versions,
    }

    pub label_enum SnapTask {
        send,
        recv,
    }

    pub label_enum ResolveStore {
        resolving,
        resolve,
        failed,
        success,
        tombstone,
    }

    pub label_enum ReplicaReadLockCheckResult {
        unlocked,
        locked,
    }

    pub label_enum WhetherSuccess {
        success,
        fail,
    }

    pub struct GcCommandCounterVec: LocalIntCounter {
        "type" => GcCommandKind,
    }

    pub struct SnapTaskCounterVec: LocalIntCounter {
        "type" => SnapTask,
    }

    pub struct GcTaskCounterVec: LocalIntCounter {
        "task" => GcCommandKind,
    }

    pub struct GcTaskFailCounterVec: LocalIntCounter {
        "task" => GcCommandKind,
    }

    pub struct ResolveStoreCounterVec: LocalIntCounter {
        "type" => ResolveStore
    }

    pub struct GrpcMsgFailCounterVec: LocalIntCounter {
        "type" => GrpcTypeKind,
    }

    pub struct GrpcProxyMsgCounterVec: LocalIntCounter {
        "type" => GrpcTypeKind,
        "success" => WhetherSuccess,
    }

    pub struct GrpcMsgHistogramVec: LocalHistogram {
        "type" => GrpcTypeKind,
    }

    pub struct ReplicaReadLockCheckHistogramVec: LocalHistogram {
        "result" => ReplicaReadLockCheckResult,
    }
}

make_static_metric! {
    pub label_enum GlobalGrpcTypeKind {
        kv_get,
    }

    pub label_enum BatchableRequestKind {
        kv_get,
        raw_get,
    }

    pub struct GrpcMsgHistogramGlobal: Histogram {
        "type" => GlobalGrpcTypeKind,
    }

    pub struct RequestBatchSizeHistogramVec: Histogram {
        "type" => BatchableRequestKind,
    }

    pub label_enum FlushReason {
        full,
        full_after_delay,
        delay,
        eof,
        wake,
    }

    pub struct RaftMessageFlushCounterVec: IntCounter {
        "reason" => FlushReason,
    }
}

lazy_static! {
    pub static ref GC_COMMAND_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "gc_command_total",
        "Total number of GC commands received.",
        &["type"]
    )
    .unwrap();
    pub static ref SNAP_TASK_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_snapshot_task_total",
        "Total number of snapshot task",
        &["type"]
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
    pub static ref RESOLVE_STORE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_resolve_store_total",
        "Total number of resolving store",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_MSG_FAIL_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_grpc_msg_fail_total",
        "Total number of handle grpc message failure",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_PROXY_MSG_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_grpc_proxy_msg_total",
        "Total number of handle grpc proxy message",
        &["type", "success"]
    )
    .unwrap();
    pub static ref GC_KEYS_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_gcworker_gc_keys",
        "Counter of keys affected during gc",
        &["cf", "tag"]
    )
    .unwrap();
    pub static ref GC_KEY_FAILURES: IntCounter = register_int_counter!(
        "tikv_gcworker_gc_key_failures",
        "Counter of gc key failures"
    ).unwrap();
    pub static ref GRPC_MSG_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_grpc_msg_duration_seconds",
        "Bucketed histogram of grpc server messages",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SERVER_INFO_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_server_info",
        "Indicate the tikv server info, and the value is the server startup timestamp(s).",
        &["version", "hash"]
    )
    .unwrap();
    pub static ref REPLICA_READ_LOCK_CHECK_HISTOGRAM_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_replica_read_lock_check_duration_seconds",
            "Duration of memory lock checking for replica read",
            &["result"],
            exponential_buckets(1e-6f64, 4f64, 10).unwrap() // 1us ~ 262ms
        )
        .unwrap();
    pub static ref ADDRESS_RESOLVE_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_server_address_resolve_duration_secs",
            "Duration of resolving store address",
            exponential_buckets(0.0001, 2.0, 20).unwrap()
        )
        .unwrap();
}

lazy_static! {
    pub static ref GRPC_MSG_HISTOGRAM_STATIC: GrpcMsgHistogramVec =
        auto_flush_from!(GRPC_MSG_HISTOGRAM_VEC, GrpcMsgHistogramVec);
    pub static ref GRPC_MSG_HISTOGRAM_GLOBAL: GrpcMsgHistogramGlobal =
        GrpcMsgHistogramGlobal::from(&GRPC_MSG_HISTOGRAM_VEC);
    pub static ref GC_COMMAND_COUNTER_VEC_STATIC: GcCommandCounterVec =
        auto_flush_from!(GC_COMMAND_COUNTER_VEC, GcCommandCounterVec);
    pub static ref SNAP_TASK_COUNTER_STATIC: SnapTaskCounterVec =
        auto_flush_from!(SNAP_TASK_COUNTER, SnapTaskCounterVec);
    pub static ref GC_GCTASK_COUNTER_STATIC: GcTaskCounterVec =
        auto_flush_from!(GC_GCTASK_COUNTER_VEC, GcTaskCounterVec);
    pub static ref GC_GCTASK_FAIL_COUNTER_STATIC: GcTaskFailCounterVec =
        auto_flush_from!(GC_GCTASK_FAIL_COUNTER_VEC, GcTaskFailCounterVec);
    pub static ref RESOLVE_STORE_COUNTER_STATIC: ResolveStoreCounterVec =
        auto_flush_from!(RESOLVE_STORE_COUNTER, ResolveStoreCounterVec);
    pub static ref GRPC_MSG_FAIL_COUNTER: GrpcMsgFailCounterVec =
        auto_flush_from!(GRPC_MSG_FAIL_COUNTER_VEC, GrpcMsgFailCounterVec);
    pub static ref GRPC_PROXY_MSG_COUNTER: GrpcProxyMsgCounterVec =
        auto_flush_from!(GRPC_PROXY_MSG_COUNTER_VEC, GrpcProxyMsgCounterVec);
    pub static ref GC_KEYS_COUNTER_STATIC: GcKeysCounterVec =
        auto_flush_from!(GC_KEYS_COUNTER_VEC, GcKeysCounterVec);
    pub static ref REPLICA_READ_LOCK_CHECK_HISTOGRAM_VEC_STATIC: ReplicaReadLockCheckHistogramVec = auto_flush_from!(
        REPLICA_READ_LOCK_CHECK_HISTOGRAM_VEC,
        ReplicaReadLockCheckHistogramVec
    );
}

lazy_static! {
    pub static ref SEND_SNAP_HISTOGRAM: Histogram = register_histogram!(
        "tikv_server_send_snapshot_duration_seconds",
        "Bucketed histogram of server send snapshots duration",
        exponential_buckets(0.05, 2.0, 20).unwrap()
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
    pub static ref GC_TOO_BUSY_COUNTER: IntCounter = register_int_counter!(
        "tikv_gc_worker_too_busy",
        "Counter of occurrence of gc_worker being too busy"
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
    pub static ref TTL_CHECKER_PROCESSED_REGIONS_GAUGE: IntGauge = register_int_gauge!(
        "tikv_ttl_checker_processed_regions",
        "Processed regions by ttl checker"
    )
    .unwrap();
    pub static ref TTL_CHECKER_ACTIONS_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_ttl_checker_actions",
        "Actions of ttl checker",
        &["type"]
    )
    .unwrap();
    pub static ref TTL_CHECKER_COMPACT_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_ttl_checker_compact_duration",
        "Duration of ttl checker compact files execution",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref TTL_CHECKER_POLL_INTERVAL_GAUGE: IntGauge = register_int_gauge!(
        "tikv_ttl_checker_poll_interval",
        "Interval of ttl checker poll"
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
    pub static ref REPORT_FAILURE_MSG_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_server_report_failure_msg_total",
        "Total number of reporting failure messages",
        &["type", "store_id"]
    )
    .unwrap();
    pub static ref RAFT_MESSAGE_FLUSH_COUNTER: RaftMessageFlushCounterVec =
        register_static_int_counter_vec!(
            RaftMessageFlushCounterVec,
            "tikv_server_raft_message_flush_total",
            "Total number of raft messages flushed immediately",
            &["reason"]
        )
        .unwrap();
    pub static ref CONFIG_ROCKSDB_GAUGE: GaugeVec = register_gauge_vec!(
        "tikv_config_rocksdb",
        "Config information of rocksdb",
        &["cf", "name"]
    )
    .unwrap();
    pub static ref REQUEST_BATCH_SIZE_HISTOGRAM_VEC: RequestBatchSizeHistogramVec =
        register_static_histogram_vec!(
            RequestBatchSizeHistogramVec,
            "tikv_server_request_batch_size",
            "Size of request batch input",
            &["type"],
            vec![1.0, 2.0, 4.0, 8.0, 12.0, 16.0, 20.0, 24.0, 28.0, 32.0, 64.0]
        )
        .unwrap();
    pub static ref CPU_CORES_QUOTA_GAUGE: Gauge = register_gauge!(
        "tikv_server_cpu_cores_quota",
        "Total CPU cores quota for TiKV server"
    )
    .unwrap();
    pub static ref MEM_TRACE_SUM_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "tikv_server_mem_trace_sum",
        "The sum of memory trace for TiKV server",
        &["name"]
    )
    .unwrap();
    pub static ref MEMORY_USAGE_GAUGE: IntGauge =
        register_int_gauge!("tikv_server_memory_usage", "Memory usage for the instance").unwrap();
    pub static ref RAFT_APPEND_REJECTS: IntCounter = register_int_counter!(
        "tikv_server_raft_append_rejects",
        "Count for rejected Raft append messages"
    )
    .unwrap();
}

make_auto_flush_static_metric! {
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
        err_leader_memory_lock_check,
    }

    pub label_enum RequestTypeKind {
        write,
        snapshot,
    }

    pub struct AsyncRequestsCounterVec: LocalIntCounter {
        "type" => RequestTypeKind,
        "status" => RequestStatusKind,
    }

    pub struct AsyncRequestsDurationVec: LocalHistogram {
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
    pub static ref ASYNC_REQUESTS_COUNTER: IntCounterVec = register_int_counter_vec!(
        "tikv_storage_engine_async_request_total",
        "Total number of engine asynchronous requests",
        &["type", "status"]
    )
    .unwrap();
    pub static ref ASYNC_REQUESTS_DURATIONS: HistogramVec = register_histogram_vec!(
        "tikv_storage_engine_async_request_duration_seconds",
        "Bucketed histogram of processing successful asynchronous requests.",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
}

lazy_static! {
    pub static ref ASYNC_REQUESTS_COUNTER_VEC: AsyncRequestsCounterVec =
        auto_flush_from!(ASYNC_REQUESTS_COUNTER, AsyncRequestsCounterVec);
    pub static ref ASYNC_REQUESTS_DURATIONS_VEC: AsyncRequestsDurationVec =
        auto_flush_from!(ASYNC_REQUESTS_DURATIONS, AsyncRequestsDurationVec);
}

// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum MvccConflictKind {
        prewrite_write_conflict,
        rolled_back,
        commit_lock_not_found,
        rollback_committed,
        acquire_pessimistic_lock_conflict,
        pipelined_acquire_pessimistic_lock_amend_fail,
        pipelined_acquire_pessimistic_lock_amend_success,
    }

    pub label_enum MvccDuplicateCommandKind {
        prewrite,
        commit,
        rollback,
        acquire_pessimistic_lock,
    }

    pub label_enum MvccCheckTxnStatusKind {
        rollback,
        update_ts,
        get_commit_info,
        pessimistic_rollback,
    }

    pub label_enum MvccPrewriteAssertionPerfKind {
        none,
        write_loaded,
        non_data_version_reload,
        write_not_loaded_reload,
        write_not_loaded_skip
    }

    pub label_enum ScanLockReadTimeSource {
        resolve_lock,
        pessimistic_rollback,
    }

    pub struct MvccConflictCounterVec: IntCounter {
        "type" => MvccConflictKind,
    }

    pub struct MvccDuplicateCmdCounterVec: IntCounter {
        "type" => MvccDuplicateCommandKind,
    }

    pub struct MvccCheckTxnStatusCounterVec: IntCounter {
        "type" => MvccCheckTxnStatusKind,
    }

    pub struct MvccPrewriteAssertionPerfCounterVec: IntCounter {
        "type" => MvccPrewriteAssertionPerfKind,
    }

    pub struct MvccPrewriteRequestAfterCommitCounterVec: IntCounter {
        "type" => {
            non_retry_req,
            retry_req,
        },
    }

    pub struct ScanLockReadTimeVec: Histogram {
        "type" => ScanLockReadTimeSource,
    }
}

lazy_static! {
    pub static ref MVCC_VERSIONS_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_storage_mvcc_versions",
        "Histogram of versions for each key",
        &["key_mode"],
        exponential_buckets(1.0, 2.0, 30).unwrap()
    )
    .unwrap();
    pub static ref GC_DELETE_VERSIONS_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_storage_mvcc_gc_delete_versions",
        "Histogram of versions deleted by gc for each key",
        &["key_mode"],
        exponential_buckets(1.0, 2.0, 30).unwrap()
    )
    .unwrap();
    pub static ref MVCC_CONFLICT_COUNTER: MvccConflictCounterVec = {
        register_static_int_counter_vec!(
            MvccConflictCounterVec,
            "tikv_storage_mvcc_conflict_counter",
            "Total number of conflict error",
            &["type"]
        )
        .unwrap()
    };
    pub static ref MVCC_DUPLICATE_CMD_COUNTER_VEC: MvccDuplicateCmdCounterVec = {
        register_static_int_counter_vec!(
            MvccDuplicateCmdCounterVec,
            "tikv_storage_mvcc_duplicate_cmd_counter",
            "Total number of duplicated commands",
            &["type"]
        )
        .unwrap()
    };
    pub static ref MVCC_CHECK_TXN_STATUS_COUNTER_VEC: MvccCheckTxnStatusCounterVec = {
        register_static_int_counter_vec!(
            MvccCheckTxnStatusCounterVec,
            "tikv_storage_mvcc_check_txn_status",
            "Counter of different results of check_txn_status",
            &["type"]
        )
        .unwrap()
    };
    pub static ref MVCC_PREWRITE_ASSERTION_PERF_COUNTER_VEC: MvccPrewriteAssertionPerfCounterVec = {
        register_static_int_counter_vec!(
            MvccPrewriteAssertionPerfCounterVec,
            "tikv_storage_mvcc_prewrite_assertion_perf",
            "Counter of assertion operations in transactions",
            &["type"]
        )
        .unwrap()
    };
    pub static ref MVCC_PREWRITE_REQUEST_AFTER_COMMIT_COUNTER_VEC: MvccPrewriteRequestAfterCommitCounterVec = {
        register_static_int_counter_vec!(
            MvccPrewriteRequestAfterCommitCounterVec,
            "tikv_storage_mvcc_prewrite_request_after_commit_counter",
            "Counter of prewrite requests of already-committed transactions that are determined by checking TxnStatucCache",
            &["type"]
        )
        .unwrap()
    };
    pub static ref SCAN_LOCK_READ_TIME_VEC: ScanLockReadTimeVec = register_static_histogram_vec!(
        ScanLockReadTimeVec,
        "tikv_storage_mvcc_scan_lock_read_duration_seconds",
        "Bucketed histogram of memory lock read lock hold for scan lock",
        &["type"],
        exponential_buckets(0.00001, 2.0, 20).unwrap()
    )
    .unwrap();
}

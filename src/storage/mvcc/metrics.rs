// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum MvccConflictKind {
        prewrite_write_conflict,
        commit_lock_not_found,
        rollback_committed,
        acquire_pessimistic_lock_conflict,
    }

    pub label_enum MvccDuplicateCommandKind {
        prewrite,
        commit,
        rollback,
        acquire_pessimistic_lock,
    }

    pub struct MvccConflictCounterVec: IntCounter {
        "type" => MvccConflictKind,
    }

    pub struct MvccDuplicateCmdCounterVec: IntCounter {
        "type" => MvccDuplicateCommandKind,
    }
}

lazy_static! {
    pub static ref MVCC_VERSIONS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_storage_mvcc_versions",
        "Histogram of versions for each key",
        exponential_buckets(1.0, 2.0, 30).unwrap()
    )
    .unwrap();
    pub static ref GC_DELETE_VERSIONS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_storage_mvcc_gc_delete_versions",
        "Histogram of versions deleted by gc for each key",
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
}

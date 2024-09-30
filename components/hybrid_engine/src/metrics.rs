// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, IntCounterVec};
use prometheus_static_metric::{auto_flush_from, make_auto_flush_static_metric};

make_auto_flush_static_metric! {
    pub label_enum SnapshotType {
        rocksdb,
        region_cache_engine,
    }

    pub struct SnapshotTypeCountVec: LocalIntCounter {
        "type" => SnapshotType,
    }

    pub label_enum FailedReason {
        no_read_ts,
        not_cached,
        too_old_read,
        epoch_not_match,
    }

    pub struct FailedReasonCountVec: LocalIntCounter {
        "type" => FailedReason,
    }
}

lazy_static! {
    pub static ref SNAPSHOT_TYPE_COUNT_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_snapshot_type_count",
        "Number of each snapshot type used for iteration",
        &["type"],
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_VEC: IntCounterVec =
        register_int_counter_vec!(
            "tikv_in_memory_engine_snapshot_acquire_failed_reason_count",
            "The reasons for why range cache snapshot is not acquired",
            &["type"],
        )
        .unwrap();
}

lazy_static! {
    pub static ref SNAPSHOT_TYPE_COUNT_STATIC: SnapshotTypeCountVec =
        auto_flush_from!(SNAPSHOT_TYPE_COUNT_VEC, SnapshotTypeCountVec);
    pub static ref IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC: FailedReasonCountVec = auto_flush_from!(
        IN_MEMORY_ENGINE_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_VEC,
        FailedReasonCountVec
    );
}

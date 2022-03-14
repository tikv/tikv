// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum SnapType {
       generate,
       apply,
    }

    pub label_enum SnapStatus {
        all,
        success,
        abort,
        fail,
        delay,
        ignore,
    }

    pub struct SnapCounter: LocalIntCounter {
        "type" => SnapType,
        "status" => SnapStatus,
    }

    pub struct CheckSplitCounter : LocalIntCounter {
        "type" => SnapStatus,
    }

    pub struct SnapHistogram : LocalHistogram {
        "type" => SnapType,
    }
}

make_static_metric! {
    pub label_enum RejectReason {
        store_id_mismatch,
        peer_id_mismatch,
        term_mismatch,
        lease_expire,
        no_region,
        no_lease,
        epoch,
        applied_term,
        channel_full,
        safe_ts,
    }

    pub struct ReadRejectCounter : IntCounter {
       "reason" => RejectReason
    }
}

lazy_static! {
    pub static ref SNAP_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_snapshot_total",
        "Total number of raftstore snapshot processed.",
        &["type", "status"]
    )
    .unwrap();
    pub static ref SNAP_COUNTER: SnapCounter = auto_flush_from!(SNAP_COUNTER_VEC, SnapCounter);
    pub static ref CHECK_SPILT_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_check_split_total",
        "Total number of raftstore split check.",
        &["type"]
    )
    .unwrap();
    pub static ref CHECK_SPILT_COUNTER: CheckSplitCounter =
        auto_flush_from!(CHECK_SPILT_COUNTER_VEC, CheckSplitCounter);
    pub static ref SNAP_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_raftstore_snapshot_duration_seconds",
        "Bucketed histogram of raftstore snapshot process duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SNAP_HISTOGRAM: SnapHistogram =
        auto_flush_from!(SNAP_HISTOGRAM_VEC, SnapHistogram);
    pub static ref CHECK_SPILT_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_check_split_duration_seconds",
        "Bucketed histogram of raftstore split check duration",
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COMPACT_RANGE_CF: HistogramVec = register_histogram_vec!(
        "tikv_compact_range_cf_duration_seconds",
        "Bucketed histogram of compact range for cf execution",
        &["cf"]
    )
    .unwrap();
    pub static ref REGION_HASH_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_hash_duration_seconds",
        "Bucketed histogram of raftstore hash computation duration"
    )
    .unwrap();
    pub static ref STALE_PEER_PENDING_DELETE_RANGE_GAUGE: Gauge = register_gauge!(
        "tikv_pending_delete_ranges_of_stale_peer",
        "Total number of tikv pending delete range of stale peer"
    )
    .unwrap();
    pub static ref CLEAN_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_clean_region_count",
        "Total number of region-worker clean range operations",
        &["type"]
    )
    .unwrap();
    pub static ref LOCAL_READ_REJECT_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_raftstore_local_read_reject_total",
        "Total number of rejections from the local reader.",
        &["reason"]
    )
    .unwrap();
    pub static ref LOCAL_READ_REJECT: ReadRejectCounter =
        ReadRejectCounter::from(&LOCAL_READ_REJECT_VEC);
    pub static ref LOCAL_READ_EXECUTED_REQUESTS: IntCounter = register_int_counter!(
        "tikv_raftstore_local_read_executed_requests",
        "Total number of requests directly executed by local reader."
    )
    .unwrap();
    pub static ref LOCAL_READ_EXECUTED_CACHE_REQUESTS: IntCounter = register_int_counter!(
        "tikv_raftstore_local_read_cache_requests",
        "Total number of requests directly executed by local reader."
    )
    .unwrap();
    pub static ref LOCAL_READ_EXECUTED_STALE_READ_REQUESTS: IntCounter = register_int_counter!(
        "tikv_raftstore_local_read_executed_stale_read_requests",
        "Total number of stale read requests directly executed by local reader."
    )
    .unwrap();
    pub static ref RAFT_LOG_GC_WRITE_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_raft_log_gc_write_duration_secs",
        "Bucketed histogram of write duration of raft log gc.",
        exponential_buckets(0.0001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RAFT_LOG_GC_SEEK_OPERATIONS: IntCounter = register_int_counter!(
        "tikv_raftstore_raft_log_gc_seek_operations_count",
        "Total number of seek operations from raft log gc."
    )
    .unwrap();
    pub static ref RAFT_LOG_GC_DELETED_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_raft_log_gc_deleted_keys",
        "Bucket of number of deleted keys from raft log gc.",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RAFT_LOG_GC_FAILED: IntCounter = register_int_counter!(
        "tikv_raftstore_raft_log_gc_failed",
        "Total number of failed raft log gc."
    )
    .unwrap();
    pub static ref RAFT_LOG_GC_KV_SYNC_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_raft_log_kv_sync_duration_secs",
        "Bucketed histogram of kv sync duration of raft log gc.",
        exponential_buckets(0.0001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref LOCAL_READ_RENEW_LEASE_ADVANCE_COUNTER: IntCounter = register_int_counter!(
        "tikv_raftstore_local_read_renew_lease_advance_count",
        "Total number of renewing lease in advance from local reader."
    )
    .unwrap();
}

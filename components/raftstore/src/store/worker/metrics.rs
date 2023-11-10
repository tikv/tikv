// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::RefCell, time::Duration};

use lazy_static::lazy_static;
use prometheus::{local::LocalIntCounter, *};
use prometheus_static_metric::*;
use tikv_util::time::Instant;

make_auto_flush_static_metric! {
    pub label_enum SnapType {
       generate,
       apply
    }

    // snapshot task status
    // |all---------start--------------|
    //               |
    //               |
    //               V
    // |success|abort|fail|delay|ignore|
    pub label_enum SnapStatus {
        all,
        start,
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
        cache_miss,
        safe_ts,
        witness,
        flashback_not_prepared,
        flashback_in_progress,
        wait_data,
    }

    pub struct LocalReadRejectCounter : LocalIntCounter {
       "reason" => RejectReason,
    }
}

pub struct LocalReadMetrics {
    pub local_executed_requests: LocalIntCounter,
    pub local_executed_stale_read_requests: LocalIntCounter,
    pub local_executed_stale_read_fallback_success_requests: LocalIntCounter,
    pub local_executed_stale_read_fallback_failure_requests: LocalIntCounter,
    pub local_executed_replica_read_requests: LocalIntCounter,
    pub local_executed_snapshot_cache_hit: LocalIntCounter,
    pub reject_reason: LocalReadRejectCounter,
    pub renew_lease_advance: LocalIntCounter,
    last_flush_time: Instant,
}

thread_local! {
    pub static TLS_LOCAL_READ_METRICS: RefCell<LocalReadMetrics> = RefCell::new(
        LocalReadMetrics {
            local_executed_requests: LOCAL_READ_EXECUTED_REQUESTS.local(),
            local_executed_stale_read_requests: LOCAL_READ_EXECUTED_STALE_READ_REQUESTS.local(),
            local_executed_stale_read_fallback_success_requests: LOCAL_READ_EXECUTED_STALE_READ_FALLBACK_SUCCESS_REQUESTS.local(),
            local_executed_stale_read_fallback_failure_requests: LOCAL_READ_EXECUTED_STALE_READ_FALLBACK_FAILURE_REQUESTS.local(),
            local_executed_replica_read_requests: LOCAL_READ_EXECUTED_REPLICA_READ_REQUESTS.local(),
            local_executed_snapshot_cache_hit: LOCAL_READ_EXECUTED_CACHE_REQUESTS.local(),
            reject_reason: LocalReadRejectCounter::from(&LOCAL_READ_REJECT_VEC),
            renew_lease_advance: LOCAL_READ_RENEW_LEASE_ADVANCE_COUNTER.local(),
            last_flush_time: Instant::now_coarse(),
        }
    );
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

pub fn maybe_tls_local_read_metrics_flush() {
    TLS_LOCAL_READ_METRICS.with(|m| {
        let mut m = m.borrow_mut();

        if m.last_flush_time.saturating_elapsed() >= Duration::from_millis(METRICS_FLUSH_INTERVAL) {
            m.local_executed_requests.flush();
            m.local_executed_stale_read_requests.flush();
            m.local_executed_stale_read_fallback_success_requests
                .flush();
            m.local_executed_stale_read_fallback_failure_requests
                .flush();
            m.local_executed_replica_read_requests.flush();
            m.local_executed_snapshot_cache_hit.flush();
            m.reject_reason.flush();
            m.renew_lease_advance.flush();
            m.last_flush_time = Instant::now_coarse();
        }
    });
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
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref SNAP_HISTOGRAM: SnapHistogram =
        auto_flush_from!(SNAP_HISTOGRAM_VEC, SnapHistogram);
    pub static ref SNAP_GEN_WAIT_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_snapshot_generation_wait_duration_seconds",
        "Bucketed histogram of raftstore snapshot generation wait duration",
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref CHECK_SPILT_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_check_split_duration_seconds",
        "Bucketed histogram of raftstore split check duration",
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref COMPACT_RANGE_CF: HistogramVec = register_histogram_vec!(
        "tikv_compact_range_cf_duration_seconds",
        "Bucketed histogram of compact range for cf execution",
        &["cf"]
    )
    .unwrap();
    pub static ref FULL_COMPACT: Histogram = register_histogram!(
        "tikv_storage_full_compact_duration_seconds",
        "Bucketed histogram of full compaction for the storage."
    )
    .unwrap();
    pub static ref FULL_COMPACT_INCREMENTAL: Histogram = register_histogram!(
        "tikv_storage_full_compact_increment_duration_seconds",
        "Bucketed histogram of full compaction increments for the storage."
    )
    .unwrap();
    pub static ref FULL_COMPACT_PAUSE: Histogram = register_histogram!(
        "tikv_storage_full_compact_pause_duration_seconds",
        "Bucketed histogram of full compaction pauses for the storage."
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
    pub static ref LOCAL_READ_EXECUTED_STALE_READ_FALLBACK_SUCCESS_REQUESTS: IntCounter =
        register_int_counter!(
            "tikv_raftstore_local_read_executed_stale_read_fallback_success_requests",
            "Total number of stale read requests executed by local leader peer as snapshot read."
        )
        .unwrap();
    pub static ref LOCAL_READ_EXECUTED_STALE_READ_FALLBACK_FAILURE_REQUESTS: IntCounter =
        register_int_counter!(
            "tikv_raftstore_local_read_executed_stale_read_fallback_failure_requests",
            "Total number of stale read requests failed to be executed by local leader peer as snapshot read."
        )
        .unwrap();
    pub static ref LOCAL_READ_EXECUTED_REPLICA_READ_REQUESTS: IntCounter = register_int_counter!(
        "tikv_raftstore_local_read_executed_replica_read_requests",
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

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::OnceLock;
use prometheus::*;

// When adding new metrics, remember to update in the grafana dashboard, for
// example update the tikv_details.dashboard.py.

static BACKUP_RANGE_HISTOGRAM_VEC_INSTANCE: OnceLock<HistogramVec> = OnceLock::new();
pub fn backup_range_histogram_vec() -> &'static HistogramVec {
    BACKUP_RANGE_HISTOGRAM_VEC_INSTANCE.get_or_init(|| {
        register_histogram_vec!(
            "tikv_backup_range_duration_seconds",
            "Bucketed histogram of backup range duration",
            &["type"],
            // Start from 10ms.
            exponential_buckets(0.01, 2.0, 16).unwrap()
        )
        .unwrap()
    })
}

static BACKUP_RANGE_SIZE_HISTOGRAM_VEC_INSTANCE: OnceLock<HistogramVec> = OnceLock::new();
pub fn backup_range_size_histogram_vec() -> &'static HistogramVec {
    BACKUP_RANGE_SIZE_HISTOGRAM_VEC_INSTANCE.get_or_init(|| {
        register_histogram_vec!(
            "tikv_backup_range_size_bytes",
            "Bucketed histogram of backup range size",
            &["cf"],
            // Start from 4 KB.
            exponential_buckets((4 * (1 << 10)) as f64, 2.0, 20).unwrap()
        )
        .unwrap()
    })
}

static BACKUP_THREAD_POOL_SIZE_GAUGE_INSTANCE: OnceLock<IntGauge> = OnceLock::new();
pub fn backup_thread_pool_size_gauge() -> &'static IntGauge {
    BACKUP_THREAD_POOL_SIZE_GAUGE_INSTANCE.get_or_init(|| {
        register_int_gauge!(
            "tikv_backup_thread_pool_size",
            "Total size of backup thread pool"
        )
        .unwrap()
    })
}

static BACKUP_RANGE_ERROR_VEC_INSTANCE: OnceLock<IntCounterVec> = OnceLock::new();
pub fn backup_range_error_vec() -> &'static IntCounterVec {
    BACKUP_RANGE_ERROR_VEC_INSTANCE.get_or_init(|| {
        register_int_counter_vec!(
            "tikv_backup_error_counter",
            "Total number of backup errors",
            &["error"]
        )
        .unwrap()
    })
}

static BACKUP_SOFTLIMIT_GAUGE_INSTANCE: OnceLock<IntGauge> = OnceLock::new();
pub fn backup_softlimit_gauge() -> &'static IntGauge {
    BACKUP_SOFTLIMIT_GAUGE_INSTANCE.get_or_init(|| {
        register_int_gauge!(
            "tikv_backup_softlimit",
            "Soft limit applied to the backup thread pool."
        )
        .unwrap()
    })
}

static BACKUP_SCAN_WAIT_FOR_WRITER_HISTOGRAM_INSTANCE: OnceLock<Histogram> = OnceLock::new();
pub fn backup_scan_wait_for_writer_histogram() -> &'static Histogram {
    BACKUP_SCAN_WAIT_FOR_WRITER_HISTOGRAM_INSTANCE.get_or_init(|| {
        register_histogram!(
            "tikv_backup_scan_wait_for_writer_seconds",
            "The time backup scanner wait for available writer."
        )
        .unwrap()
    })
}

static BACKUP_SCAN_KV_COUNT_INSTANCE: OnceLock<IntCounterVec> = OnceLock::new();
pub fn backup_scan_kv_count() -> &'static IntCounterVec {
    BACKUP_SCAN_KV_COUNT_INSTANCE.get_or_init(|| {
        register_int_counter_vec!(
            "tikv_backup_scan_kv_count",
            "Total number of kvs backed up",
            &["cf"],
        )
        .unwrap()
    })
}

static BACKUP_SCAN_KV_SIZE_INSTANCE: OnceLock<IntCounterVec> = OnceLock::new();
pub fn backup_scan_kv_size() -> &'static IntCounterVec {
    BACKUP_SCAN_KV_SIZE_INSTANCE.get_or_init(|| {
        register_int_counter_vec!(
            "tikv_backup_scan_kv_size_bytes",
            "Total size of kvs backed up",
            &["cf"],
        )
        .unwrap()
    })
}

static BACKUP_RAW_EXPIRED_COUNT_INSTANCE: OnceLock<IntCounter> = OnceLock::new();
pub fn backup_raw_expired_count() -> &'static IntCounter {
    BACKUP_RAW_EXPIRED_COUNT_INSTANCE.get_or_init(|| {
        register_int_counter!(
            "tikv_backup_raw_expired_count",
            "Total number of rawkv expired during scan",
        )
        .unwrap()
    })
}

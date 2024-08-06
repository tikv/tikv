// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

/// When adding new metrics, remember to update in the grafana dashboard, for
/// example update the tikv_details.json file.
lazy_static! {
    pub static ref BACKUP_RANGE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_backup_range_duration_seconds",
        "Bucketed histogram of backup range duration",
        &["type"],
        // Start from 10ms.
        exponential_buckets(0.01, 2.0, 16).unwrap()
    )
    .unwrap();
    pub static ref BACKUP_RANGE_SIZE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_backup_range_size_bytes",
        "Bucketed histogram of backup range size",
        &["cf"],
        // Start from 4 KB.
        exponential_buckets((4 * (1 << 10)) as f64, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref BACKUP_THREAD_POOL_SIZE_GAUGE: IntGauge = register_int_gauge!(
        "tikv_backup_thread_pool_size",
        "Total size of backup thread pool"
    )
    .unwrap();
    pub static ref BACKUP_RANGE_ERROR_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_backup_error_counter",
        "Total number of backup errors",
        &["error"]
    )
    .unwrap();
    pub static ref BACKUP_SOFTLIMIT_GAUGE: IntGauge = register_int_gauge!(
        "tikv_backup_softlimit",
        "Soft limit applied to the backup thread pool."
    )
    .unwrap();
    pub static ref BACKUP_SCAN_WAIT_FOR_WRITER_HISTOGRAM : Histogram = register_histogram!(
        "tikv_backup_scan_wait_for_writer_seconds",
        "The time backup scanner wait for available writer."
    )
    .unwrap();
    pub static ref BACKUP_SCAN_KV_COUNT : IntCounterVec = register_int_counter_vec!(
        "tikv_backup_scan_kv_count",
        "Total number of kvs backed up",
        &["cf"],
    )
    .unwrap();
    pub static ref BACKUP_SCAN_KV_SIZE : IntCounterVec = register_int_counter_vec!(
        "tikv_backup_scan_kv_size_bytes",
        "Total size of kvs backed up",
        &["cf"],
    )
    .unwrap();
    pub static ref BACKUP_RAW_EXPIRED_COUNT : IntCounter = register_int_counter!(
        "tikv_backup_raw_expired_count",
        "Total number of rawkv expired during scan",
    )
    .unwrap();
}

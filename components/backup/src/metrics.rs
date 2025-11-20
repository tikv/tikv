// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::LazyLock;
use prometheus::*;

// When adding new metrics, remember to update in the grafana dashboard, for
// example update the tikv_details.dashboard.py.

pub static BACKUP_RANGE_HISTOGRAM_VEC: LazyLock<HistogramVec> = LazyLock::new(|| {
  register_histogram_vec!(
    "tikv_backup_range_duration_seconds",
    "Bucketed histogram of backup range duration",
    &["type"],
    exponential_buckets(0.01, 2.0, 16).unwrap()
  )
  .unwrap()
});

pub static BACKUP_RANGE_SIZE_HISTOGRAM_VEC: LazyLock<HistogramVec> = LazyLock::new(|| {
  register_histogram_vec!(
    "tikv_backup_range_size_bytes",
    "Bucketed histogram of backup range size",
    &["cf"],
    exponential_buckets((4 * (1 << 10)) as f64, 2.0, 20).unwrap()
  )
  .unwrap()
});

pub static BACKUP_THREAD_POOL_SIZE_GAUGE: LazyLock<IntGauge> = LazyLock::new(|| {
  register_int_gauge!(
    "tikv_backup_thread_pool_size",
    "Total size of backup thread pool"
  )
  .unwrap()
});

pub static BACKUP_RANGE_ERROR_VEC: LazyLock<IntCounterVec> = LazyLock::new(|| {
  register_int_counter_vec!(
    "tikv_backup_error_counter",
    "Total number of backup errors",
    &["error"]
  )
  .unwrap()
});

pub static BACKUP_SOFTLIMIT_GAUGE: LazyLock<IntGauge> = LazyLock::new(|| {
  register_int_gauge!(
    "tikv_backup_softlimit",
    "Soft limit applied to the backup thread pool."
  )
  .unwrap()
});

pub static BACKUP_SCAN_WAIT_FOR_WRITER_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
  register_histogram!(
    "tikv_backup_scan_wait_for_writer_seconds",
    "The time backup scanner wait for available writer."
  )
  .unwrap()
});

pub static BACKUP_SCAN_KV_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
  register_int_counter_vec!(
    "tikv_backup_scan_kv_count",
    "total number of kvs backed up",
    &["cf"],
  )
  .unwrap()
});

pub static BACKUP_SCAN_KV_SIZE: LazyLock<IntCounterVec> = LazyLock::new(|| {
  register_int_counter_vec!(
    "tikv_backup_scan_kv_size_bytes",
    "Total size of kvs backed up",
    &["cf"],
  )
  .unwrap()
});

pub static BACKUP_RAW_EXPIRED_COUNT: LazyLock<IntCounter> = LazyLock::new(|| {
  register_int_counter!(
    "tikv_backup_raw_expired_count",
    "Total number of rawkv expired during scan",
  )
  .unwrap()
});


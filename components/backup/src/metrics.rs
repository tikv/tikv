// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref BACKUP_REQUEST_HISTOGRAM: Histogram = register_histogram!(
        "tikv_backup_request_duration_seconds",
        "Bucketed histogram of backup requests duration"
    )
    .unwrap();
    pub static ref BACKUP_RANGE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_backup_range_duration_seconds",
        "Bucketed histogram of backup range duration",
        &["type"]
    )
    .unwrap();
    pub static ref BACKUP_RANGE_SIZE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_backup_range_size_bytes",
        "Bucketed histogram of backup range size",
        &["cf"],
        exponential_buckets(32.0, 2.0, 20).unwrap()
    )
    .unwrap();
}

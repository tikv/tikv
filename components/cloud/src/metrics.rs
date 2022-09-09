// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref CLOUD_REQUEST_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_cloud_request_duration_seconds",
        "Bucketed histogram of cloud requests duration",
        &["cloud", "req"]
    )
    .unwrap();
    pub static ref CLOUD_ERROR_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_cloud_error_count",
        "Total number of credentail errors from EKS env",
        &["cloud", "error"]
    )
    .unwrap();
}

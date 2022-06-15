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
}

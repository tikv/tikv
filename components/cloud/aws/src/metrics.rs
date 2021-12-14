// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref S3_REQUEST_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "s3_request_duration_seconds",
        "Bucketed histogram of s3 requests duration",
        &["type"]
    )
    .unwrap();
}

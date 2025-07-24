// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref RTS_TIKV_CLIENT_INIT_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_resolved_ts_tikv_client_init_duration_seconds",
        "Bucketed histogram of resolved-ts tikv client initializing duration",
        exponential_buckets(0.005, 2.0, 20).unwrap(),
    )
    .unwrap();
}
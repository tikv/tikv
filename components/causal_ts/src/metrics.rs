// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref TS_PROVIDER_TSO_BATCH_SIZE: IntGauge = register_int_gauge!(
        "tikv_causal_ts_provider_tso_batch_size",
        "TSO batch size of causal timestamp provider"
    )
    .unwrap();
    pub static ref TS_PROVIDER_GET_TS_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_causal_ts_provider_get_ts_duration_seconds",
        "Histogram of the duration of get_ts",
        &["result"],
        exponential_buckets(1e-7, 2.0, 20).unwrap() // 100ns ~ 100ms
    )
    .unwrap();
    pub static ref TS_PROVIDER_TSO_BATCH_RENEW_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_causal_ts_provider_tso_batch_renew_duration_seconds",
        "Histogram of the duration of TSO batch renew",
        &["result", "reason"],
        exponential_buckets(1e-6, 2.0, 20).unwrap() // 1us ~ 1s
    )
    .unwrap();
}

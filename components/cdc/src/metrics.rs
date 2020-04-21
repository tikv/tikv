// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref CDC_RESOLVED_TS_GAP_HISTOGRAM: Histogram = register_histogram!(
        "tikv_cdc_resolved_ts_gap_seconds",
        "Bucketed histogram of the gap between cdc resolved ts and current tso",
        exponential_buckets(0.001, 2.0, 24).unwrap()
    )
    .unwrap();
    pub static ref CDC_SCAN_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_cdc_scan_duration_seconds",
        "Bucketed histogram of cdc async scan duration",
        exponential_buckets(0.0001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref CDC_MIN_RESOLVED_TS_REGION: IntGauge = register_int_gauge!(
        "tikv_cdc_min_resolved_ts_region",
        "The region which has minimal resolved ts"
    )
    .unwrap();
    pub static ref CDC_MIN_RESOLVED_TS: IntGauge = register_int_gauge!(
        "tikv_cdc_min_resolved_ts",
        "The minimal resolved ts for current regions"
    )
    .unwrap();
    pub static ref CDC_PENDING_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_cdc_pending_bytes",
        "Bytes in memory of a pending region"
    )
    .unwrap();
    pub static ref CDC_CAPTURED_REGION_COUNT: IntGauge = register_int_gauge!(
        "tikv_cdc_captured_region_total",
        "Total number of CDC captured regions"
    )
    .unwrap();
}

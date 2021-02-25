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
        exponential_buckets(0.005, 2.0, 20).unwrap()
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
    pub static ref CDC_OLD_VALUE_CACHE_MISS: IntGauge = register_int_gauge!(
        "tikv_cdc_old_value_cache_miss",
        "Count of old value cache missing"
    )
    .unwrap();
    pub static ref CDC_OLD_VALUE_CACHE_ACCESS: IntGauge = register_int_gauge!(
        "tikv_cdc_old_value_cache_access",
        "Count of old value cache accessing"
    )
    .unwrap();
    pub static ref CDC_OLD_VALUE_CACHE_BYTES: IntGauge =
        register_int_gauge!("tikv_cdc_old_value_cache_bytes", "Bytes of old value cache").unwrap();
    pub static ref CDC_OLD_VALUE_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "tikv_cdc_old_value_scan_details",
        "Bucketed counter of scan details for old value",
        &["cf", "tag"]
    )
    .unwrap();
    pub static ref CDC_OLD_VALUE_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_cdc_old_value_duration",
        "Bucketed histogram of cdc old value scan duration",
        &["tag"],
        exponential_buckets(0.0001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref CDC_SCAN_BLOCK_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_scan_block_duration",
        "Bucketed histogram of cdc scan block duration",
        exponential_buckets(0.0001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref CDC_SINK_QUEUE_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_cdc_sink_queue_size",
        "Bucketed histogram of cdc sink queue size",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
}

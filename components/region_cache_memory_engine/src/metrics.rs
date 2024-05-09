// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum KeyCountType {
        total,
        filtered,
        below_safe_point_total,
        below_safe_point_unique,
    }

    pub struct GcFilteredCountVec: LocalIntCounter {
        "type" => KeyCountType,
    }
}

lazy_static! {
    pub static ref GC_FILTERED: IntCounterVec = register_int_counter_vec!(
        "tikv_range_cache_memory_engine_gc_filtered",
        "Filtered version by GC",
        &["type"]
    )
    .unwrap();
    pub static ref RANGE_CACHE_MEMORY_USAGE: IntGauge = register_int_gauge!(
        "tikv_range_cache_memory_usage_bytes",
        "The memory usage of the range cache engine",
    )
    .unwrap();
    pub static ref RANGE_LOAD_TIME_HISTOGRAM: Histogram = register_histogram!(
        "tikv_range_load_duration_secs",
        "Bucketed histogram of range load time duration.",
        exponential_buckets(0.001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RANGE_GC_TIME_HISTOGRAM: Histogram = register_histogram!(
        "tikv_range_gc_duration_secs",
        "Bucketed histogram of range gc time duration.",
        exponential_buckets(0.001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref WRITE_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_range_cache_engine_write_duration_seconds",
        "Bucketed histogram of write duration in range cache engine.",
        exponential_buckets(0.00001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RANGE_CACHE_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "tikv_range_cache_count",
        "The range counts of each type.",
        &["type"]
    )
    .unwrap();
}

lazy_static! {
    pub static ref GC_FILTERED_STATIC: GcFilteredCountVec =
        auto_flush_from!(GC_FILTERED, GcFilteredCountVec);
}

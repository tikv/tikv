// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::{register_gauge_vec, register_int_counter_vec, IntCounterVec, IntGauge};
use prometheus_static_metric::{auto_flush_from, make_auto_flush_static_metric};

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
    pub static ref RANGE_CACHE_MEMORY_USAGE: IntGauge = register_gauge_vec!(
        "tikv_range_cache_memory_usage_bytes",
        "The memory usage of the range cache engine",
    )
    .unwrap();
}

lazy_static! {
    pub static ref GC_FILTERED_STATIC: GcFilteredCountVec =
        auto_flush_from!(GC_FILTERED, GcFilteredCountVec);
}

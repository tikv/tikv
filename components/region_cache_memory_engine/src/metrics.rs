// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

use crate::{
    statistics::{Tickers, ENGINE_TICKER_TYPES},
    RangeCacheMemoryEngineStatistics,
};

make_auto_flush_static_metric! {
    pub label_enum KeyCountType {
        total,
        filtered,
        below_safe_point_total,
        below_safe_point_unique,
    }

    pub label_enum TickerEnum {
        bytes_read,
        iter_bytes_read,
        number_db_seek,
        number_db_seek_found,
        number_db_next,
        number_db_next_found,
        number_db_prev,
        number_db_prev_found,
    }

    pub struct GcFilteredCountVec: LocalIntCounter {
        "type" => KeyCountType,
    }

    pub struct InMemoryEngineTickerMetrics: LocalIntCounter {
        "type" => TickerEnum,
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
    pub static ref PREPARE_FOR_WRITE_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_range_cache_engine_prepare_for_write_duration_seconds",
        "Bucketed histogram of prepare for apply duration in range cache engine.",
        &["type"],
        exponential_buckets(0.00001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref RANGE_CACHE_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "tikv_range_cache_count",
        "The count of each type on range cache.",
        &["type"]
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_FLOW: IntCounterVec = register_int_counter_vec!(
        "tikv_range_cache_memory_engine_flow",
        "Bytes and keys of read/written of range cache memory engine",
        &["type"]
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_LOCATE: IntCounterVec = register_int_counter_vec!(
        "tikv_range_cache_memory_engine_locate",
        "Number of calls to seek/next/prev",
        &["type"]
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_SEEK_DURATION: Histogram = register_histogram!(
        "tikv_range_cache_memory_engine_seek_duration",
        "Histogram of seek duration",
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
}

lazy_static! {
    pub static ref GC_FILTERED_STATIC: GcFilteredCountVec =
        auto_flush_from!(GC_FILTERED, GcFilteredCountVec);
    pub static ref IN_MEMORY_ENGINE_FLOW_STATIC: InMemoryEngineTickerMetrics =
        auto_flush_from!(IN_MEMORY_ENGINE_FLOW, InMemoryEngineTickerMetrics);
    pub static ref IN_MEMORY_ENGINE_LOCATE_STATIC: InMemoryEngineTickerMetrics =
        auto_flush_from!(IN_MEMORY_ENGINE_LOCATE, InMemoryEngineTickerMetrics);
}

pub fn flush_range_cache_engine_statistics(statistics: &Arc<RangeCacheMemoryEngineStatistics>) {
    for t in ENGINE_TICKER_TYPES {
        let v = statistics.get_and_reset_ticker_count(*t);
        flush_engine_ticker_metrics(*t, v);
    }
}

fn flush_engine_ticker_metrics(t: Tickers, value: u64) {
    match t {
        Tickers::BytesRead => {
            IN_MEMORY_ENGINE_FLOW_STATIC.bytes_read.inc_by(value);
        }
        Tickers::IterBytesRead => {
            IN_MEMORY_ENGINE_FLOW_STATIC.iter_bytes_read.inc_by(value);
        }
        Tickers::NumberDbSeek => {
            IN_MEMORY_ENGINE_LOCATE_STATIC.number_db_seek.inc_by(value);
        }
        Tickers::NumberDbSeekFound => {
            IN_MEMORY_ENGINE_LOCATE_STATIC
                .number_db_seek_found
                .inc_by(value);
        }
        Tickers::NumberDbNext => {
            IN_MEMORY_ENGINE_LOCATE_STATIC.number_db_next.inc_by(value);
        }
        Tickers::NumberDbNextFound => {
            IN_MEMORY_ENGINE_LOCATE_STATIC
                .number_db_next_found
                .inc_by(value);
        }
        Tickers::NumberDbPrev => {
            IN_MEMORY_ENGINE_LOCATE_STATIC.number_db_prev.inc_by(value);
        }
        Tickers::NumberDbPrevFound => {
            IN_MEMORY_ENGINE_LOCATE_STATIC
                .number_db_prev_found
                .inc_by(value);
        }
        _ => {
            unreachable!()
        }
    }
}

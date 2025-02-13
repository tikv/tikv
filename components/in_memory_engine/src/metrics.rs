// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::EvictReason;
use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

use crate::{
    statistics::{Tickers, ENGINE_TICKER_TYPES},
    InMemoryEngineStatistics,
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

    pub label_enum EvictReasonType {
        prepare_merge,
        merge,
        auto_evict,
        load_failed,
        load_failed_without_start,
        delete_range,
        become_follower,
        memory_limit_reached,
        disabled,
        apply_snapshot,
        flashback,
        manual,
        destroy_peer,
        ingest_sst,
    }

    pub label_enum OperationType {
        put,
        delete,
    }

    pub label_enum CF {
        default,
        lock,
        write,
    }

    pub struct GcFilteredCountVec: LocalIntCounter {
        "type" => KeyCountType,
    }

    pub struct InMemoryEngineTickerMetrics: LocalIntCounter {
        "type" => TickerEnum,
    }

    pub struct EvictionDurationVec: LocalHistogram {
        "type" => EvictReasonType,
    }

    pub struct OperationTypeForCF: LocalIntCounter {
        "type" => OperationType,
        "cf" => CF,
    }
}

lazy_static! {
    pub static ref GC_FILTERED: IntCounterVec = register_int_counter_vec!(
        "tikv_in_memory_engine_gc_filtered",
        "Filtered version by GC",
        &["type"]
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_MEMORY_USAGE: IntGauge = register_int_gauge!(
        "tikv_in_memory_engine_memory_usage_bytes",
        "The memory usage of the region cache engine",
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_LOAD_TIME_HISTOGRAM: Histogram = register_histogram!(
        "tikv_in_memory_engine_load_duration_secs",
        "Bucketed histogram of region load time duration.",
        exponential_buckets(0.001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_GC_TIME_HISTOGRAM: Histogram = register_histogram!(
        "tikv_in_memory_engine_gc_duration_secs",
        "Bucketed histogram of region gc time duration.",
        exponential_buckets(0.001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM: HistogramVec =
        register_histogram_vec!(
            "tikv_in_memory_engine_eviction_duration_secs",
            "Bucketed histogram of region eviction time duration.",
            &["type"],
            exponential_buckets(0.001, 2.0, 20).unwrap()
        )
        .unwrap();
    pub static ref IN_MEMORY_ENGINE_WRITE_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "tikv_in_memory_engine_write_duration_seconds",
        "Bucketed histogram of write duration in region cache engine.",
        exponential_buckets(0.00001, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_PREPARE_FOR_WRITE_DURATION_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_in_memory_engine_prepare_for_write_duration_seconds",
            "Bucketed histogram of prepare for write duration in region cache engine.",
            exponential_buckets(0.00001, 2.0, 20).unwrap()
        )
        .unwrap();
    pub static ref IN_MEMORY_ENGINE_CACHE_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "tikv_in_memory_engine_cache_count",
        "The count of each type on region cache.",
        &["type"]
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_FLOW: IntCounterVec = register_int_counter_vec!(
        "tikv_in_memory_engine_flow",
        "Bytes and keys of read/written of in-memory engine",
        &["type"]
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_LOCATE: IntCounterVec = register_int_counter_vec!(
        "tikv_in_memory_engine_locate",
        "Number of calls to seek/next/prev",
        &["type"]
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_SEEK_DURATION: Histogram = register_histogram!(
        "tikv_in_memory_engine_seek_duration",
        "Histogram of seek duration",
        exponential_buckets(0.00001, 2.0, 26).unwrap()
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_KV_OPERATIONS: IntCounterVec = register_int_counter_vec!(
        "tikv_in_memory_engine_kv_operations",
        "Number of kv operations",
        &["type", "cf"]
    ).unwrap();
    pub static ref IN_MEMORY_ENGINE_OLDEST_SAFE_POINT: IntGauge = register_int_gauge!(
        "tikv_in_memory_engine_oldest_safe_point",
        "The oldest safe point in the in-memory engine",
    )
    .unwrap();
    pub static ref IN_MEMORY_ENGINE_NEWEST_SAFE_POINT: IntGauge = register_int_gauge!(
        "tikv_in_memory_engine_newest_safe_point",
        "The newest safe point in the in-memory engine",
    )
    .unwrap();
    pub static ref SAFE_POINT_GAP: IntGauge = register_int_gauge!(
        "tikv_safe_point_gap_with_in_memory_engine",
        "The gap between tikv auto gc safe point and the oldest auto gc safe point in the in-memory engine",
    )
    .unwrap();
}

lazy_static! {
    pub static ref IN_MEMORY_ENGINE_GC_FILTERED_STATIC: GcFilteredCountVec =
        auto_flush_from!(GC_FILTERED, GcFilteredCountVec);
    pub static ref IN_MEMORY_ENGINE_FLOW_STATIC: InMemoryEngineTickerMetrics =
        auto_flush_from!(IN_MEMORY_ENGINE_FLOW, InMemoryEngineTickerMetrics);
    pub static ref IN_MEMORY_ENGINE_LOCATE_STATIC: InMemoryEngineTickerMetrics =
        auto_flush_from!(IN_MEMORY_ENGINE_LOCATE, InMemoryEngineTickerMetrics);
    pub static ref IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC: EvictionDurationVec = auto_flush_from!(
        IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM,
        EvictionDurationVec
    );
    pub static ref IN_MEMORY_ENGINE_OPERATION_STATIC: OperationTypeForCF =
        auto_flush_from!(IN_MEMORY_ENGINE_KV_OPERATIONS, OperationTypeForCF);
}

pub fn flush_in_memory_engine_statistics(statistics: &Arc<InMemoryEngineStatistics>) {
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

pub(crate) fn observe_eviction_duration(secs: f64, evict_reason: EvictReason) {
    match evict_reason {
        EvictReason::AutoEvict => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .auto_evict
            .observe(secs),
        EvictReason::BecomeFollower => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .become_follower
            .observe(secs),
        EvictReason::DeleteRange => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .delete_range
            .observe(secs),
        EvictReason::LoadFailed => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .load_failed
            .observe(secs),
        EvictReason::LoadFailedWithoutStart => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .load_failed_without_start
            .observe(secs),
        EvictReason::MemoryLimitReached => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .memory_limit_reached
            .observe(secs),
        EvictReason::PrepareMerge => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .prepare_merge
            .observe(secs),
        EvictReason::Merge => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .merge
            .observe(secs),
        EvictReason::Disabled => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .disabled
            .observe(secs),
        EvictReason::ApplySnapshot => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .apply_snapshot
            .observe(secs),
        EvictReason::Flashback => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .flashback
            .observe(secs),
        EvictReason::Manual => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .manual
            .observe(secs),
        EvictReason::DestroyPeer => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .destroy_peer
            .observe(secs),
        EvictReason::IngestSST => IN_MEMORY_ENGINE_EVICTION_DURATION_HISTOGRAM_STATIC
            .ingest_sst
            .observe(secs),
    }
}

pub(crate) fn count_operations_for_cfs(put_operations: &[u64], delete_operations: &[u64]) {
    // according to `cf_to_id`, we have 0 for CF_DEFAULT, 1 for CF_LOCK, and 2 for
    // CF_WRITE
    assert_eq!(put_operations.len(), 3);
    assert_eq!(delete_operations.len(), 3);
    IN_MEMORY_ENGINE_OPERATION_STATIC
        .put
        .default
        .inc_by(put_operations[0]);
    IN_MEMORY_ENGINE_OPERATION_STATIC
        .put
        .lock
        .inc_by(put_operations[1]);
    IN_MEMORY_ENGINE_OPERATION_STATIC
        .put
        .write
        .inc_by(put_operations[2]);

    IN_MEMORY_ENGINE_OPERATION_STATIC
        .delete
        .default
        .inc_by(delete_operations[0]);
    IN_MEMORY_ENGINE_OPERATION_STATIC
        .delete
        .lock
        .inc_by(delete_operations[1]);
    IN_MEMORY_ENGINE_OPERATION_STATIC
        .delete
        .write
        .inc_by(delete_operations[2]);
}

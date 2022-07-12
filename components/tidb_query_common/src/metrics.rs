// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;
use lazy_static::lazy_static;

make_auto_flush_static_metric! {
    pub label_enum ExecutorName {
        batch_table_scan,
        batch_index_scan,
        batch_selection,
        batch_simple_aggr,
        batch_fast_hash_aggr,
        batch_slow_hash_aggr,
        batch_stream_aggr,
        batch_limit,
        batch_top_n,
        batch_projection,
        table_scan,
        index_scan,
        selection,
        hash_aggr,
        stream_aggr,
        top_n,
        limit,
    }

    pub struct LocalCoprExecutorCount: LocalIntCounter {
        "type" => ExecutorName,
    }
}

make_static_metric! {
    pub label_enum MemTrace {
        aggr_fast_hash,
        aggr_slow_hash,
        aggr_stream,
        aggr_simple,
        top_n,
        index_scan,
        table_scan,
        limit,
        projection,
        selection,
    }

    pub struct MemTraceGauge : IntGauge {
        "type" => MemTrace,
    }
}

lazy_static! {
    pub static ref MEMTRACE_QUERY_EXECUTOR: MemTraceGauge = register_static_int_gauge_vec!(
        MemTraceGauge,
        "tikv_query_executor_memory_usage",
        "Coprocessor query executor memory usage",
        &["type"]
    )
    .unwrap();

    static ref COPR_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_executor_count",
        "Total number of each executor",
        &["type"]
    )
    .unwrap();

    pub static ref EXECUTOR_COUNT_METRICS: LocalCoprExecutorCount =
        auto_flush_from!(COPR_EXECUTOR_COUNT, LocalCoprExecutorCount);
}

pub trait MemoryTrace {
    fn alloc_trace(&mut self, len: usize);
}


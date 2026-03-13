// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;
use tracker::with_tls_tracker;

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
        batch_index_lookup,
        batch_index_lookup_table_scan,
    }

    pub struct LocalCoprExecutorCount: LocalIntCounter {
        "type" => ExecutorName,
    }
}

lazy_static::lazy_static! {
    static ref COPR_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_executor_count",
        "Total number of each executor",
        &["type"]
    )
    .unwrap();
}

lazy_static::lazy_static! {
    pub static ref EXECUTOR_COUNT_METRICS: LocalCoprExecutorCount =
        auto_flush_from!(COPR_EXECUTOR_COUNT, LocalCoprExecutorCount);
}

#[inline]
pub fn record_executor_work(tp: ExecutorName, work: u64) {
    if work == 0 {
        return;
    }
    with_tls_tracker(|tracker| {
        debug_assert!(
            matches!(
                tp,
                ExecutorName::batch_index_scan
                    | ExecutorName::batch_table_scan
                    | ExecutorName::batch_selection
                    | ExecutorName::batch_top_n
                    | ExecutorName::batch_limit
                    | ExecutorName::batch_simple_aggr
                    | ExecutorName::batch_fast_hash_aggr
            ),
            "record_executor_work called with unsupported executor type",
        );
        match tp {
            ExecutorName::batch_index_scan => {
                tracker.metrics.executor_work_batch_index_scan = tracker
                    .metrics
                    .executor_work_batch_index_scan
                    .saturating_add(work);
            }
            ExecutorName::batch_table_scan => {
                tracker.metrics.executor_work_batch_table_scan = tracker
                    .metrics
                    .executor_work_batch_table_scan
                    .saturating_add(work);
            }
            ExecutorName::batch_selection => {
                tracker.metrics.executor_work_batch_selection = tracker
                    .metrics
                    .executor_work_batch_selection
                    .saturating_add(work);
            }
            ExecutorName::batch_top_n => {
                tracker.metrics.executor_work_batch_top_n = tracker
                    .metrics
                    .executor_work_batch_top_n
                    .saturating_add(work);
            }
            ExecutorName::batch_limit => {
                tracker.metrics.executor_work_batch_limit = tracker
                    .metrics
                    .executor_work_batch_limit
                    .saturating_add(work);
            }
            ExecutorName::batch_simple_aggr => {
                tracker.metrics.executor_work_batch_simple_aggr = tracker
                    .metrics
                    .executor_work_batch_simple_aggr
                    .saturating_add(work);
            }
            ExecutorName::batch_fast_hash_aggr => {
                tracker.metrics.executor_work_batch_fast_hash_aggr = tracker
                    .metrics
                    .executor_work_batch_fast_hash_aggr
                    .saturating_add(work);
            }
            _ => {}
        }
    });
}

#[inline]
pub fn record_coprocessor_executor_iterations(iters: u64) {
    if iters == 0 {
        return;
    }
    with_tls_tracker(|tracker| {
        tracker.metrics.coprocessor_executor_iterations = tracker
            .metrics
            .coprocessor_executor_iterations
            .saturating_add(iters);
    });
}

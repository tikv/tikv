// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::make_static_metric;

use tikv_util::metrics::{TLSMetricGroup, TLSMetricGroupInner};

make_static_metric! {
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

lazy_static::lazy_static! {
    static ref COPR_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_executor_count",
        "Total number of each executor",
        &["type"]
    )
    .unwrap();
}

pub struct RunnerBuildingMetrics {
    pub executor_count: LocalCoprExecutorCount,
}

impl RunnerBuildingMetrics {
    fn new() -> Self {
        Self {
            executor_count: LocalCoprExecutorCount::from(&COPR_EXECUTOR_COUNT),
        }
    }
}

impl TLSMetricGroupInner for RunnerBuildingMetrics {
    fn flush_all(&self) {
        self.executor_count.flush();
    }
}

thread_local! {
    pub static RUNNER_BUILDING_METRICS: TLSMetricGroup<RunnerBuildingMetrics> =
        TLSMetricGroup::new(RunnerBuildingMetrics::new());
}

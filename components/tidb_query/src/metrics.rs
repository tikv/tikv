// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

use tikv_util::metrics::TLSMetricGroup;

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

    pub label_enum AcquireSemaphoreType {
        acquired_generic,
        acquired_heavy,
        unacquired,
    }

    pub struct CoprAcquireSemaphoreTypeCounterVec: IntCounter {
        "type" => AcquireSemaphoreType,
    }
}

lazy_static::lazy_static! {
    static ref COPR_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_executor_count",
        "Total number of each executor",
        &["type"]
    )
    .unwrap();
    pub static ref COPR_ACQUIRE_SEMAPHORE_TYPE: CoprAcquireSemaphoreTypeCounterVec =
        register_static_int_counter_vec!(
            CoprAcquireSemaphoreTypeCounterVec,
            "tikv_coprocessor_acquire_semaphore_type",
            "The acquire type of the coprocessor semaphore",
            &["type"],
        )
        .unwrap();
}

thread_local! {
    pub static EXECUTOR_COUNT_METRICS: TLSMetricGroup<LocalCoprExecutorCount> =
        TLSMetricGroup::new(LocalCoprExecutorCount::from(&COPR_EXECUTOR_COUNT));
}

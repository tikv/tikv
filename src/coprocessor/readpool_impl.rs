// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;
use std::sync::{Arc, Mutex};

use crate::config::CoprocessorReadPoolConfig;
use crate::storage::kv::{destroy_tls_engine, set_tls_engine};
use crate::storage::{Engine, FlowStatistics, FlowStatsReporter, Statistics};
use tikv_util::collections::HashMap;
use tikv_util::future_pool::{Builder, Config, TaskLimitedFuturePool};

use super::metrics::*;
use prometheus::local::*;

pub struct CopLocalMetrics {
    pub local_copr_req_histogram_vec: LocalHistogramVec,
    pub local_copr_req_handle_time: LocalHistogramVec,
    pub local_copr_req_wait_time: LocalHistogramVec,
    pub local_copr_req_error: LocalIntCounterVec,
    pub local_copr_scan_keys: LocalHistogramVec,
    pub local_copr_scan_details: LocalIntCounterVec,
    pub local_copr_rocksdb_perf_counter: LocalIntCounterVec,
    local_cop_flow_stats: HashMap<u64, FlowStatistics>,
}

thread_local! {
    pub static TLS_COP_METRICS: RefCell<CopLocalMetrics> = RefCell::new(
        CopLocalMetrics {
            local_copr_req_histogram_vec:
                COPR_REQ_HISTOGRAM_VEC.local(),
            local_copr_req_handle_time:
                COPR_REQ_HANDLE_TIME.local(),
            local_copr_req_wait_time:
                COPR_REQ_WAIT_TIME.local(),
            local_copr_req_error:
                COPR_REQ_ERROR.local(),
            local_copr_scan_keys:
                COPR_SCAN_KEYS.local(),
            local_copr_scan_details:
                COPR_SCAN_DETAILS.local(),
            local_copr_rocksdb_perf_counter:
                COPR_ROCKSDB_PERF_COUNTER.local(),
            local_cop_flow_stats:
                HashMap::default(),
        }
    );
}

pub fn build_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &CoprocessorReadPoolConfig,
    reporter: R,
    engine: E,
) -> Vec<TaskLimitedFuturePool> {
    let names = vec!["cop-low", "cop-normal", "cop-high"];
    let configs: Vec<Config> = config.to_future_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let reporter = reporter.clone();
            let reporter2 = reporter.clone();
            let engine = Arc::new(Mutex::new(engine.clone()));
            Builder::from_config(config)
                .name_prefix(name)
                .on_tick(move || tls_flush(&reporter))
                .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                .before_stop(move || {
                    destroy_tls_engine::<E>();
                    tls_flush(&reporter2)
                })
                .build_with_task_limit()
        })
        .collect()
}

pub fn build_read_pool_for_test<E: Engine>(
    config: &CoprocessorReadPoolConfig,
    engine: E,
) -> Vec<TaskLimitedFuturePool> {
    let configs: Vec<Config> = config.to_future_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .map(|config| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            Builder::from_config(config)
                .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                .before_stop(|| destroy_tls_engine::<E>())
                .build_with_task_limit()
        })
        .collect()
}

#[inline]
fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_COP_METRICS.with(|m| {
        // Flush Prometheus metrics
        let mut cop_metrics = m.borrow_mut();
        cop_metrics.local_copr_req_histogram_vec.flush();
        cop_metrics.local_copr_req_handle_time.flush();
        cop_metrics.local_copr_req_wait_time.flush();
        cop_metrics.local_copr_scan_keys.flush();
        cop_metrics.local_copr_rocksdb_perf_counter.flush();
        cop_metrics.local_copr_scan_details.flush();

        // Report PD metrics
        if cop_metrics.local_cop_flow_stats.is_empty() {
            // Stats to report to PD is empty, ignore.
            return;
        }

        let mut read_stats = HashMap::default();
        mem::swap(&mut read_stats, &mut cop_metrics.local_cop_flow_stats);

        reporter.report_read_stats(read_stats);
    });
}

pub fn tls_collect_cf_stats(region_id: u64, type_str: &str, stats: &Statistics) {
    // cf statistics group by type
    for (cf, details) in stats.details() {
        for (tag, count) in details {
            TLS_COP_METRICS.with(|m| {
                m.borrow_mut()
                    .local_copr_scan_details
                    .with_label_values(&[type_str, cf, tag])
                    .inc_by(count as i64);
            });
        }
    }
    // flow statistics group by region
    tls_collect_read_flow(region_id, stats);
}

#[inline]
pub fn tls_collect_read_flow(region_id: u64, statistics: &crate::storage::Statistics) {
    TLS_COP_METRICS.with(|m| {
        let map = &mut m.borrow_mut().local_cop_flow_stats;
        let flow_stats = map
            .entry(region_id)
            .or_insert_with(crate::storage::FlowStatistics::default);
        flow_stats.add(&statistics.write.flow_stats);
        flow_stats.add(&statistics.data.flow_stats);
    });
}

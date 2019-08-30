// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;

use crate::config::CoprReadPoolConfig;
use crate::storage::kv::{destroy_tls_engine, set_tls_engine, NoopReporter};
use crate::storage::{Engine, FlowStatistics, FlowStatsReporter, Statistics};
use tikv_util::collections::HashMap;
use tikv_util::future_pool::{Builder, CloneFactory, FuturePool, TickRunner};

use super::metrics::*;
use prometheus::local::*;

pub struct CopLocalMetrics {
    pub local_copr_req_histogram_vec: LocalHistogramVec,
    pub local_copr_req_handle_time: LocalHistogramVec,
    pub local_copr_req_wait_time: LocalHistogramVec,
    pub local_copr_scan_keys: LocalHistogramVec,
    pub local_copr_rocksdb_perf_counter: LocalIntCounterVec,
    local_scan_details: HashMap<&'static str, Statistics>,
    local_cop_flow_stats: HashMap<u64, FlowStatistics>,
}

#[derive(Clone)]
pub struct MetricsFlusher<E, R> {
    reporter: R,
    e: E,
}

impl<E, R> MetricsFlusher<E, R> {
    pub fn new(r: R, e: E) -> MetricsFlusher<E, R> {
        MetricsFlusher { reporter: r, e }
    }
}

impl<E: Engine, R: FlowStatsReporter> TickRunner for MetricsFlusher<E, R> {
    fn start(&mut self) {
        set_tls_engine(self.e.clone());
    }

    fn on_tick(&mut self) {
        tls_flush(&self.reporter);
    }

    fn end(&mut self) {
        destroy_tls_engine::<E>();
        tls_flush(&self.reporter);
    }
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
            local_copr_scan_keys:
                COPR_SCAN_KEYS.local(),
            local_copr_rocksdb_perf_counter:
                COPR_ROCKSDB_PERF_COUNTER.local(),
            local_scan_details:
                HashMap::default(),
            local_cop_flow_stats:
                HashMap::default(),
        }
    );
}

pub fn build_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &CoprReadPoolConfig,
    reporter: R,
    engine: E,
) -> Vec<FuturePool> {
    ["low", "normal", "high"]
        .iter()
        .map(|p| {
            let name = format!("cop-{}", p);
            config
                .configure_builder(
                    p,
                    Builder::new(
                        name,
                        CloneFactory(MetricsFlusher {
                            reporter: reporter.clone(),
                            e: engine.clone(),
                        }),
                    ),
                )
                .build()
        })
        .collect()
}

pub fn build_read_pool_for_test<E: Engine>(engine: E) -> Vec<FuturePool> {
    build_read_pool(
        &CoprReadPoolConfig::default_for_test(),
        NoopReporter,
        engine,
    )
}

fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_COP_METRICS.with(|m| {
        // Flush Prometheus metrics
        let mut m = m.borrow_mut();
        m.local_copr_req_histogram_vec.flush();
        m.local_copr_req_handle_time.flush();
        m.local_copr_req_wait_time.flush();
        m.local_copr_scan_keys.flush();
        m.local_copr_rocksdb_perf_counter.flush();

        for (cmd, stat) in m.local_scan_details.drain() {
            for (cf, cf_details) in stat.details().iter() {
                for (tag, count) in cf_details.iter() {
                    COPR_SCAN_DETAILS
                        .with_label_values(&[cmd, *cf, *tag])
                        .inc_by(*count as i64);
                }
            }
        }

        // Report PD metrics
        if m.local_cop_flow_stats.is_empty() {
            // Stats to report to PD is empty, ignore.
            return;
        }

        let mut read_stats = HashMap::default();
        mem::swap(&mut read_stats, &mut m.local_cop_flow_stats);

        reporter.report_read_stats(read_stats);
    });
}

pub fn tls_collect_scan_details(cmd: &'static str, stats: &Statistics) {
    TLS_COP_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_collect_read_flow(region_id: u64, statistics: &Statistics) {
    TLS_COP_METRICS.with(|m| {
        let map = &mut m.borrow_mut().local_cop_flow_stats;
        let flow_stats = map
            .entry(region_id)
            .or_insert_with(crate::storage::FlowStatistics::default);
        flow_stats.add(&statistics.write.flow_stats);
        flow_stats.add(&statistics.data.flow_stats);
    });
}

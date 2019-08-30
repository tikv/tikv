// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;
use std::time::Duration;

use prometheus::local::*;

use crate::config::StorageReadPoolConfig;
use crate::storage::kv::{destroy_tls_engine, set_tls_engine, NoopReporter};
use crate::storage::{FlowStatistics, FlowStatsReporter, Statistics};
use tikv_util::collections::HashMap;
use tikv_util::future_pool::{Builder, CloneFactory, FuturePool, TickRunner};

use super::metrics::*;
use super::Engine;

pub struct StorageLocalMetrics {
    local_sched_histogram_vec: LocalHistogramVec,
    local_sched_processing_read_histogram_vec: LocalHistogramVec,
    local_kv_command_keyread_histogram_vec: LocalHistogramVec,
    local_kv_command_counter_vec: LocalIntCounterVec,
    local_sched_commands_pri_counter_vec: LocalIntCounterVec,
    local_scan_details: HashMap<&'static str, Statistics>,
    local_read_flow_stats: HashMap<u64, FlowStatistics>,
}

thread_local! {
    static TLS_STORAGE_METRICS: RefCell<StorageLocalMetrics> = RefCell::new(
        StorageLocalMetrics {
            local_sched_histogram_vec: SCHED_HISTOGRAM_VEC.local(),
            local_sched_processing_read_histogram_vec: SCHED_PROCESSING_READ_HISTOGRAM_VEC.local(),
            local_kv_command_keyread_histogram_vec: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
            local_kv_command_counter_vec: KV_COMMAND_COUNTER_VEC.local(),
            local_sched_commands_pri_counter_vec: SCHED_COMMANDS_PRI_COUNTER_VEC.local(),
            local_scan_details: HashMap::default(),
            local_read_flow_stats: HashMap::default(),
        }
    );
}

#[derive(Clone)]
pub struct MetricsFlusher<E, R> {
    reporter: R,
    e: E,
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

pub fn build_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &StorageReadPoolConfig,
    reporter: R,
    engine: E,
) -> Vec<FuturePool> {
    ["low", "normal", "high"]
        .iter()
        .map(|p| {
            let name = format!("store-read-{}", p);
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
        &StorageReadPoolConfig::default_for_test(),
        NoopReporter,
        engine,
    )
}

fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        // Flush Prometheus metrics
        m.local_sched_histogram_vec.flush();
        m.local_sched_processing_read_histogram_vec.flush();
        m.local_kv_command_keyread_histogram_vec.flush();
        m.local_kv_command_counter_vec.flush();
        m.local_sched_commands_pri_counter_vec.flush();

        for (cmd, stat) in m.local_scan_details.drain() {
            for (cf, cf_details) in stat.details().iter() {
                for (tag, count) in cf_details.iter() {
                    KV_COMMAND_SCAN_DETAILS
                        .with_label_values(&[cmd, *cf, *tag])
                        .inc_by(*count as i64);
                }
            }
        }

        // Report PD metrics
        if m.local_read_flow_stats.is_empty() {
            // Stats to report to PD is empty, ignore.
            return;
        }

        let mut read_stats = HashMap::default();
        mem::swap(&mut read_stats, &mut m.local_read_flow_stats);

        reporter.report_read_stats(read_stats);
    });
}

pub fn tls_collect_command_count(cmd: &str, priority: CommandPriority) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut storage_metrics = m.borrow_mut();
        storage_metrics
            .local_kv_command_counter_vec
            .with_label_values(&[cmd])
            .inc();
        storage_metrics
            .local_sched_commands_pri_counter_vec
            .with_label_values(&[priority.get_str()])
            .inc();
    });
}

pub fn tls_collect_command_duration(cmd: &str, duration: Duration) {
    TLS_STORAGE_METRICS.with(|m| {
        m.borrow_mut()
            .local_sched_histogram_vec
            .with_label_values(&[cmd])
            .observe(tikv_util::time::duration_to_sec(duration))
    });
}

pub fn tls_collect_key_reads(cmd: &str, count: usize) {
    TLS_STORAGE_METRICS.with(|m| {
        m.borrow_mut()
            .local_kv_command_keyread_histogram_vec
            .with_label_values(&[cmd])
            .observe(count as f64)
    });
}

pub fn tls_processing_read_observe_duration<F, R>(cmd: &str, f: F) -> R
where
    F: FnOnce() -> R,
{
    TLS_STORAGE_METRICS.with(|m| {
        let now = tikv_util::time::Instant::now_coarse();
        let ret = f();
        m.borrow_mut()
            .local_sched_processing_read_histogram_vec
            .with_label_values(&[cmd])
            .observe(now.elapsed_secs());
        ret
    })
}

pub fn tls_collect_scan_details(cmd: &'static str, stats: &Statistics) {
    TLS_STORAGE_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_collect_read_flow(region_id: u64, statistics: &Statistics) {
    TLS_STORAGE_METRICS.with(|m| {
        let map = &mut m.borrow_mut().local_read_flow_stats;
        let flow_stats = map
            .entry(region_id)
            .or_insert_with(crate::storage::FlowStatistics::default);
        flow_stats.add(&statistics.write.flow_stats);
        flow_stats.add(&statistics.data.flow_stats);
    });
}

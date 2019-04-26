// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;

use prometheus::local::*;
use tikv_util::collections::HashMap;
use tikv_util::future_pool::Builder as FuturePoolBuilder;
use tikv_util::future_pool::FuturePool;

use crate::storage::metrics::*;
use crate::storage::{Engine, Statistics, StatisticsSummary};

pub struct SchedLocalMetrics {
    stats: HashMap<&'static str, StatisticsSummary>,
    processing_read_duration: LocalHistogramVec,
    processing_write_duration: LocalHistogramVec,
    command_keyread_duration: LocalHistogramVec,
}

thread_local! {
     static TLS_SCHED_METRICS: RefCell<SchedLocalMetrics> = RefCell::new(
        SchedLocalMetrics {
            stats: HashMap::default(),
            processing_read_duration: SCHED_PROCESSING_READ_HISTOGRAM_VEC.local(),
            processing_write_duration: SCHED_PROCESSING_WRITE_HISTOGRAM_VEC.local(),
            command_keyread_duration: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
        }
    );
}

#[derive(Clone)]
pub struct SchedPool<E: Engine> {
    pub engine: E,
    pub pool: FuturePool,
}

impl<E: Engine> SchedPool<E> {
    pub fn new(engine: E, pool_size: usize, name_prefix: &str) -> Self {
        let pool = FuturePoolBuilder::new()
            .pool_size(pool_size)
            .name_prefix(name_prefix)
            .on_tick(move || tls_flush())
            .before_stop(move || tls_flush())
            .build();
        SchedPool { engine, pool }
    }
}

pub fn build_sched_pool(pool_size: usize, name_prefix: &str) -> FuturePool {
    FuturePoolBuilder::new()
        .pool_size(pool_size)
        .name_prefix(name_prefix)
        .on_tick(move || tls_flush())
        .before_stop(move || tls_flush())
        .build()
}

#[inline]
pub fn tls_add_statistics(cmd: &'static str, stat: &Statistics) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .stats
            .entry(cmd)
            .or_insert_with(Default::default)
            .add_statistics(stat);
    });
}

#[inline]
pub fn tls_flush() {
    TLS_SCHED_METRICS.with(|m| {
        let mut sched_metrics = m.borrow_mut();
        for (cmd, stat) in sched_metrics.stats.drain() {
            for (cf, details) in stat.stat.details() {
                for (tag, count) in details {
                    KV_COMMAND_SCAN_DETAILS
                        .with_label_values(&[cmd, cf, tag])
                        .inc_by(count as i64);
                }
            }
        }
        sched_metrics.processing_read_duration.flush();
        sched_metrics.processing_write_duration.flush();
        sched_metrics.command_keyread_duration.flush();
    });
}

#[inline]
pub fn tls_start_read_duration(cmd: &str) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .processing_read_duration
            .with_label_values(&[cmd])
            .start_coarse_timer();
    });
}

#[inline]
pub fn tls_collect_key_read_duration(cmd: &str, count: f64) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .command_keyread_duration
            .with_label_values(&[cmd])
            .observe(count);
    });
}

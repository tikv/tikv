// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use tikv_util::time::Duration;

use collections::HashMap;
use file_system::{set_io_type, IOType};
use prometheus::local::*;
use tikv_util::yatp_pool::{FuturePool, PoolTicker, YatpPoolBuilder};

use crate::storage::kv::{destroy_tls_engine, set_tls_engine, Engine, Statistics};
use crate::storage::metrics::*;

pub struct SchedLocalMetrics {
    local_scan_details: HashMap<&'static str, Statistics>,
    processing_read_duration: LocalHistogramVec,
    processing_write_duration: LocalHistogramVec,
    command_keyread_histogram_vec: LocalHistogramVec,
}

thread_local! {
     static TLS_SCHED_METRICS: RefCell<SchedLocalMetrics> = RefCell::new(
        SchedLocalMetrics {
            local_scan_details: HashMap::default(),
            processing_read_duration: SCHED_PROCESSING_READ_HISTOGRAM_VEC.local(),
            processing_write_duration: SCHED_PROCESSING_WRITE_HISTOGRAM_VEC.local(),
            command_keyread_histogram_vec: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
        }
    );
}

#[derive(Clone)]
pub struct SchedPool {
    pub pool: FuturePool,
}

#[derive(Clone)]
pub struct SchedTicker;

impl PoolTicker for SchedTicker {
    fn on_tick(&mut self) {
        tls_flush();
    }
}

impl SchedPool {
    pub fn new<E: Engine>(engine: E, pool_size: usize, name_prefix: &str) -> Self {
        let engine = Arc::new(Mutex::new(engine));
        let pool = YatpPoolBuilder::new(SchedTicker {})
            .thread_count(pool_size, pool_size)
            .name_prefix(name_prefix)
            // Safety: by setting `after_start` and `before_stop`, `FuturePool` ensures
            // the tls_engine invariants.
            .after_start(move || {
                set_tls_engine(engine.lock().unwrap().clone());
                set_io_type(IOType::ForegroundWrite);
            })
            .before_stop(move || unsafe {
                // Safety: we ensure the `set_` and `destroy_` calls use the same engine type.
                destroy_tls_engine::<E>();
                tls_flush();
            })
            .build_future_pool();
        SchedPool { pool }
    }
}

pub fn tls_collect_scan_details(cmd: &'static str, stats: &Statistics) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_flush() {
    TLS_SCHED_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        for (cmd, stat) in m.local_scan_details.drain() {
            for (cf, cf_details) in stat.details().iter() {
                for (tag, count) in cf_details.iter() {
                    KV_COMMAND_SCAN_DETAILS
                        .with_label_values(&[cmd, *cf, *tag])
                        .inc_by(*count as u64);
                }
            }
        }
        m.processing_read_duration.flush();
        m.processing_write_duration.flush();
        m.command_keyread_histogram_vec.flush();
    });
}

pub fn tls_collect_read_duration(cmd: &str, duration: Duration) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .processing_read_duration
            .with_label_values(&[cmd])
            .observe(tikv_util::time::duration_to_sec(duration))
    });
}

pub fn tls_collect_keyread_histogram_vec(cmd: &str, count: f64) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .command_keyread_histogram_vec
            .with_label_values(&[cmd])
            .observe(count);
    });
}

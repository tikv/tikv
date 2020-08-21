// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::config::UnifiedReadPoolConfig;
use crate::storage::kv::{
    destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter, Statistics,
};
use crate::storage::metrics::*;
use prometheus::local::*;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tikv_util::collections::HashMap;
use tikv_util::read_pool::{PoolTicker, ReadPoolBuilder};
pub use tikv_util::read_pool::{ReadPool, ReadPoolError, ReadPoolHandle};
use tikv_util::time::Instant;

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

pub fn tls_collect_scan_details(cmd: &'static str, stats: &Statistics) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
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

#[cfg(test)]
pub fn get_unified_read_pool_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "unified-read-pool-test-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(not(test))]
pub fn get_unified_read_pool_name() -> String {
    "unified-read-pool".to_string()
}

pub struct ReporterTicker<R>
where
    R: FlowStatsReporter,
{
    reporter: R,
    last_tick_time: Instant,
    tick_count: usize,
}

impl<R> Clone for ReporterTicker<R>
where
    R: FlowStatsReporter,
{
    fn clone(&self) -> Self {
        Self {
            reporter: self.reporter.clone(),
            last_tick_time: self.last_tick_time,
            tick_count: 0,
        }
    }
}

impl<R> PoolTicker for ReporterTicker<R>
where
    R: FlowStatsReporter,
{
    fn on_tick(&mut self) {
        self.flush_metrics_on_tick();
    }
}

impl<R> ReporterTicker<R>
where
    R: FlowStatsReporter,
{
    pub fn new(reporter: R) -> Self {
        Self {
            reporter,
            last_tick_time: Instant::now(),
            tick_count: 0,
        }
    }

    fn flush_metrics_on_tick(&mut self) {
        const TICK_INTERVAL: Duration = Duration::from_secs(1);
        const TICK_COUNT_LIMIT: usize = 10;
        // Do nothing if no tick passed
        self.tick_count += 1;
        if self.tick_count < TICK_COUNT_LIMIT {
            return;
        }
        self.tick_count = 0;
        if self.last_tick_time.elapsed() < TICK_INTERVAL {
            return;
        }
        self.last_tick_time = Instant::now();
        crate::storage::metrics::tls_flush(&self.reporter);
        crate::coprocessor::metrics::tls_flush(&self.reporter);
        TLS_SCHED_METRICS.with(|m| {
            let mut m = m.borrow_mut();
            for (cmd, stat) in m.local_scan_details.drain() {
                for (cf, cf_details) in stat.details().iter() {
                    for (tag, count) in cf_details.iter() {
                        KV_COMMAND_SCAN_DETAILS
                            .with_label_values(&[cmd, *cf, *tag])
                            .inc_by(*count as i64);
                    }
                }
            }
            m.processing_read_duration.flush();
            m.processing_write_duration.flush();
            m.command_keyread_histogram_vec.flush();
        });
    }
}

pub fn build_yatp_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
) -> ReadPool {
    let unified_read_pool_name = get_unified_read_pool_name();
    let mut builder = ReadPoolBuilder::new(ReporterTicker::new(reporter));
    let raftkv = Arc::new(Mutex::new(engine));
    let pool = builder
        .name_prefix(unified_read_pool_name.clone())
        .stack_size(config.stack_size.0 as usize)
        .thread_count(config.min_thread_count, config.max_thread_count)
        .after_start(move || {
            let engine = raftkv.lock().unwrap().clone();
            set_tls_engine(engine);
        })
        .before_stop(|| unsafe {
            destroy_tls_engine::<E>();
        })
        .build_multi_level_pool();
    ReadPool::Yatp {
        pool,
        running_tasks: metrics::UNIFIED_READ_POOL_RUNNING_TASKS
            .with_label_values(&[&unified_read_pool_name]),
        max_tasks: config
            .max_tasks_per_worker
            .saturating_mul(config.max_thread_count),
    }
}

mod metrics {
    use prometheus::*;

    lazy_static! {
        pub static ref UNIFIED_READ_POOL_RUNNING_TASKS: IntGaugeVec = register_int_gauge_vec!(
            "tikv_unified_read_pool_running_tasks",
            "The number of running tasks in the unified read pool",
            &["name"]
        )
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::{destroy_tls_engine, set_tls_engine};
    use crate::storage::{RocksEngine as RocksKV, TestEngineBuilder};
    use futures03::channel::oneshot;
    use kvproto::kvrpcpb::CommandPri;
    use raftstore::store::ReadStats;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use tikv_util::read_pool::{ReadPoolBuilder, ReadPoolError};

    #[derive(Clone)]
    struct DummyReporter;

    impl FlowStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
    }

    #[test]
    fn test_yatp_full() {
        // max running tasks number should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let ticker = ReporterTicker::new(DummyReporter {});
        let kv = Arc::new(Mutex::new(engine));
        let pool = ReadPoolBuilder::new(ticker)
            .after_start(move || {
                let engine = kv.lock().unwrap().clone();
                set_tls_engine(engine);
            })
            .before_stop(|| unsafe {
                destroy_tls_engine::<RocksKV>();
            })
            .max_tasks(2)
            .thread_count(1, 1)
            .build();

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let handle = pool.handle();
        let (task1, tx1) = gen_task();
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();

        assert!(handle.spawn(task1, CommandPri::Normal, 1).is_ok());
        assert!(handle.spawn(task2, CommandPri::Normal, 2).is_ok());

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task3, CommandPri::Normal, 3) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
        tx1.send(()).unwrap();

        thread::sleep(Duration::from_millis(300));
        assert!(handle.spawn(task4, CommandPri::Normal, 4).is_ok());
    }
}

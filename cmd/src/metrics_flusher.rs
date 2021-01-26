// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{Engines, KvEngine, RaftEngine};
use file_system::{BytesFetcher, MetricsTask as IOMetricsTask};
use tikv_util::{
    time::Instant,
    worker::{Runnable, RunnableWithTimer},
};

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_ENGINE_METRICS_RESET_INTERVAL: Duration = Duration::from_millis(60_000);

pub struct MetricsFlusher<K: KvEngine, R: RaftEngine> {
    engines: Engines<K, R>,
    engine_metrics_last_reset: Instant,
    io_task: IOMetricsTask,
}

impl<K: KvEngine, R: RaftEngine> MetricsFlusher<K, R> {
    pub fn new(engines: Engines<K, R>, fetcher: BytesFetcher) -> Self {
        MetricsFlusher {
            engines,
            engine_metrics_last_reset: Instant::now(),
            io_task: IOMetricsTask::new(fetcher),
        }
    }
}

pub struct DummyTask;

impl std::fmt::Display for DummyTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dummy task")
    }
}

impl<K: KvEngine, R: RaftEngine> Runnable for MetricsFlusher<K, R> {
    type Task = DummyTask;
}

impl<K: KvEngine, R: RaftEngine> RunnableWithTimer for MetricsFlusher<K, R> {
    fn on_timeout(&mut self) {
        // flush engines metrics
        self.engines.kv.flush_metrics("kv");
        self.engines.raft.flush_metrics("raft");
        if self.engine_metrics_last_reset.elapsed() >= DEFAULT_ENGINE_METRICS_RESET_INTERVAL {
            self.engines.kv.reset_statistics();
            self.engines.raft.reset_statistics();
            self.engine_metrics_last_reset = Instant::now();
        }
        // flush IO metrics
        self.io_task.on_tick();
    }
    fn get_interval(&self) -> Duration {
        DEFAULT_METRICS_FLUSH_INTERVAL
    }
}

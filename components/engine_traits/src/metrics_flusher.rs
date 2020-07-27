// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::{Duration, Instant};
use tikv_util::worker::{Runnable, Scheduler};

use crate::*;

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const FLUSHER_RESET_INTERVAL: Duration = Duration::from_millis(60_000);

pub struct MetricsFlusher<K: KvEngine, R: KvEngine> {
    pub engines: KvEngines<K, R>,
    interval: Duration,
    last_reset: Instant,
    scheduler: Option<Scheduler<usize>>,
}

impl<K: KvEngine, R: KvEngine> Runnable<usize> for MetricsFlusher<K, R> {
    fn register_scheduler(&mut self, scheduler: Scheduler<usize>) {
        scheduler.register_timeout(0, self.interval);
        self.scheduler = Some(scheduler);
    }

    fn run(&mut self, _: usize) {
        self.flush();
        self.scheduler
            .as_ref()
            .unwrap()
            .register_timeout(0, self.interval);
    }
}

impl<K: KvEngine, R: KvEngine> MetricsFlusher<K, R> {
    pub fn new(engines: KvEngines<K, R>) -> Self {
        MetricsFlusher {
            engines,
            interval: DEFAULT_FLUSH_INTERVAL,
            last_reset: Instant::now(),
            scheduler: None,
        }
    }

    pub fn set_flush_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    fn flush(&mut self) {
        self.engines
            .kv
            .flush_metrics("kv", self.engines.shared_block_cache);
        self.engines
            .raft
            .flush_metrics("raft", self.engines.shared_block_cache);
        if self.last_reset.elapsed() >= FLUSHER_RESET_INTERVAL {
            self.engines.kv.reset_statistics();
            self.engines.raft.reset_statistics();
            self.last_reset = Instant::now();
        }
    }
}

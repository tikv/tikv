// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::{Duration, Instant};

use tikv_util::IntervalRunnable;

use crate::engine::KvEngine;
use crate::engines::Engines;
use crate::raft_engine::RaftEngine;

const FLUSHER_RESET_INTERVAL: Duration = Duration::from_millis(60_000);

pub struct MetricsTask<K: KvEngine, R: RaftEngine> {
    pub engines: Engines<K, R>,
    last_reset: Instant,
}

impl<K: KvEngine, R: RaftEngine> MetricsTask<K, R> {
    pub fn new(engines: Engines<K, R>) -> Self {
        MetricsTask {
            engines,
            last_reset: Instant::now(),
        }
    }
}

impl<K: KvEngine, R: RaftEngine> IntervalRunnable for MetricsTask<K, R> {
    fn on_tick(&mut self) {
        self.engines.kv.flush_metrics("kv");
        self.engines.raft.flush_metrics("raft");
        if self.last_reset.elapsed() >= FLUSHER_RESET_INTERVAL {
            self.engines.kv.reset_statistics();
            self.engines.raft.reset_statistics();
            self.last_reset = Instant::now();
        }
    }
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod builder;
mod metrics;
mod park;
mod scheduler;
mod stats;
mod task;
mod worker;

use scheduler::Scheduler;
use stats::{StatsMap, TaskStats};
use task::{ArcTask, Task};

use crossbeam::deque::{Injector, Steal, Stealer, Worker as LocalQueue};
use crossbeam::queue::ArrayQueue;
use prometheus::{IntCounter, IntGauge};
use rand::prelude::*;
use tikv_util::time::Instant;

use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub use builder::Builder;

const TICK_INTERVAL: Duration = Duration::from_secs(1);

thread_local! {
    static THREAD_LAST_TICK_TIME: Cell<Instant> = Cell::new(Instant::now_coarse());
}

struct Env {
    on_tick: Option<Box<dyn Fn() + Send + Sync>>,
    metrics_running_task_count: IntGauge,
    metrics_handled_task_count: IntCounter,
}

#[derive(Clone)]
pub struct MultilevelPool {
    scheduler: Scheduler,
    stats_map: StatsMap,
    env: Arc<Env>,
    pool_size: usize,
    max_tasks: usize,
}

impl MultilevelPool {
    pub fn spawn<F>(&self, task: F, token: u64, nice: u8)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // at begin a token has top priority
        let stats = self.stats_map.get_stats(token);
        self.scheduler
            .add_task(ArcTask::new(task, self.scheduler.clone(), stats.clone()));
    }
}

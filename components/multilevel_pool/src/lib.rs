// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod builder;
mod metrics;
mod park;
mod stats;
mod task;
mod worker;

use park::Unparker;
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
    injectors: Arc<[Injector<ArcTask>]>,
    stats: StatsMap,
    sleepers: Arc<ArrayQueue<Unparker>>,
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
        let stat = self.stats.get_stats(token);
        let injector = match stat.elapsed.load(Ordering::SeqCst) {
            0..=999 => &self.injectors[0],
            1000..=299_999 => &self.injectors[1],
            _ => &self.injectors[2],
        };
        injector.push(ArcTask::new(
            task,
            self.injectors.clone(),
            self.sleepers.clone(),
            stat.clone(),
            nice,
            token,
        ));
        if let Ok(unparker) = self.sleepers.pop() {
            unparker.unpark();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

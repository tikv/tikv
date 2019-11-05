// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod builder;
mod metrics;
mod park;
mod scheduler;
mod stats;
mod task;
mod worker;

use scheduler::Scheduler;
use stats::StatsMap;
use task::ArcTask;

use futures::channel::oneshot;
use futures::FutureExt;
use prometheus::{IntCounter, IntGauge};
use tikv_util::time::Instant;

use std::cell::Cell;
use std::future::Future;
use std::sync::Arc;
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
    pub fn spawn<F>(&self, task: F, token: u64)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let env = self.env.clone();
        env.metrics_running_task_count.inc();
        let wrapped_task = async move {
            task.await;
            env.metrics_handled_task_count.inc();
            env.metrics_running_task_count.dec();
            try_tick_thread(&env);
        };
        // at begin a token has top priority
        let stats = self.stats_map.get_stats(token);
        self.scheduler.add_task(ArcTask::new(
            wrapped_task,
            self.scheduler.clone(),
            stats.clone(),
        ));
    }

    pub fn spawn_handle<F, R>(&self, task: F, token: u64) -> impl Future<Output = R>
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let env = self.env.clone();
        env.metrics_running_task_count.inc();
        let (tx, rx) = oneshot::channel();
        let wrapped_task = async move {
            let res = task.await;
            env.metrics_handled_task_count.inc();
            env.metrics_running_task_count.dec();
            try_tick_thread(&env);
            tx.send(res).ok();
        };
        // at begin a token has top priority
        let stats = self.stats_map.get_stats(token);
        self.scheduler.add_task(ArcTask::new(
            wrapped_task,
            self.scheduler.clone(),
            stats.clone(),
        ));
        rx.map(|res| res.expect("oneshot channel cancelled"))
    }
}

/// Tries to trigger a tick in current thread.
///
/// This function is effective only when it is called in thread pool worker
/// thread.
#[inline]
fn try_tick_thread(env: &Env) {
    THREAD_LAST_TICK_TIME.with(|tls_last_tick| {
        let now = Instant::now_coarse();
        let last_tick = tls_last_tick.get();
        if now.duration_since(last_tick) < TICK_INTERVAL {
            return;
        }
        tls_last_tick.set(now);
        if let Some(f) = &env.on_tick {
            f();
        }
    })
}

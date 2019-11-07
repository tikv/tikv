// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod builder;
mod metrics;
mod park;
mod scheduler;
mod stats;
mod task;
mod tokio_park;
mod worker;

use scheduler::Scheduler;
use stats::StatsMap;
use task::ArcTask;

use futures::FutureExt;
use prometheus::{IntCounter, IntGauge};
use rand::prelude::*;
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Full {
    pub current_tasks: usize,
    pub max_tasks: usize,
}

impl std::fmt::Display for Full {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "multilevel pool is full")
    }
}

impl std::error::Error for Full {
    fn description(&self) -> &str {
        "multilevel pool is full"
    }
}

pub struct SpawnOption {
    pub task_id: u64,
    pub fixed_level: Option<usize>,
}

impl Default for SpawnOption {
    fn default() -> SpawnOption {
        SpawnOption {
            task_id: thread_rng().next_u64(),
            fixed_level: None,
        }
    }
}

#[derive(Clone)]
pub struct MultiLevelPool {
    scheduler: Scheduler,
    stats_map: StatsMap,
    env: Arc<Env>,
    pool_size: usize,
    max_tasks: usize,
}

impl MultiLevelPool {
    /// Gets current running task count.
    #[inline]
    pub fn get_running_task_count(&self) -> usize {
        // As long as different future pool has different name prefix, we can safely use the value
        // in metrics.
        self.env.metrics_running_task_count.get() as usize
    }

    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    fn gate_spawn(&self) -> Result<(), Full> {
        let current_tasks = self.get_running_task_count();
        if current_tasks >= self.max_tasks {
            Err(Full {
                current_tasks,
                max_tasks: self.max_tasks,
            })
        } else {
            Ok(())
        }
    }

    /// Spawns a new `Future` to the thread pool. `token` is a unique ID of a job.
    /// Multiple `Future`s with the same token share the same statistics.
    pub fn spawn<F>(&self, task: F, opt: SpawnOption) -> Result<(), Full>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.gate_spawn()?;
        let env = self.env.clone();
        env.metrics_running_task_count.inc();
        let wrapped_task = async move {
            task.await;
            env.metrics_handled_task_count.inc();
            env.metrics_running_task_count.dec();
            try_tick_thread(&env);
        };
        let stats = self.stats_map.get_stats(opt.task_id);
        self.scheduler.add_task(ArcTask::new(
            wrapped_task,
            self.scheduler.clone(),
            stats.clone(),
            opt.fixed_level,
        ));
        Ok(())
    }

    /// Spawns a new `Future` to the thread pool, returning a `Future` (0.1) representing the
    /// produced value. Returns `Err(Full)` when running tasks exceed the limit.
    ///
    /// `token` is a unique ID of a job. Multiple `Future`s with the same token share the same
    /// statistics.
    pub fn spawn_handle<F, R>(
        &self,
        task: F,
        opt: SpawnOption,
    ) -> Result<impl Future<Output = R>, Full>
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        self.gate_spawn()?;
        let env = self.env.clone();
        env.metrics_running_task_count.inc();
        let (tx, rx) = tokio_sync::oneshot::channel();
        let wrapped_task = async move {
            let res = task.await;
            env.metrics_handled_task_count.inc();
            env.metrics_running_task_count.dec();
            try_tick_thread(&env);
            let _ = tx.send(res);
        };
        let stats = self.stats_map.get_stats(opt.task_id);
        self.scheduler.add_task(ArcTask::new(
            wrapped_task,
            self.scheduler.clone(),
            stats.clone(),
            opt.fixed_level,
        ));
        // Panic here because it must be a bug if the channel tx is dropped unexpectedly
        Ok(rx.map(|res| res.expect("oneshot channel cancelled")))
    }

    /// Spawns a new `Future` to the thread pool, returning a `Future` (std) representing the
    /// produced value. Returns `Err(Full)` when running tasks exceed the limit.
    ///
    /// `token` is a unique ID of a job. Multiple `Future`s with the same token share the same
    /// statistics.
    pub fn spawn_handle_legacy<F, T, E>(
        &self,
        task: F,
        opt: SpawnOption,
    ) -> Result<impl futures01::Future<Item = T, Error = E>, Full>
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        use futures01::Future;
        self.gate_spawn()?;
        let env = self.env.clone();
        env.metrics_running_task_count.inc();
        let (tx, rx) = tokio01_sync::oneshot::channel();
        let wrapped_task = async move {
            let res = task.await;
            env.metrics_handled_task_count.inc();
            env.metrics_running_task_count.dec();
            try_tick_thread(&env);
            let _ = tx.send(res);
        };
        let stats = self.stats_map.get_stats(opt.task_id);
        self.scheduler.add_task(ArcTask::new(
            wrapped_task,
            self.scheduler.clone(),
            stats.clone(),
            opt.fixed_level,
        ));
        // Panic here because it must be a bug if the channel tx is dropped unexpectedly
        Ok(rx.then(|res| res.expect("oneshot channel cancelled")))
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

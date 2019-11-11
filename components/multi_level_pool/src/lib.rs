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
    level_elapsed: [IntCounter; 3],
    level_proportions: [IntGauge; 2],
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

const BACKGROUND_TASK_NUM: usize = 2;

#[derive(Clone)]
pub struct MultiLevelPool {
    scheduler: Scheduler,
    stats_map: StatsMap,
    env: Arc<Env>,
    pool_size: usize,
    max_tasks: usize,
}

impl MultiLevelPool {
    /// Gets current running task count, excluding the background tasks.
    #[inline]
    pub fn get_running_task_count(&self) -> usize {
        // As long as different future pool has different name prefix, we can safely use the value
        // in metrics.
        // The background tasks may be not running when this method is called, so we saturates
        // the subtraction.
        (self.env.metrics_running_task_count.get() as usize).saturating_sub(BACKGROUND_TASK_NUM)
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
        let scheduler = self.scheduler.clone();

        Ok(async move {
            scheduler.add_task(ArcTask::new(
                wrapped_task,
                scheduler.clone(),
                stats,
                opt.fixed_level,
            ));
            // Panic here because it must be a bug if the channel tx is dropped unexpectedly
            rx.map(|res| res.expect("oneshot channel cancelled")).await
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread;

    use futures::executor::block_on;
    use tokio_timer::delay_for;

    fn spawn_future_and_wait(pool: &MultiLevelPool, duration: Duration) {
        block_on(
            pool.spawn_handle(
                async move {
                    delay_for(duration).await;
                },
                SpawnOption::default(),
            )
            .unwrap(),
        );
    }

    fn spawn_future_without_wait(pool: &MultiLevelPool, duration: Duration) {
        pool.spawn(
            async move {
                delay_for(duration).await;
            },
            SpawnOption::default(),
        )
        .unwrap();
    }

    #[test]
    fn test_tick() {
        let tick_sequence = Arc::new(AtomicUsize::new(0));

        let tick_sequence2 = tick_sequence.clone();
        let (tx, rx) = mpsc::sync_channel(1000);

        let pool = Builder::new()
            .pool_size(1)
            .on_tick(move || {
                let seq = tick_sequence2.fetch_add(1, Ordering::SeqCst);
                tx.send(seq).unwrap();
            })
            .build();

        assert!(rx.try_recv().is_err());

        // Tick is not emitted since there is no task
        thread::sleep(TICK_INTERVAL * 2);
        assert!(rx.try_recv().is_err());

        // Tick is emitted because long enough time has elapsed since pool is created
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert!(rx.try_recv().is_err());

        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);

        // So far we have only elapsed TICK_INTERVAL * 0.2, so no ticks so far.
        assert!(rx.try_recv().is_err());

        // Even if long enough time has elapsed, tick is not emitted until next task arrives
        thread::sleep(TICK_INTERVAL * 2);
        assert!(rx.try_recv().is_err());

        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert_eq!(rx.try_recv().unwrap(), 0);
        assert!(rx.try_recv().is_err());

        // Tick is not emitted if there is no task
        thread::sleep(TICK_INTERVAL * 2);
        assert!(rx.try_recv().is_err());

        // Tick is emitted since long enough time has passed
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(rx.try_recv().is_err());

        // Tick is emitted immediately after a long task
        spawn_future_and_wait(&pool, TICK_INTERVAL * 2);
        assert_eq!(rx.try_recv().unwrap(), 2);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_handle_drop() {
        let pool = Builder::new().pool_size(1).build();

        let (tx, rx) = mpsc::sync_channel(10);

        let tx2 = tx.clone();
        pool.spawn(
            async move {
                delay_for(Duration::from_millis(200)).await;
                tx2.send(11).unwrap();
            },
            SpawnOption::default(),
        )
        .unwrap();

        let tx2 = tx.clone();
        drop(
            pool.spawn_handle(
                async move {
                    tx2.send(7).unwrap();
                },
                SpawnOption::default(),
            )
            .unwrap(),
        );

        thread::sleep(Duration::from_millis(500));

        assert_eq!(rx.try_recv().unwrap(), 11);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_handle_result() {
        let pool = Builder::new().pool_size(1).build();

        let handle = pool.spawn_handle(async { 42 }, SpawnOption::default());

        assert_eq!(block_on(handle.unwrap()), 42);
    }

    #[test]
    fn test_running_task_count() {
        let pool = Builder::new()
            .name_prefix("future_pool_for_running_task_test") // The name is important
            .pool_size(2)
            .build();

        assert_eq!(pool.get_running_task_count(), 0);

        spawn_future_without_wait(&pool, Duration::from_millis(500)); // f1
        assert_eq!(pool.get_running_task_count(), 1);

        spawn_future_without_wait(&pool, Duration::from_millis(1000)); // f2
        assert_eq!(pool.get_running_task_count(), 2);

        spawn_future_without_wait(&pool, Duration::from_millis(1500));
        assert_eq!(pool.get_running_task_count(), 3);

        thread::sleep(Duration::from_millis(700)); // f1 completed, f2 elapsed 700
        assert_eq!(pool.get_running_task_count(), 2);

        spawn_future_without_wait(&pool, Duration::from_millis(1500));
        assert_eq!(pool.get_running_task_count(), 3);

        thread::sleep(Duration::from_millis(2700));
        assert_eq!(pool.get_running_task_count(), 0);
    }

    fn spawn_long_time_future(
        pool: &MultiLevelPool,
        id: u64,
        future_duration_ms: u64,
    ) -> Result<impl Future<Output = u64>, Full> {
        pool.spawn_handle(
            async move {
                delay_for(Duration::from_millis(future_duration_ms)).await;
                id
            },
            SpawnOption::default(),
        )
    }

    fn wait_on_new_thread<F>(sender: mpsc::Sender<F::Output>, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        thread::spawn(move || {
            let r = block_on(future);
            sender.send(r).unwrap();
        });
    }

    #[test]
    fn test_full() {
        let (tx, rx) = mpsc::channel();

        let read_pool = Builder::new()
            .name_prefix("future_pool_test_full")
            .pool_size(2)
            .max_tasks(4)
            .build();

        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 0, 5).unwrap(),
        );
        // not full
        assert_eq!(rx.recv().unwrap(), 0);

        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 1, 100).unwrap(),
        );
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 2, 200).unwrap(),
        );
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 3, 300).unwrap(),
        );
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 4, 400).unwrap(),
        );
        // no available results (running = 4)
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());

        // full
        assert!(spawn_long_time_future(&read_pool, 5, 100).is_err());

        // full
        assert!(spawn_long_time_future(&read_pool, 6, 100).is_err());

        // wait a future completes (running = 3)
        assert_eq!(rx.recv().unwrap(), 1);

        // add new (running = 4)
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 7, 5).unwrap(),
        );

        // full
        assert!(spawn_long_time_future(&read_pool, 8, 100).is_err());

        assert!(rx.recv().is_ok());
        assert!(rx.recv().is_ok());
        assert!(rx.recv().is_ok());
        assert!(rx.recv().is_ok());

        // no more results
        assert!(rx.recv_timeout(Duration::from_millis(500)).is_err());
    }
}

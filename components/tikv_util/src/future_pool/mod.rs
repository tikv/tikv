// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This mod implemented a wrapped future pool that supports `on_tick()` which
//! is invoked no less than the specific interval.

mod metrics;

use std::sync::Arc;
use std::time::Duration;

use self::metrics::*;
use crate::thd_name;
use crate::time::Instant;
use chocolates::thread_pool::future::{
    FutureThreadPool, Runner as FutureRunner, RunnerFactory as FutureRunnerFactory, SpawnHandle,
};
use chocolates::thread_pool::{self, Config as ThreadPoolConfig, PoolContext};
use futures::{lazy, Future};
use prometheus::local::LocalIntCounter;
use prometheus::{IntCounter, IntGauge};

const TICK_INTERVAL: Duration = Duration::from_secs(1);

pub trait TickRunner: Send {
    fn start(&mut self) {}
    fn on_tick(&mut self) {}
    fn handle(&mut self, _handle_time: Instant) {}
    fn end(&mut self) {}
}

pub trait TickRunnerFactory {
    type TickRunner: TickRunner;

    fn produce(&mut self) -> Self::TickRunner;
}

struct Metrics {
    running_task_count: IntGauge,
    handled_task_count: IntCounter,
}

struct Runner<T> {
    runner: FutureRunner,
    tick_runner: T,
    last_tick_time: Instant,
    metrics: Arc<Metrics>,
    local_handled_task_count: LocalIntCounter,
}

impl<T: TickRunner> thread_pool::Runner for Runner<T> {
    type Task = <FutureRunner as thread_pool::Runner>::Task;

    fn start(&mut self, ctx: &mut PoolContext<Self::Task>) {
        self.last_tick_time = Instant::now_coarse();
        self.runner.start(ctx);
        self.tick_runner.start();
    }

    fn pause(&mut self, ctx: &PoolContext<Self::Task>) -> bool {
        self.runner.pause(ctx)
    }

    fn resume(&mut self, ctx: &PoolContext<Self::Task>) {
        self.runner.resume(ctx)
    }

    fn handle(&mut self, ctx: &mut PoolContext<Self::Task>, task: Self::Task) -> bool {
        if !self.runner.handle(ctx, task) {
            return false;
        }
        let now = Instant::now_coarse();
        self.tick_runner.handle(now);
        self.local_handled_task_count.inc();
        self.metrics.running_task_count.dec();
        if now.duration_since(self.last_tick_time) < TICK_INTERVAL {
            return true;
        }
        self.last_tick_time = now;
        self.local_handled_task_count.flush();
        self.tick_runner.on_tick();
        true
    }

    fn end(&mut self, ctx: &PoolContext<Self::Task>) {
        self.runner.end(ctx);
        self.tick_runner.end();
        self.local_handled_task_count.flush();
    }
}

struct RunnerFactory<F> {
    tick_runner_factory: F,
    metrics: Arc<Metrics>,
    factory: FutureRunnerFactory,
}

impl<F: TickRunnerFactory> thread_pool::RunnerFactory for RunnerFactory<F> {
    type Runner = Runner<F::TickRunner>;

    fn produce(&mut self) -> Runner<F::TickRunner> {
        Runner {
            runner: self.factory.produce(),
            tick_runner: self.tick_runner_factory.produce(),
            last_tick_time: Instant::now(),
            metrics: self.metrics.clone(),
            local_handled_task_count: self.metrics.handled_task_count.local(),
        }
    }
}

#[derive(Clone)]
pub struct FuturePool {
    pool: Arc<FutureThreadPool>,
    metrics: Arc<Metrics>,
    max_tasks: usize,
}

impl std::fmt::Debug for FuturePool {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "FuturePool")
    }
}

impl crate::AssertSend for FuturePool {}
impl crate::AssertSync for FuturePool {}

impl FuturePool {
    /// Gets current running task count.
    #[inline]
    pub fn get_running_task_count(&self) -> usize {
        // As long as different future pool has different name prefix, we can safely use the value
        // in metrics.
        self.metrics.running_task_count.get() as usize
    }

    fn gate_spawn(&self) -> Result<(), Full> {
        fail_point!("future_pool_spawn_full", |_| Err(Full {
            current_tasks: 100,
            max_tasks: 100,
        }));

        if self.max_tasks == std::usize::MAX {
            return Ok(());
        }

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

    /// Spawns a future in the pool.
    pub fn spawn<F, R>(&self, future_fn: F) -> Result<(), Full>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Future<Item = (), Error = ()> + Send + 'static,
    {
        self.gate_spawn()?;

        self.metrics.running_task_count.inc();
        self.pool.spawn_future(lazy(future_fn));
        Ok(())
    }

    /// Spawns a future in the pool and returns a handle to the result of the future.
    ///
    /// The future will not be executed if the handle is not polled.
    #[must_use]
    pub fn spawn_handle<F, R>(&self, future_fn: F) -> Result<SpawnHandle<R::Item, R::Error>, Full>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Future + Send + 'static,
        R::Item: Send,
        R::Error: Send,
    {
        self.gate_spawn()?;

        self.metrics.running_task_count.inc();
        Ok(self.pool.spawn_future_handle(lazy(future_fn)))
    }
}

pub struct Noop;

impl TickRunner for Noop {}

#[derive(Clone)]
pub struct NoopFactory;

impl TickRunnerFactory for NoopFactory {
    type TickRunner = Noop;

    fn produce(&mut self) -> Noop {
        Noop
    }
}

#[derive(Clone)]
pub struct CloneFactory<T>(pub T);

impl<T: Clone + TickRunner> TickRunnerFactory for CloneFactory<T> {
    type TickRunner = T;

    fn produce(&mut self) -> T {
        self.0.clone()
    }
}

pub struct Builder<F> {
    cfg: ThreadPoolConfig,
    f: F,
    name_prefix: String,
    max_tasks: usize,
}

impl<F> Builder<F>
where
    F: TickRunnerFactory + Send,
    F::TickRunner: 'static,
{
    pub fn new(name_prefix: impl Into<String>, f: F) -> Builder<F> {
        let prefix = name_prefix.into();
        Builder {
            name_prefix: prefix.clone(),
            f,
            cfg: ThreadPoolConfig::new(thd_name!(prefix)),
            max_tasks: std::usize::MAX,
        }
    }

    pub fn build(self) -> FuturePool {
        let metrics = Arc::new(Metrics {
            running_task_count: FUTUREPOOL_RUNNING_TASK_VEC.with_label_values(&[&self.name_prefix]),
            handled_task_count: FUTUREPOOL_HANDLED_TASK_VEC.with_label_values(&[&self.name_prefix]),
        });
        let factory = RunnerFactory {
            tick_runner_factory: self.f,
            metrics: metrics.clone(),
            factory: FutureRunnerFactory::default(),
        };
        let pool = Arc::new(self.cfg.spawn(factory));
        FuturePool {
            pool,
            metrics,
            max_tasks: self.max_tasks,
        }
    }
}

impl<F> Builder<F> {
    pub fn pool_size(mut self, val: usize) -> Self {
        self.cfg.max_thread_count(val);
        self.cfg.min_thread_count(val);
        self
    }

    pub fn stack_size(mut self, val: usize) -> Self {
        self.cfg.stack_size(val);
        self
    }

    pub fn max_tasks(mut self, val: usize) -> Self {
        self.max_tasks = val;
        self
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Full {
    pub current_tasks: usize,
    pub max_tasks: usize,
}

impl std::fmt::Display for Full {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "future pool is full")
    }
}

impl std::error::Error for Full {
    fn description(&self) -> &str {
        "future pool is full"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread;

    use futures::future;

    fn spawn_future_and_wait(pool: &FuturePool, duration: Duration) {
        pool.spawn_handle(move || {
            thread::sleep(duration);
            future::ok::<_, ()>(())
        })
        .unwrap()
        .wait()
        .unwrap();
    }

    fn spawn_future_without_wait(pool: &FuturePool, duration: Duration) {
        pool.spawn(move || {
            thread::sleep(duration);
            future::ok::<_, ()>(())
        })
        .unwrap();
    }

    #[derive(Clone)]
    struct TickSequenceRecorder {
        tick_sequence: Arc<AtomicUsize>,
        tx: mpsc::SyncSender<usize>,
    }

    impl TickSequenceRecorder {
        fn new(
            tick_sequence: Arc<AtomicUsize>,
            tx: mpsc::SyncSender<usize>,
        ) -> TickSequenceRecorder {
            TickSequenceRecorder { tick_sequence, tx }
        }
    }

    impl TickRunner for TickSequenceRecorder {
        fn on_tick(&mut self) {
            let seq = self.tick_sequence.fetch_add(1, Ordering::SeqCst);
            self.tx.send(seq).unwrap();
        }
    }

    #[test]
    fn test_tick() {
        let tick_sequence = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::sync_channel(1000);
        let recorder = TickSequenceRecorder::new(tick_sequence.clone(), tx);

        let pool = Builder::new("test_tick", CloneFactory(recorder))
            .pool_size(1)
            .build();

        assert!(rx.try_recv().is_err());

        // Tick is not emitted since there is no task
        thread::sleep(TICK_INTERVAL * 2);
        assert!(rx.try_recv().is_err());

        // Tick is emitted because long enough time has elapsed since pool is created
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert_eq!(rx.try_recv().unwrap(), 0);

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
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(rx.try_recv().is_err());

        // Tick is not emitted if there is no task
        thread::sleep(TICK_INTERVAL * 2);
        assert!(rx.try_recv().is_err());

        // Tick is emitted since long enough time has passed
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert_eq!(rx.try_recv().unwrap(), 2);
        assert!(rx.try_recv().is_err());

        // Tick is emitted immediately after a long task
        spawn_future_and_wait(&pool, TICK_INTERVAL * 2);
        assert_eq!(rx.try_recv().unwrap(), 3);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_tick_multi_thread() {
        let tick_sequence = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::sync_channel(1000);
        let recorder = TickSequenceRecorder::new(tick_sequence.clone(), tx);

        let pool = Builder::new("test_tick_multi_thread", CloneFactory(recorder))
            .pool_size(2)
            .build();

        assert!(rx.try_recv().is_err());

        // Spawn two tasks, each will be processed in one worker thread.
        spawn_future_without_wait(&pool, TICK_INTERVAL / 2);
        spawn_future_without_wait(&pool, TICK_INTERVAL / 2);

        assert!(rx.try_recv().is_err());

        // Wait long enough time to trigger a tick.
        thread::sleep(TICK_INTERVAL * 2);

        assert!(rx.try_recv().is_err());

        // These two tasks should both trigger a tick.
        spawn_future_without_wait(&pool, TICK_INTERVAL);
        spawn_future_without_wait(&pool, TICK_INTERVAL / 2);

        // Wait until these tasks are finished.
        thread::sleep(TICK_INTERVAL * 2);

        assert_eq!(rx.try_recv().unwrap(), 0);
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_handle_drop() {
        let pool = Builder::new("test_handle_drop", NoopFactory)
            .pool_size(1)
            .build();

        let (tx, rx) = mpsc::sync_channel(10);

        let tx2 = tx.clone();
        pool.spawn(move || {
            thread::sleep(Duration::from_millis(200));
            tx2.send(11).unwrap();
            future::ok::<_, ()>(())
        })
        .unwrap();

        let tx2 = tx.clone();
        drop(
            pool.spawn_handle(move || {
                tx2.send(7).unwrap();
                future::ok::<_, ()>(())
            })
            .unwrap(),
        );

        thread::sleep(Duration::from_millis(500));

        assert_eq!(rx.try_recv().unwrap(), 11);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_handle_result() {
        let pool = Builder::new("test_handle_result", NoopFactory)
            .pool_size(1)
            .build();

        let handle = pool.spawn_handle(move || future::ok::<_, ()>(42));

        assert_eq!(handle.unwrap().wait().unwrap(), 42);
    }

    #[test]
    fn test_running_task_count() {
        let pool = Builder::new("future_pool_for_running_task_test", NoopFactory) // The name is important
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
        pool: &FuturePool,
        id: u64,
        future_duration_ms: u64,
    ) -> Result<SpawnHandle<u64, ()>, Full> {
        pool.spawn_handle(move || {
            thread::sleep(Duration::from_millis(future_duration_ms));
            future::ok::<u64, ()>(id)
        })
    }

    fn wait_on_new_thread<F>(
        sender: mpsc::Sender<std::result::Result<F::Item, F::Error>>,
        future: F,
    ) where
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        thread::spawn(move || {
            let r = future.wait();
            sender.send(r).unwrap();
        });
    }

    #[test]
    fn test_full() {
        let (tx, rx) = mpsc::channel();

        let read_pool = Builder::new("future_pool_test_full", NoopFactory)
            .pool_size(2)
            .max_tasks(4)
            .build();

        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 0, 5).unwrap(),
        );
        // not full
        assert_eq!(rx.recv().unwrap(), Ok(0));

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
        assert_eq!(rx.recv().unwrap(), Ok(1));

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

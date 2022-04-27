// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This mod implemented a wrapped future pool that supports `on_tick()` which
//! is invoked no less than the specific interval.

use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use fail::fail_point;
use futures::channel::oneshot::{self, Canceled};
use prometheus::{IntCounter, IntGauge};
use yatp::task::future;

pub type ThreadPool = yatp::ThreadPool<future::TaskCell>;

use super::metrics;

#[derive(Clone)]
struct Env {
    metrics_running_task_count: IntGauge,
    metrics_handled_task_count: IntCounter,
}

#[derive(Clone)]
pub struct FuturePool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for FuturePool {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "FuturePool")
    }
}

impl crate::AssertSend for FuturePool {}
impl crate::AssertSync for FuturePool {}

impl FuturePool {
    pub fn from_pool(pool: ThreadPool, name: &str, pool_size: usize, max_tasks: usize) -> Self {
        let env = Env {
            metrics_running_task_count: metrics::FUTUREPOOL_RUNNING_TASK_VEC
                .with_label_values(&[name]),
            metrics_handled_task_count: metrics::FUTUREPOOL_HANDLED_TASK_VEC
                .with_label_values(&[name]),
        };
        FuturePool {
            inner: Arc::new(PoolInner {
                pool,
                env,
                pool_size: AtomicUsize::new(pool_size),
                max_tasks,
            }),
        }
    }

    /// Gets inner thread pool size.
    #[inline]
    pub fn get_pool_size(&self) -> usize {
        self.inner.pool_size.load(Ordering::Relaxed)
    }

    pub fn scale_pool_size(&self, thread_count: usize) {
        self.inner.scale_pool_size(thread_count)
    }

    /// Gets current running task count.
    #[inline]
    pub fn get_running_task_count(&self) -> usize {
        // As long as different future pool has different name prefix, we can safely use the value
        // in metrics.
        self.inner.get_running_task_count()
    }

    /// Spawns a future in the pool.
    pub fn spawn<F>(&self, future: F) -> Result<(), Full>
    where
        F: Future + Send + 'static,
    {
        self.inner.spawn(future)
    }

    /// Spawns a future in the pool and returns a handle to the result of the future.
    ///
    /// The future will not be executed if the handle is not polled.
    pub fn spawn_handle<F>(
        &self,
        future: F,
    ) -> Result<impl Future<Output = Result<F::Output, Canceled>>, Full>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.inner.spawn_handle(future)
    }
}

struct PoolInner {
    pool: ThreadPool,
    env: Env,
    // for accessing pool_size config since yatp doesn't offer such getter.
    pool_size: AtomicUsize,
    max_tasks: usize,
}

impl PoolInner {
    #[inline]
    fn scale_pool_size(&self, thread_count: usize) {
        self.pool.scale_workers(thread_count);
        self.pool_size.store(thread_count, Ordering::Release);
    }

    fn get_running_task_count(&self) -> usize {
        // As long as different future pool has different name prefix, we can safely use the value
        // in metrics.
        self.env.metrics_running_task_count.get() as usize
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

    fn spawn<F>(&self, future: F) -> Result<(), Full>
    where
        F: Future + Send + 'static,
    {
        let metrics_handled_task_count = self.env.metrics_handled_task_count.clone();
        let metrics_running_task_count = self.env.metrics_running_task_count.clone();

        self.gate_spawn()?;

        metrics_running_task_count.inc();

        self.pool.spawn(async move {
            let _ = future.await;
            metrics_handled_task_count.inc();
            metrics_running_task_count.dec();
        });
        Ok(())
    }

    fn spawn_handle<F>(
        &self,
        future: F,
    ) -> Result<impl Future<Output = Result<F::Output, Canceled>>, Full>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let metrics_handled_task_count = self.env.metrics_handled_task_count.clone();
        let metrics_running_task_count = self.env.metrics_running_task_count.clone();

        self.gate_spawn()?;

        let (tx, rx) = oneshot::channel();
        metrics_running_task_count.inc();
        self.pool.spawn(async move {
            let res = future.await;
            metrics_handled_task_count.inc();
            metrics_running_task_count.dec();
            let _ = tx.send(res);
        });
        Ok(rx)
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
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc, Mutex,
        },
        thread,
        time::Duration,
    };

    use futures::executor::block_on;

    use super::{
        super::{DefaultTicker, PoolTicker, YatpPoolBuilder as Builder, TICK_INTERVAL},
        *,
    };

    fn spawn_future_and_wait(pool: &FuturePool, duration: Duration) {
        block_on(
            pool.spawn_handle(async move {
                thread::sleep(duration);
            })
            .unwrap(),
        )
        .unwrap();
    }

    fn spawn_future_without_wait(pool: &FuturePool, duration: Duration) {
        pool.spawn(async move {
            thread::sleep(duration);
        })
        .unwrap();
    }

    #[derive(Clone)]
    pub struct SequenceTicker {
        tick: Arc<dyn Fn() + Send + Sync>,
    }

    impl SequenceTicker {
        pub fn new<F>(tick: F) -> SequenceTicker
        where
            F: Fn() + Send + Sync + 'static,
        {
            SequenceTicker {
                tick: Arc::new(tick),
            }
        }
    }

    impl PoolTicker for SequenceTicker {
        fn on_tick(&mut self) {
            (self.tick)();
        }
    }

    #[test]
    fn test_tick() {
        let tick_sequence = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = mpsc::sync_channel(1000);
        let rx = Arc::new(Mutex::new(rx));
        let ticker = SequenceTicker::new(move || {
            let seq = tick_sequence.fetch_add(1, Ordering::SeqCst);
            tx.send(seq).unwrap();
        });

        let pool = Builder::new(ticker)
            .thread_count(1, 1, 1)
            .build_future_pool();
        let try_recv_tick = || {
            let rx = rx.clone();
            block_on(
                pool.spawn_handle(async move { rx.lock().unwrap().try_recv() })
                    .unwrap(),
            )
            .unwrap()
        };

        assert!(try_recv_tick().is_err());

        // Tick is emitted because long enough time has elapsed since pool is created
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert!(try_recv_tick().is_err());

        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);

        // So far we have only elapsed TICK_INTERVAL * 0.2, so no ticks so far.
        assert!(try_recv_tick().is_err());

        // Even if long enough time has elapsed, tick is not emitted until next task arrives
        thread::sleep(TICK_INTERVAL * 2);
        assert!(try_recv_tick().is_err());

        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert_eq!(try_recv_tick().unwrap(), 0);
        assert!(try_recv_tick().is_err());

        // Tick is not emitted if there is no task
        thread::sleep(TICK_INTERVAL * 2);
        assert!(try_recv_tick().is_err());

        // Tick is emitted since long enough time has passed
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert_eq!(try_recv_tick().unwrap(), 1);
        assert!(try_recv_tick().is_err());

        // Tick is emitted immediately after a long task
        spawn_future_and_wait(&pool, TICK_INTERVAL * 2);
        assert_eq!(try_recv_tick().unwrap(), 2);
        assert!(try_recv_tick().is_err());
    }

    #[test]
    fn test_tick_multi_thread() {
        let tick_sequence = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = mpsc::sync_channel(1000);
        let ticker = SequenceTicker::new(move || {
            let seq = tick_sequence.fetch_add(1, Ordering::SeqCst);
            tx.send(seq).unwrap();
        });

        let pool = Builder::new(ticker)
            .thread_count(2, 2, 2)
            .build_future_pool();

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
    fn test_handle_result() {
        let pool = Builder::new(DefaultTicker {})
            .thread_count(1, 1, 1)
            .build_future_pool();

        let handle = pool.spawn_handle(async { 42 });

        assert_eq!(block_on(handle.unwrap()).unwrap(), 42);
    }

    #[test]
    fn test_running_task_count() {
        let pool = Builder::new(DefaultTicker {})
            .name_prefix("future_pool_for_running_task_test") // The name is important
            .thread_count(2, 2, 2)
            .build_future_pool();

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
    ) -> Result<impl Future<Output = Result<u64, Canceled>>, Full> {
        pool.spawn_handle(async move {
            thread::sleep(Duration::from_millis(future_duration_ms));
            id
        })
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

        let read_pool = Builder::new(DefaultTicker {})
            .name_prefix("future_pool_test_full")
            .thread_count(2, 2, 2)
            .max_tasks(4)
            .build_future_pool();

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
        wait_on_new_thread(tx, spawn_long_time_future(&read_pool, 7, 5).unwrap());

        // full
        assert!(spawn_long_time_future(&read_pool, 8, 100).is_err());

        assert!(rx.recv().is_ok());
        assert!(rx.recv().is_ok());
        assert!(rx.recv().is_ok());
        assert!(rx.recv().is_ok());

        // no more results
        assert!(rx.recv_timeout(Duration::from_millis(500)).is_err());
    }

    #[test]
    fn test_scale_pool_size() {
        let pool = Builder::new(DefaultTicker {})
            .thread_count(1, 4, 8)
            .build_future_pool();

        assert_eq!(pool.get_pool_size(), 4);
        let cloned = pool.clone();

        pool.scale_pool_size(8);
        assert_eq!(pool.get_pool_size(), 8);
        assert_eq!(cloned.get_pool_size(), 8);

        pool.scale_pool_size(1);
        assert_eq!(pool.get_pool_size(), 1);
        assert_eq!(cloned.get_pool_size(), 1);
    }
}

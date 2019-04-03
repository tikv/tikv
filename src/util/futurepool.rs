// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.
//! This mod implemented a wrapped future pool that supports `on_tick()` which
//! is invoked no less than the specific interval.

use std::cell::{Cell, RefCell, RefMut};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use futures::Future;
use futures_cpupool::{self as cpupool, CpuFuture, CpuPool};
use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec};

use crate::util;
use crate::util::collections::HashMap;
use crate::util::time::Instant;

lazy_static! {
    pub static ref FUTUREPOOL_PENDING_TASK_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_futurepool_pending_task_total",
        "Current future_pool pending + running tasks.",
        &["name"]
    )
    .unwrap();
    pub static ref FUTUREPOOL_HANDLED_TASK_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_futurepool_handled_task_total",
        "Total number of future_pool handled tasks.",
        &["name"]
    )
    .unwrap();
}

pub trait Context: fmt::Debug + Send {
    /// Will be invoked periodically (no less than specified interval).
    /// When there is no task, it will NOT be invoked.
    fn on_tick(&mut self) {}
}

/// A delegator to wrap `Context` to provide `on_tick` feature.
#[derive(Debug)]
struct ContextDelegator<T: Context> {
    tick_interval: Duration,
    inner: RefCell<T>,
    last_tick: Cell<Option<Instant>>,
}

impl<T: Context> ContextDelegator<T> {
    fn new(context: T, tick_interval: Duration) -> ContextDelegator<T> {
        ContextDelegator {
            tick_interval,
            inner: RefCell::new(context),
            last_tick: Cell::new(None),
        }
    }

    fn context_mut(&self) -> RefMut<'_, T> {
        self.inner.borrow_mut()
    }

    fn on_task_finish(&self) {
        let now = Instant::now_coarse();
        let last_tick = self.last_tick.get();
        if last_tick.is_none() {
            // set last_tick when the first future is resolved
            self.last_tick.set(Some(now));
            return;
        }
        if now.duration_since(last_tick.unwrap()) < self.tick_interval {
            return;
        }
        self.last_tick.set(Some(now));
        self.context_mut().on_tick();
    }
}

#[derive(Debug)]
pub struct ContextDelegators<T: Context> {
    delegators: Arc<HashMap<thread::ThreadId, ContextDelegator<T>>>,
}

/// Users can only retrive a Context for the current thread so that `HashMap<..>` is `Sync`.
/// Thus `ContextDelegators` is `Send` & `Sync`.
unsafe impl<T: Context> Send for ContextDelegators<T> {}
unsafe impl<T: Context> Sync for ContextDelegators<T> {}

impl<T: Context> Clone for ContextDelegators<T> {
    fn clone(&self) -> Self {
        ContextDelegators {
            delegators: Arc::clone(&self.delegators),
        }
    }
}

impl<T: Context> ContextDelegators<T> {
    fn new(delegators: HashMap<thread::ThreadId, ContextDelegator<T>>) -> Self {
        ContextDelegators {
            delegators: Arc::new(delegators),
        }
    }

    /// This function should be called in the future pool thread. Otherwise it will panic.
    pub fn current_thread_context_mut(&self) -> RefMut<'_, T> {
        let delegator = self.get_current_thread_delegator();
        delegator.context_mut()
    }

    fn get_current_thread_delegator(&self) -> &ContextDelegator<T> {
        let thread_id = thread::current().id();
        if let Some(delegator) = self.delegators.get(&thread_id) {
            delegator
        } else {
            panic!("Called from threads out of the future thread pool");
        }
    }
}

/// A future thread pool that supports `on_tick` for each thread.
pub struct FuturePool<T: Context + 'static> {
    pool: CpuPool,
    context_delegators: ContextDelegators<T>,
    running_task_count: Arc<AtomicUsize>,
    metrics_pending_task_count: IntGauge,
    metrics_handled_task_count: IntCounter,
}

impl<T: Context + 'static> fmt::Debug for FuturePool<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("FuturePool")
            .field("pool", &self.pool)
            .field("context_delegators", &self.context_delegators)
            .finish()
    }
}

impl<T: Context + 'static> Clone for FuturePool<T> {
    fn clone(&self) -> FuturePool<T> {
        FuturePool {
            pool: self.pool.clone(),
            context_delegators: self.context_delegators.clone(),
            running_task_count: Arc::clone(&self.running_task_count),
            metrics_pending_task_count: self.metrics_pending_task_count.clone(),
            metrics_handled_task_count: self.metrics_handled_task_count.clone(),
        }
    }
}

impl<T: Context + 'static> util::AssertSend for FuturePool<T> {}
impl<T: Context + 'static> util::AssertSync for FuturePool<T> {}

impl<T: Context + 'static> FuturePool<T> {
    pub fn new<F>(
        pool_size: usize,
        stack_size: usize,
        name_prefix: &str,
        tick_interval: Duration,
        context_factory: F,
    ) -> FuturePool<T>
    where
        F: Fn() -> T,
    {
        let (tx, rx) = mpsc::sync_channel(pool_size);
        let pool = cpupool::Builder::new()
            .pool_size(pool_size)
            .stack_size(stack_size)
            .name_prefix(name_prefix)
            .after_start(move || {
                // We only need to know each thread's id and we can build context later
                // by invoking `context_factory` in a non-concurrent way.
                let thread_id = thread::current().id();
                tx.send(thread_id).unwrap();
            })
            .create();
        let contexts = (0..pool_size)
            .map(|_| {
                let thread_id = rx.recv().unwrap();
                let context_delegator = ContextDelegator::new(context_factory(), tick_interval);
                (thread_id, context_delegator)
            })
            .collect();
        FuturePool {
            pool,
            context_delegators: ContextDelegators::new(contexts),
            running_task_count: Arc::new(AtomicUsize::new(0)),
            metrics_pending_task_count: FUTUREPOOL_PENDING_TASK_VEC
                .with_label_values(&[name_prefix]),
            metrics_handled_task_count: FUTUREPOOL_HANDLED_TASK_VEC
                .with_label_values(&[name_prefix]),
        }
    }

    /// Gets current running task count
    #[inline]
    pub fn get_running_task_count(&self) -> usize {
        self.running_task_count.load(Ordering::Acquire)
    }

    pub fn spawn<F, R>(&self, future_factory: R) -> CpuFuture<F::Item, F::Error>
    where
        R: FnOnce(ContextDelegators<T>) -> F + Send + 'static,
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        let running_task_count = Arc::clone(&self.running_task_count);
        let metrics_pending_task_count = self.metrics_pending_task_count.clone();
        let metrics_handled_task_count = self.metrics_handled_task_count.clone();
        let delegators = self.context_delegators.clone();
        let func = move || {
            future_factory(delegators.clone()).then(move |r| {
                let delegator = delegators.get_current_thread_delegator();
                delegator.on_task_finish();
                running_task_count.fetch_sub(1, Ordering::Release);
                metrics_pending_task_count.dec();
                metrics_handled_task_count.inc();
                r
            })
        };

        self.running_task_count.fetch_add(1, Ordering::Release);
        self.metrics_pending_task_count.inc();
        self.pool.spawn_fn(func)
    }
}

#[cfg(test)]
mod tests {
    use futures::future;
    use std::sync::mpsc::{channel, Sender};
    use std::thread;
    use std::time::Duration;

    pub use super::*;

    fn spawn_long_time_future<T: Context>(
        pool: &FuturePool<T>,
        future_duration_ms: u64,
    ) -> CpuFuture<(), ()> {
        pool.spawn(move |_| {
            thread::sleep(Duration::from_millis(future_duration_ms));
            future::ok::<(), ()>(())
        })
    }

    fn spawn_long_time_future_and_wait<T: Context>(pool: &FuturePool<T>, future_duration_ms: u64) {
        spawn_long_time_future(pool, future_duration_ms)
            .wait()
            .unwrap();
    }

    #[test]
    fn test_context() {
        #[derive(Debug)]
        struct MyContext {
            ctx_thread_id: thread::ThreadId,
        }
        impl Context for MyContext {}

        let pool = FuturePool::new(
            1,
            1024000,
            "test-pool",
            Duration::from_millis(50),
            move || MyContext {
                ctx_thread_id: thread::current().id(),
            },
        );

        let main_thread_id = thread::current().id();

        pool.spawn(move |ctxd| {
            // future_factory is executed in future pool
            let current_thread_id = thread::current().id();
            assert_ne!(main_thread_id, current_thread_id);

            // Context is created in main thread
            let ctx = ctxd.current_thread_context_mut();
            assert_eq!(ctx.ctx_thread_id, main_thread_id);
            future::ok::<(), ()>(())
        })
        .wait()
        .unwrap();
    }

    #[test]
    fn test_tick() {
        struct MyContext {
            tx: Sender<i32>,
            sn: i32,
        }
        impl fmt::Debug for MyContext {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "MyContext")
            }
        }
        impl Context for MyContext {
            fn on_tick(&mut self) {
                self.tx.send(self.sn).unwrap();
                self.sn += 1;
            }
        }

        let (tx, rx) = channel();

        let pool = FuturePool::new(
            1,
            1024000,
            "test-pool",
            Duration::from_millis(200),
            move || MyContext {
                tx: tx.clone(),
                sn: 0,
            },
        );
        assert!(rx.try_recv().is_err());

        // Tick is not emitted since there is no task
        thread::sleep(Duration::from_millis(400));
        assert!(rx.try_recv().is_err());

        // Tick is not emitted immediately for the first future
        spawn_long_time_future_and_wait(&pool, 10);
        assert!(rx.try_recv().is_err());

        spawn_long_time_future_and_wait(&pool, 1);
        spawn_long_time_future_and_wait(&pool, 1);
        spawn_long_time_future_and_wait(&pool, 1);
        spawn_long_time_future_and_wait(&pool, 1);
        assert!(rx.try_recv().is_err());

        // Even if there are tasks previously, tick is not emitted until next task arrives
        thread::sleep(Duration::from_millis(400));
        assert!(rx.try_recv().is_err());

        // Next task arrives && long time passed, tick is emitted
        spawn_long_time_future_and_wait(&pool, 400);
        assert_eq!(rx.try_recv().unwrap(), 0);
        assert!(rx.try_recv().is_err());

        // Tick is not emitted if there is no task
        thread::sleep(Duration::from_millis(400));
        assert!(rx.try_recv().is_err());

        // Tick is emitted since long enough time has passed
        spawn_long_time_future_and_wait(&pool, 1);
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_task_count() {
        #[derive(Debug)]
        struct MyContext;
        impl Context for MyContext {}

        let pool = FuturePool::new(
            2,
            1024000,
            "test-pool",
            Duration::from_millis(50),
            move || MyContext {},
        );

        assert_eq!(pool.get_running_task_count(), 0);
        let f1 = spawn_long_time_future(&pool, 100);
        assert_eq!(pool.get_running_task_count(), 1);
        let f2 = spawn_long_time_future(&pool, 200);
        assert_eq!(pool.get_running_task_count(), 2);
        let f3 = spawn_long_time_future(&pool, 300);
        assert_eq!(pool.get_running_task_count(), 3);
        f1.wait().unwrap();
        assert_eq!(pool.get_running_task_count(), 2);
        let f4 = spawn_long_time_future(&pool, 300);
        let f5 = spawn_long_time_future(&pool, 300);
        assert_eq!(pool.get_running_task_count(), 4);
        f2.join(f3).wait().unwrap();
        assert_eq!(pool.get_running_task_count(), 2);
        f4.join(f5).wait().unwrap();
        assert_eq!(pool.get_running_task_count(), 0);
    }
}

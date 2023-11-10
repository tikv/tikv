// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod future_pool;
pub mod metrics;

use std::sync::Arc;

use fail::fail_point;
pub use future_pool::{Full, FuturePool};
use futures::{compat::Stream01CompatExt, StreamExt};
use prometheus::{local::LocalHistogram, Histogram, HistogramOpts};
use yatp::{
    pool::{CloneRunnerBuilder, Local, Remote, Runner},
    queue::{multilevel, priority, Extras, QueueType, TaskCell as _},
    task::future::{Runner as FutureRunner, TaskCell},
    ThreadPool,
};

use crate::{
    thread_group::GroupProperties,
    time::{Duration, Instant},
    timer::GLOBAL_TIMER_HANDLE,
};

const DEFAULT_CLEANUP_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(10)
};

fn background_cleanup_task<F>(cleanup: F) -> TaskCell
where
    F: Fn() -> Option<std::time::Instant> + Send + 'static,
{
    let mut interval = GLOBAL_TIMER_HANDLE
        .interval(
            std::time::Instant::now() + DEFAULT_CLEANUP_INTERVAL,
            DEFAULT_CLEANUP_INTERVAL,
        )
        .compat();
    TaskCell::new(
        async move {
            while let Some(Ok(_)) = interval.next().await {
                cleanup();
            }
        },
        Extras::multilevel_default(),
    )
}

/// CleanupMethod describes how a pool cleanup its internal task-elapsed map. A
/// task-elapsed map is used for tracking how long each task has been running,
/// so that the pool can adjust the level of a task according to its running
/// time. To prevent a task-elapsed map from growing too large, the following
/// strategies are provided for cleaning up it periodically.
pub enum CleanupMethod {
    /// Cleanup in place on spawning.
    InPlace,
    /// Cleanup in this pool (the one to be built) locally.
    Local,
    /// Cleanup in the given remote pool.
    Remote(Remote<TaskCell>),
}

impl CleanupMethod {
    /// Returns the perferred cleanup interval used for creating a queue
    /// builder.
    fn preferred_interval(&self) -> Option<std::time::Duration> {
        match self {
            Self::InPlace => Some(DEFAULT_CLEANUP_INTERVAL),
            _ => None,
        }
    }

    /// Tries to create a task from the cleanup function and spawn it if
    /// possible, returns Some(task) if there is a task shall be spawned but
    /// hasn't been spawned (that is, need to be spawned locally later).
    fn try_spawn<F>(&self, cleanup: F) -> Option<TaskCell>
    where
        F: Fn() -> Option<std::time::Instant> + Send + 'static,
    {
        match self {
            Self::InPlace => None,
            Self::Local => Some(background_cleanup_task(cleanup)),
            Self::Remote(remote) => {
                remote.spawn(background_cleanup_task(cleanup));
                None
            }
        }
    }
}

pub(crate) const TICK_INTERVAL: Duration = Duration::from_secs(1);

fn tick_interval() -> Duration {
    fail_point!("mock_tick_interval", |_| { Duration::from_millis(1) });
    TICK_INTERVAL
}

pub trait PoolTicker: Send + Clone + 'static {
    fn on_tick(&mut self);
}

#[derive(Clone)]
pub struct TickerWrapper<T: PoolTicker> {
    ticker: T,
    last_tick_time: Instant,
}

impl<T: PoolTicker> TickerWrapper<T> {
    pub fn new(ticker: T) -> Self {
        Self {
            ticker,
            last_tick_time: Instant::now_coarse(),
        }
    }

    // Returns whether tick has been triggered.
    pub fn try_tick(&mut self) -> bool {
        let now = Instant::now_coarse();
        if now.saturating_duration_since(self.last_tick_time) < tick_interval() {
            return false;
        }
        self.last_tick_time = now;
        self.ticker.on_tick();
        true
    }

    pub fn on_tick(&mut self) {
        self.ticker.on_tick();
    }
}

#[derive(Clone, Default)]
pub struct DefaultTicker {}

impl PoolTicker for DefaultTicker {
    fn on_tick(&mut self) {}
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub workers: usize,
    pub max_tasks_per_worker: usize,
    pub stack_size: usize,
}

impl Config {
    pub fn default_for_test() -> Self {
        Self {
            workers: 2,
            max_tasks_per_worker: std::usize::MAX,
            stack_size: 2_000_000,
        }
    }
}

#[derive(Clone)]
pub struct YatpPoolRunner<T: PoolTicker> {
    inner: FutureRunner,
    ticker: TickerWrapper<T>,
    props: Option<GroupProperties>,
    after_start: Option<Arc<dyn Fn() + Send + Sync>>,
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    before_pause: Option<Arc<dyn Fn() + Send + Sync>>,

    // Statistics about the schedule wait duration.
    // local histogram for high,medium,low priority tasks.
    schedule_wait_durations: [LocalHistogram; 3],
    // return the index of `schedule_wait_durations` from task metadata.
    metric_idx_from_task_meta: Arc<dyn Fn(&[u8]) -> usize + Send + Sync>,
}

impl<T: PoolTicker> Runner for YatpPoolRunner<T> {
    type TaskCell = TaskCell;

    fn start(&mut self, local: &mut Local<Self::TaskCell>) {
        crate::sys::thread::call_thread_start_hooks();
        crate::sys::thread::add_thread_name_to_map();
        if let Some(props) = self.props.take() {
            crate::thread_group::set_properties(Some(props));
        }
        self.inner.start(local);
        if let Some(f) = self.after_start.take() {
            f();
        }
        tikv_alloc::add_thread_memory_accessor()
    }

    fn handle(&mut self, local: &mut Local<Self::TaskCell>, mut task_cell: Self::TaskCell) -> bool {
        let extras = task_cell.mut_extras();
        if let Some(schedule_time) = extras.schedule_time() {
            let idx = (*self.metric_idx_from_task_meta)(extras.metadata());
            self.schedule_wait_durations[idx].observe(schedule_time.elapsed().as_secs_f64());
        }
        let finished = self.inner.handle(local, task_cell);
        if self.ticker.try_tick() {
            self.schedule_wait_durations.iter().for_each(|m| m.flush());
        }
        finished
    }

    fn pause(&mut self, local: &mut Local<Self::TaskCell>) -> bool {
        if let Some(f) = self.before_pause.as_ref() {
            f();
        }
        self.inner.pause(local)
    }

    fn resume(&mut self, local: &mut Local<Self::TaskCell>) {
        self.inner.resume(local)
    }

    fn end(&mut self, local: &mut Local<Self::TaskCell>) {
        if let Some(f) = self.before_stop.as_ref() {
            f();
        }
        self.ticker.on_tick();
        self.inner.end(local);
        tikv_alloc::remove_thread_memory_accessor();
        crate::sys::thread::remove_thread_name_from_map()
    }
}

impl<T: PoolTicker> YatpPoolRunner<T> {
    pub fn new(
        inner: FutureRunner,
        ticker: TickerWrapper<T>,
        after_start: Option<Arc<dyn Fn() + Send + Sync>>,
        before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
        before_pause: Option<Arc<dyn Fn() + Send + Sync>>,
        schedule_wait_durations: [Histogram; 3],
        metric_idx_from_task_meta: Arc<dyn Fn(&[u8]) -> usize + Send + Sync>,
    ) -> Self {
        YatpPoolRunner {
            inner,
            ticker,
            props: crate::thread_group::current_properties(),
            after_start,
            before_stop,
            before_pause,
            schedule_wait_durations: schedule_wait_durations.map(|m| m.local()),
            metric_idx_from_task_meta,
        }
    }
}

pub struct YatpPoolBuilder<T: PoolTicker> {
    name_prefix: Option<String>,
    ticker: TickerWrapper<T>,
    after_start: Option<Arc<dyn Fn() + Send + Sync>>,
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    before_pause: Option<Arc<dyn Fn() + Send + Sync>>,
    min_thread_count: usize,
    core_thread_count: usize,
    max_thread_count: usize,
    stack_size: usize,
    max_tasks: usize,
    cleanup_method: CleanupMethod,

    // whether to tracker task scheduling wait duration
    enable_task_wait_metrics: bool,
    metric_idx_from_task_meta: Option<Arc<dyn Fn(&[u8]) -> usize + Send + Sync>>,

    #[cfg(test)]
    background_cleanup_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl<T: PoolTicker> YatpPoolBuilder<T> {
    pub fn new(ticker: T) -> Self {
        Self {
            ticker: TickerWrapper::new(ticker),
            name_prefix: None,
            after_start: None,
            before_stop: None,
            before_pause: None,
            min_thread_count: 1,
            core_thread_count: 1,
            max_thread_count: 1,
            stack_size: 0,
            max_tasks: std::usize::MAX,
            cleanup_method: CleanupMethod::InPlace,

            enable_task_wait_metrics: false,
            metric_idx_from_task_meta: None,

            #[cfg(test)]
            background_cleanup_hook: None,
        }
    }

    pub fn config(self, config: Config) -> Self {
        // TODO: maybe we should use (1, num_cpu) for min and max thread count.
        self.thread_count(config.workers, config.workers, config.workers)
            .stack_size(config.stack_size)
            .max_tasks(config.workers.saturating_mul(config.max_tasks_per_worker))
    }

    pub fn stack_size(mut self, val: usize) -> Self {
        self.stack_size = val;
        self
    }

    pub fn name_prefix(mut self, val: impl Into<String>) -> Self {
        let name = val.into();
        self.name_prefix = Some(name);
        self
    }

    pub fn thread_count(
        mut self,
        min_thread_count: usize,
        core_thread_count: usize,
        max_thread_count: usize,
    ) -> Self {
        self.min_thread_count = min_thread_count;
        self.core_thread_count = core_thread_count;
        self.max_thread_count = max_thread_count;
        self
    }

    pub fn max_tasks(mut self, tasks: usize) -> Self {
        self.max_tasks = tasks;
        self
    }

    pub fn cleanup_method(mut self, method: CleanupMethod) -> Self {
        self.cleanup_method = method;
        self
    }

    pub fn before_stop<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.before_stop = Some(Arc::new(f));
        self
    }

    pub fn after_start<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.after_start = Some(Arc::new(f));
        self
    }

    pub fn before_pause<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.before_pause = Some(Arc::new(f));
        self
    }

    pub fn enable_task_wait_metrics(mut self) -> Self {
        self.enable_task_wait_metrics = true;
        self
    }

    pub fn metric_idx_from_task_meta(
        mut self,
        f: Arc<dyn Fn(&[u8]) -> usize + Send + Sync>,
    ) -> Self {
        self.metric_idx_from_task_meta = Some(f);
        self
    }

    pub fn build_future_pool(self) -> FuturePool {
        let name = self
            .name_prefix
            .clone()
            .unwrap_or_else(|| "yatp_pool".to_string());
        let size = self.core_thread_count;
        let task = self.max_tasks;
        let pool = self.build_single_level_pool();
        FuturePool::from_pool(pool, &name, size, task)
    }

    pub fn build_priority_future_pool(
        self,
        priority_provider: Arc<dyn priority::TaskPriorityProvider>,
    ) -> FuturePool {
        let name = self
            .name_prefix
            .clone()
            .unwrap_or_else(|| "yatp_pool".to_string());
        let size = self.core_thread_count;
        let task = self.max_tasks;
        let pool = self.build_priority_pool(priority_provider);
        FuturePool::from_pool(pool, &name, size, task)
    }

    pub fn build_single_level_pool(self) -> ThreadPool<TaskCell> {
        let (builder, runner) = self.create_builder();
        builder.build_with_queue_and_runner(
            yatp::queue::QueueType::SingleLevel,
            yatp::pool::CloneRunnerBuilder(runner),
        )
    }

    pub fn build_multi_level_pool(self) -> ThreadPool<TaskCell> {
        let name = self
            .name_prefix
            .clone()
            .unwrap_or_else(|| "yatp_pool".to_string());
        let multilevel_builder = multilevel::Builder::new(
            multilevel::Config::default()
                .name(Some(name))
                .cleanup_interval(self.cleanup_method.preferred_interval()),
        );
        let pending_task = self.try_spawn_cleanup(multilevel_builder.cleanup_fn());
        let (builder, read_pool_runner) = self.create_builder();
        let runner_builder =
            multilevel_builder.runner_builder(CloneRunnerBuilder(read_pool_runner));
        let pool = builder
            .build_with_queue_and_runner(QueueType::Multilevel(multilevel_builder), runner_builder);
        if let Some(task) = pending_task {
            pool.spawn(task);
        }
        pool
    }

    pub fn build_priority_pool(
        self,
        priority_provider: Arc<dyn priority::TaskPriorityProvider>,
    ) -> ThreadPool<TaskCell> {
        let name = self
            .name_prefix
            .clone()
            .unwrap_or_else(|| "yatp_pool".to_string());
        let priority_builder = priority::Builder::new(
            priority::Config::default()
                .name(Some(name))
                .cleanup_interval(self.cleanup_method.preferred_interval()),
            priority_provider,
        );
        let pending_task = self.try_spawn_cleanup(priority_builder.cleanup_fn());
        let (builder, read_pool_runner) = self.create_builder();
        let runner_builder = priority_builder.runner_builder(CloneRunnerBuilder(read_pool_runner));
        let pool = builder
            .build_with_queue_and_runner(QueueType::Priority(priority_builder), runner_builder);
        if let Some(task) = pending_task {
            pool.spawn(task);
        }
        pool
    }

    #[cfg(test)]
    fn background_cleanup_hook<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.background_cleanup_hook = Some(Arc::new(f));
        self
    }

    #[cfg(test)]
    fn try_spawn_cleanup<F>(&self, cleanup: F) -> Option<TaskCell>
    where
        F: Fn() -> Option<std::time::Instant> + Send + 'static,
    {
        if let Some(hook) = &self.background_cleanup_hook {
            let on_cleanup = hook.clone();
            self.cleanup_method.try_spawn(move || {
                on_cleanup();
                cleanup()
            })
        } else {
            self.cleanup_method.try_spawn(cleanup)
        }
    }

    #[cfg(not(test))]
    fn try_spawn_cleanup<F>(&self, cleanup: F) -> Option<TaskCell>
    where
        F: Fn() -> Option<std::time::Instant> + Send + 'static,
    {
        self.cleanup_method.try_spawn(cleanup)
    }

    fn create_builder(mut self) -> (yatp::Builder, YatpPoolRunner<T>) {
        let name = self.name_prefix.unwrap_or_else(|| "yatp_pool".to_string());
        let mut builder = yatp::Builder::new(thd_name!(name));
        builder
            .stack_size(self.stack_size)
            .min_thread_count(self.min_thread_count)
            .core_thread_count(self.core_thread_count)
            .max_thread_count(self.max_thread_count);

        let after_start = self.after_start.take();
        let before_stop = self.before_stop.take();
        let before_pause = self.before_pause.take();
        let schedule_wait_durations = if self.enable_task_wait_metrics {
            ["high", "medium", "low"].map(|p| {
                metrics::YATP_POOL_SCHEDULE_WAIT_DURATION_VEC.with_label_values(&[&name, p])
            })
        } else {
            std::array::from_fn(|_| Histogram::with_opts(HistogramOpts::new("_", "_")).unwrap())
        };
        let metric_idx_from_task_meta = self
            .metric_idx_from_task_meta
            .unwrap_or_else(|| Arc::new(|_| 0));
        let read_pool_runner = YatpPoolRunner::new(
            Default::default(),
            self.ticker.clone(),
            after_start,
            before_stop,
            before_pause,
            schedule_wait_durations,
            metric_idx_from_task_meta,
        );
        (builder, read_pool_runner)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{atomic, mpsc},
        thread,
    };

    use futures::compat::Future01CompatExt;

    use super::*;
    use crate::{timer::GLOBAL_TIMER_HANDLE, worker};

    #[test]
    fn test_record_schedule_wait_duration() {
        let name = "test_record_schedule_wait_duration";
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .enable_task_wait_metrics()
            .build_single_level_pool();
        let (tx, rx) = mpsc::channel();
        for _ in 0..3 {
            let tx = tx.clone();
            pool.spawn(async move {
                GLOBAL_TIMER_HANDLE
                    .delay(std::time::Instant::now() + Duration::from_millis(100))
                    .compat()
                    .await
                    .unwrap();
                tx.send(()).unwrap();
            });
        }
        for _ in 0..3 {
            rx.recv().unwrap();
        }
        // Drop the pool so the local metrics are flushed.
        drop(pool);
        let histogram =
            metrics::YATP_POOL_SCHEDULE_WAIT_DURATION_VEC.with_label_values(&[name, "high"]);
        assert_eq!(histogram.get_sample_count() as u32, 6, "{:?}", histogram);
    }

    #[test]
    fn test_cleanup_in_place_by_default() {
        let name = "test_cleanup_default";
        let count = Arc::new(atomic::AtomicU32::new(0));
        let n = count.clone();
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .background_cleanup_hook(move || {
                n.fetch_add(1, atomic::Ordering::SeqCst);
            })
            .build_multi_level_pool();

        thread::sleep(3 * DEFAULT_CLEANUP_INTERVAL);
        drop(pool);
        assert_eq!(0, count.load(atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_cleanup_in_local_pool() {
        let name = "test_cleanup_local";
        let count = Arc::new(atomic::AtomicU32::new(0));
        let n = count.clone();
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .cleanup_method(CleanupMethod::Local)
            .background_cleanup_hook(move || {
                n.fetch_add(1, atomic::Ordering::SeqCst);
                let t = thread::current();
                assert!(t.name().unwrap().starts_with(name));
            })
            .build_multi_level_pool();

        thread::sleep(3 * DEFAULT_CLEANUP_INTERVAL + DEFAULT_CLEANUP_INTERVAL / 2);
        drop(pool);
        thread::sleep(2 * DEFAULT_CLEANUP_INTERVAL);
        assert!(3 == count.load(atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_cleanup_in_remote_pool() {
        let name = "test_cleanup_remote";
        let bg_name = "test_background";
        let bg_pool = worker::Builder::new(bg_name).create();
        let count = Arc::new(atomic::AtomicU32::new(0));
        let n = count.clone();
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .cleanup_method(CleanupMethod::Remote(bg_pool.remote()))
            .background_cleanup_hook(move || {
                n.fetch_add(1, atomic::Ordering::SeqCst);
                let t = thread::current();
                assert!(t.name().unwrap().starts_with(bg_name));
            })
            .build_multi_level_pool();

        thread::sleep(3 * DEFAULT_CLEANUP_INTERVAL + DEFAULT_CLEANUP_INTERVAL / 2);
        drop(pool);
        thread::sleep(2 * DEFAULT_CLEANUP_INTERVAL);
        assert!(5 == count.load(atomic::Ordering::SeqCst));
        drop(bg_pool);
        thread::sleep(2 * DEFAULT_CLEANUP_INTERVAL);
        assert!(5 == count.load(atomic::Ordering::SeqCst));
    }
}

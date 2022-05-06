// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod future_pool;
mod metrics;
use std::sync::Arc;

use fail::fail_point;
pub use future_pool::{Full, FuturePool};
use yatp::{
    pool::{CloneRunnerBuilder, Local, Runner},
    queue::{multilevel, QueueType},
    task::future::{Runner as FutureRunner, TaskCell},
    ThreadPool,
};

use crate::{
    thread_group::GroupProperties,
    time::{Duration, Instant},
};

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

    pub fn try_tick(&mut self) {
        let now = Instant::now_coarse();
        if now.saturating_duration_since(self.last_tick_time) < tick_interval() {
            return;
        }
        self.last_tick_time = now;
        self.ticker.on_tick();
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
}

impl<T: PoolTicker> Runner for YatpPoolRunner<T> {
    type TaskCell = TaskCell;

    fn start(&mut self, local: &mut Local<Self::TaskCell>) {
        if let Some(props) = self.props.take() {
            crate::thread_group::set_properties(Some(props));
        }
        self.inner.start(local);
        if let Some(f) = self.after_start.take() {
            f();
        }
        tikv_alloc::add_thread_memory_accessor()
    }

    fn handle(&mut self, local: &mut Local<Self::TaskCell>, task_cell: Self::TaskCell) -> bool {
        let finished = self.inner.handle(local, task_cell);
        self.ticker.try_tick();
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
        tikv_alloc::remove_thread_memory_accessor()
    }
}

impl<T: PoolTicker> YatpPoolRunner<T> {
    pub fn new(
        inner: FutureRunner,
        ticker: TickerWrapper<T>,
        after_start: Option<Arc<dyn Fn() + Send + Sync>>,
        before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
        before_pause: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Self {
        YatpPoolRunner {
            inner,
            ticker,
            props: crate::thread_group::current_properties(),
            after_start,
            before_stop,
            before_pause,
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
        }
    }

    pub fn config(&mut self, config: Config) -> &mut Self {
        // TODO: maybe we should use (1, num_cpu) for min and max thread count.
        self.thread_count(config.workers, config.workers, config.workers)
            .stack_size(config.stack_size)
            .max_tasks(config.workers.saturating_mul(config.max_tasks_per_worker))
    }

    pub fn stack_size(&mut self, val: usize) -> &mut Self {
        self.stack_size = val;
        self
    }

    pub fn name_prefix(&mut self, val: impl Into<String>) -> &mut Self {
        let name = val.into();
        self.name_prefix = Some(name);
        self
    }

    pub fn thread_count(
        &mut self,
        min_thread_count: usize,
        core_thread_count: usize,
        max_thread_count: usize,
    ) -> &mut Self {
        self.min_thread_count = min_thread_count;
        self.core_thread_count = core_thread_count;
        self.max_thread_count = max_thread_count;
        self
    }

    pub fn max_tasks(&mut self, tasks: usize) -> &mut Self {
        self.max_tasks = tasks;
        self
    }

    pub fn before_stop<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.before_stop = Some(Arc::new(f));
        self
    }

    pub fn after_start<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.after_start = Some(Arc::new(f));
        self
    }

    pub fn before_pause<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.before_pause = Some(Arc::new(f));
        self
    }

    pub fn build_future_pool(&mut self) -> FuturePool {
        let pool = self.build_single_level_pool();
        let name = self.name_prefix.as_deref().unwrap_or("yatp_pool");
        FuturePool::from_pool(pool, name, self.core_thread_count, self.max_tasks)
    }

    pub fn build_single_level_pool(&mut self) -> ThreadPool<TaskCell> {
        let (builder, runner) = self.create_builder();
        builder.build_with_queue_and_runner(
            yatp::queue::QueueType::SingleLevel,
            yatp::pool::CloneRunnerBuilder(runner),
        )
    }

    pub fn build_multi_level_pool(&mut self) -> ThreadPool<TaskCell> {
        let (builder, read_pool_runner) = self.create_builder();
        let name = self.name_prefix.as_deref().unwrap_or("yatp_pool");
        let multilevel_builder =
            multilevel::Builder::new(multilevel::Config::default().name(Some(name)));
        let runner_builder =
            multilevel_builder.runner_builder(CloneRunnerBuilder(read_pool_runner));
        builder
            .build_with_queue_and_runner(QueueType::Multilevel(multilevel_builder), runner_builder)
    }

    fn create_builder(&mut self) -> (yatp::Builder, YatpPoolRunner<T>) {
        let mut builder = yatp::Builder::new(thd_name!(
            self.name_prefix.clone().unwrap_or_else(|| "".to_string())
        ));
        builder
            .stack_size(self.stack_size)
            .min_thread_count(self.min_thread_count)
            .core_thread_count(self.core_thread_count)
            .max_thread_count(self.max_thread_count);

        let after_start = self.after_start.take();
        let before_stop = self.before_stop.take();
        let before_pause = self.before_pause.take();
        let read_pool_runner = YatpPoolRunner::new(
            Default::default(),
            self.ticker.clone(),
            after_start,
            before_stop,
            before_pause,
        );
        (builder, read_pool_runner)
    }
}

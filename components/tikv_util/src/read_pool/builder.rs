// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::future_pool::FuturePool;
use super::read_pool::ReadPool;
use std::sync::Arc;
use yatp::pool::{CloneRunnerBuilder, Local, Runner};
use yatp::queue::{multilevel, QueueType};
use yatp::task::future::{Runner as FutureRunner, TaskCell};

pub trait PoolTicker: Send + Clone + 'static {
    fn on_tick(&mut self);
}

#[derive(Clone, Default)]
pub struct DefaultTicker {}

impl PoolTicker for DefaultTicker {
    fn on_tick(&mut self) {}
}

#[derive(Clone)]
pub struct ReadPoolRunner<T: PoolTicker> {
    inner: FutureRunner,
    ticker: T,
    after_start: Option<Arc<dyn Fn() + Send + Sync>>,
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    before_pause: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl<T: PoolTicker> Runner for ReadPoolRunner<T> {
    type TaskCell = TaskCell;

    fn start(&mut self, local: &mut Local<Self::TaskCell>) {
        if let Some(f) = self.after_start.take() {
            f();
        }
        self.inner.start(local);
        tikv_alloc::add_thread_memory_accessor()
    }

    fn handle(&mut self, local: &mut Local<Self::TaskCell>, task_cell: Self::TaskCell) -> bool {
        let finished = self.inner.handle(local, task_cell);
        self.ticker.on_tick();
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

impl<T: PoolTicker> ReadPoolRunner<T> {
    pub fn new(
        inner: FutureRunner,
        ticker: T,
        after_start: Option<Arc<dyn Fn() + Send + Sync>>,
        before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
        before_pause: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Self {
        ReadPoolRunner {
            inner,
            ticker,
            after_start,
            before_stop,
            before_pause,
        }
    }
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

pub struct ReadPoolBuilder<T: PoolTicker> {
    name_prefix: Option<String>,
    ticker: T,
    after_start: Option<Arc<dyn Fn() + Send + Sync>>,
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    before_pause: Option<Arc<dyn Fn() + Send + Sync>>,
    min_thread_count: usize,
    max_thread_count: usize,
    max_tasks: usize,
    stack_size: usize,
}

impl<T: PoolTicker> ReadPoolBuilder<T> {
    pub fn new(ticker: T) -> Self {
        Self {
            ticker,
            name_prefix: None,
            after_start: None,
            before_stop: None,
            before_pause: None,
            min_thread_count: 1,
            max_thread_count: 1,
            stack_size: 0,
            max_tasks: std::usize::MAX,
        }
    }

    pub fn config(&mut self, cfg: Config) -> &mut Self {
        self.max_thread_count = cfg.workers;
        self.max_tasks = cfg.workers.saturating_mul(cfg.max_tasks_per_worker);
        self.stack_size = cfg.stack_size;
        self
    }

    pub fn stack_size(&mut self, val: usize) -> &mut Self {
        self.stack_size = val;
        self
    }

    pub fn max_tasks(&mut self, val: usize) -> &mut Self {
        self.max_tasks = val;
        self
    }

    pub fn name_prefix(&mut self, val: impl Into<String>) -> &mut Self {
        let name = val.into();
        self.name_prefix = Some(name);
        self
    }

    pub fn thread_count(&mut self, min_thread_count: usize, max_thread_count: usize) -> &mut Self {
        self.min_thread_count = min_thread_count;
        self.max_thread_count = max_thread_count;
        self
    }

    pub fn pool_size(&mut self, pool_size: usize) -> &mut Self {
        self.min_thread_count = pool_size;
        self.max_thread_count = pool_size;
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
        let (builder, runner) = self.create_builder();
        let pool = builder.build_with_queue_and_runner(
            yatp::queue::QueueType::SingleLevel,
            yatp::pool::CloneRunnerBuilder(runner),
        );
        let name = if let Some(name) = &self.name_prefix {
            name.as_str()
        } else {
            "yatp_pool"
        };
        FuturePool::from_pool(pool, name, self.max_thread_count, self.max_tasks)
    }

    pub fn build_yatp_pool(&mut self) -> ReadPool {
        let (builder, read_pool_runner) = self.create_builder();
        let name = if let Some(name) = &self.name_prefix {
            name.as_str()
        } else {
            "yatp_pool"
        };
        let multilevel_builder =
            multilevel::Builder::new(multilevel::Config::default().name(Some(name)));
        let runner_builder =
            multilevel_builder.runner_builder(CloneRunnerBuilder(read_pool_runner));
        let pool = builder
            .build_with_queue_and_runner(QueueType::Multilevel(multilevel_builder), runner_builder);
        ReadPool::Yatp {
            pool,
            running_tasks: metrics::UNIFIED_READ_POOL_RUNNING_TASKS.with_label_values(&[name]),
            max_tasks: self.max_tasks,
        }
    }

    fn create_builder(&mut self) -> (yatp::Builder, ReadPoolRunner<T>) {
        let mut builder =
            yatp::Builder::new(self.name_prefix.clone().unwrap_or_else(|| "".to_string()));
        builder
            .stack_size(self.stack_size)
            .min_thread_count(self.min_thread_count)
            .max_thread_count(self.max_thread_count);

        let after_start = self.after_start.take();
        let before_stop = self.before_stop.take();
        let before_pause = self.before_pause.take();
        let read_pool_runner = ReadPoolRunner::new(
            Default::default(),
            self.ticker.clone(),
            after_start,
            before_stop,
            before_pause,
        );
        (builder, read_pool_runner)
    }
}
mod metrics {
    use prometheus::*;

    lazy_static! {
        pub static ref UNIFIED_READ_POOL_RUNNING_TASKS: IntGaugeVec = register_int_gauge_vec!(
            "tikv_unified_read_pool_running_tasks",
            "The number of running tasks in the unified read pool",
            &["name"]
        )
        .unwrap();
    }
}

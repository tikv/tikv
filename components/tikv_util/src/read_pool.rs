// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::future_pool::{self, FuturePool};
use futures::sync::oneshot;
use futures::{future, Future};
use kvproto::kvrpcpb::CommandPri;
use std::future::Future as StdFuture;
use std::sync::Arc;
use yatp::pool::{CloneRunnerBuilder, Local, Runner};
use yatp::queue::Extras;
use yatp::queue::{multilevel, QueueType};
use yatp::task::future::{Runner as FutureRunner, TaskCell};
use yatp::Remote;

use prometheus::IntGauge;

quick_error! {
    #[derive(Debug)]
    pub enum ReadPoolError {
        FuturePoolFull(err: future_pool::Full) {
            from()
            cause(err)
            display("{}", err)
        }
        UnifiedReadPoolFull {
            display("Unified read pool is full")
        }
        Canceled(err: oneshot::Canceled) {
            from()
            cause(err)
            display("{}", err)
        }
    }
}

pub trait PoolTicker: Send + Clone + 'static {
    fn on_tick(&mut self);
}

#[derive(Clone, Default)]
pub struct DefaultTicker {}

impl PoolTicker for DefaultTicker {
    fn on_tick(&mut self) {}
}

pub enum ReadPool {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        pool: yatp::ThreadPool<TaskCell>,
        running_tasks: IntGauge,
        max_tasks: usize,
    },
}

impl ReadPool {
    pub fn handle(&self) -> ReadPoolHandle {
        match self {
            ReadPool::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => ReadPoolHandle::FuturePools {
                read_pool_high: read_pool_high.clone(),
                read_pool_normal: read_pool_normal.clone(),
                read_pool_low: read_pool_low.clone(),
            },
            ReadPool::Yatp {
                pool,
                running_tasks,
                max_tasks,
            } => ReadPoolHandle::Yatp {
                remote: pool.remote().clone(),
                running_tasks: running_tasks.clone(),
                max_tasks: *max_tasks,
            },
        }
    }

    pub fn remote(&self) -> Option<Remote<TaskCell>> {
        match self {
            ReadPool::Yatp { pool, .. } => Some(pool.remote().clone()),
            ReadPool::FuturePools { .. } => None,
        }
    }
}

#[derive(Clone)]
pub enum ReadPoolHandle {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        remote: Remote<TaskCell>,
        running_tasks: IntGauge,
        max_tasks: usize,
    },
}

impl ReadPoolHandle {
    pub fn spawn<F>(&self, f: F, priority: CommandPri, task_id: u64) -> Result<(), ReadPoolError>
    where
        F: StdFuture<Output = ()> + Send + 'static,
    {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => {
                let pool = match priority {
                    CommandPri::High => read_pool_high,
                    CommandPri::Normal => read_pool_normal,
                    CommandPri::Low => read_pool_low,
                };

                pool.spawn(f)?;
            }
            ReadPoolHandle::Yatp {
                remote,
                running_tasks,
                max_tasks,
            } => {
                let running_tasks = running_tasks.clone();
                // Note that the running task number limit is not strict.
                // If several tasks are spawned at the same time while the running task number
                // is close to the limit, they may all pass this check and the number of running
                // tasks may exceed the limit.
                if running_tasks.get() as usize >= *max_tasks {
                    return Err(ReadPoolError::UnifiedReadPoolFull);
                }

                let fixed_level = match priority {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                let extras = Extras::new_multilevel(task_id, fixed_level);
                let task_cell = TaskCell::new(
                    async move {
                        running_tasks.inc();
                        f.await;
                        running_tasks.dec();
                    },
                    extras,
                );
                remote.spawn(task_cell);
            }
        }
        Ok(())
    }

    pub fn spawn_handle<F, T>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
    ) -> impl Future<Item = T, Error = ReadPoolError>
    where
        F: StdFuture<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<T>();
        let spawn_res = self.spawn(
            async move {
                let _ = tx.send(f.await);
            },
            priority,
            task_id,
        );
        if let Err(e) = spawn_res {
            future::Either::A(future::err(e))
        } else {
            future::Either::B(rx.map_err(ReadPoolError::from))
        }
    }
}

impl From<Vec<FuturePool>> for ReadPool {
    fn from(mut v: Vec<FuturePool>) -> ReadPool {
        assert_eq!(v.len(), 3);
        let read_pool_high = v.remove(2);
        let read_pool_normal = v.remove(1);
        let read_pool_low = v.remove(0);
        ReadPool::FuturePools {
            read_pool_high,
            read_pool_normal,
            read_pool_low,
        }
    }
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

pub struct ReadPoolBuilder<T: PoolTicker> {
    name_prefix: Option<String>,
    ticker: T,
    after_start: Option<Arc<dyn Fn() + Send + Sync>>,
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    before_pause: Option<Arc<dyn Fn() + Send + Sync>>,
    min_thread_count: usize,
    max_thread_count: usize,
    stack_size: usize,
    max_tasks: usize,
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

    pub fn stack_size(&mut self, val: usize) -> &mut Self {
        self.stack_size = val;
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

    pub fn build(&mut self) -> ReadPool {
        let name = if let Some(name) = &self.name_prefix {
            name.as_str()
        } else {
            "yatp_pool"
        };
        let mut builder = yatp::Builder::new(name);
        builder
            .stack_size(self.stack_size)
            .min_thread_count(self.min_thread_count)
            .max_thread_count(self.max_thread_count);
        let multilevel_builder =
            multilevel::Builder::new(multilevel::Config::default().name(Some(name)));
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
}

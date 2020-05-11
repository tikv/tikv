// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use yatp::pool::CloneRunnerBuilder;
use yatp::queue::QueueType;
use yatp::task::future;
use yatp::Builder as YatpBuilder;

use super::metrics::*;
use crate::future_pool::FuturePoolRunner;

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

pub struct Builder {
    name_prefix: Option<String>,
    on_tick: Option<Arc<dyn Fn() + Send + Sync>>,
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    after_start: Option<Arc<dyn Fn() + Send + Sync>>,
    // for accessing pool_size config since yatp doesn't offer such getter.
    pool_size: usize,
    stack_size: usize,
    max_tasks: usize,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            name_prefix: None,
            on_tick: None,
            before_stop: None,
            after_start: None,
            pool_size: 0,
            stack_size: 0,
            max_tasks: std::usize::MAX,
        }
    }

    pub fn from_config(config: Config) -> Self {
        let mut builder = Self::new();
        builder
            .pool_size(config.workers)
            .stack_size(config.stack_size)
            .max_tasks(config.workers.saturating_mul(config.max_tasks_per_worker));
        builder
    }

    pub fn pool_size(&mut self, val: usize) -> &mut Self {
        self.pool_size = val;
        self
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

    pub fn on_tick<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_tick = Some(Arc::new(f));
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

    pub fn max_tasks(&mut self, val: usize) -> &mut Self {
        self.max_tasks = val;
        self
    }

    pub fn build(&mut self) -> super::FuturePool {
        let name = if let Some(name) = &self.name_prefix {
            name.as_str()
        } else {
            "future_pool"
        };
        let env = super::Env {
            metrics_running_task_count: FUTUREPOOL_RUNNING_TASK_VEC.with_label_values(&[name]),
            metrics_handled_task_count: FUTUREPOOL_HANDLED_TASK_VEC.with_label_values(&[name]),
            metrics_pool_schedule_duration: FUTUREPOOL_SCHEDULE_DURATION_VEC
                .with_label_values(&[name]),
        };
        let runner = FuturePoolRunner {
            inner: future::Runner::default(),
            before_stop: self.before_stop.clone(),
            after_start: self.after_start.clone(),
            on_tick: self.on_tick.clone(),
            env: env.clone(),
        };
        // If zero `pool_size` or `stack_size` is passed in, yatp will keep its default
        // configuration unchanged.
        let pool = Arc::new(
            YatpBuilder::new(name)
                .max_thread_count(self.pool_size)
                .stack_size(self.stack_size)
                .build_with_queue_and_runner(QueueType::SingleLevel, CloneRunnerBuilder(runner)),
        );
        super::FuturePool {
            pool,
            env,
            pool_size: self.pool_size,
            max_tasks: self.max_tasks,
        }
    }
}

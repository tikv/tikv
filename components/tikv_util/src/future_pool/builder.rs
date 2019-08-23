// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tokio_threadpool::Builder as TokioBuilder;

use super::metrics::*;

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
    inner_builder: TokioBuilder,
    name_prefix: Option<String>,
    on_tick: Option<Box<dyn Fn() + Send + Sync>>,
    max_tasks: usize,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            inner_builder: TokioBuilder::new(),
            name_prefix: None,
            on_tick: None,
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
        self.inner_builder.pool_size(val);
        self
    }

    pub fn stack_size(&mut self, val: usize) -> &mut Self {
        self.inner_builder.stack_size(val);
        self
    }

    pub fn name_prefix(&mut self, val: impl Into<String>) -> &mut Self {
        let name = val.into();
        self.name_prefix = Some(name.clone());
        self.inner_builder.name_prefix(name);
        self
    }

    pub fn on_tick<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_tick = Some(Box::new(f));
        self
    }

    pub fn before_stop<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.inner_builder.before_stop(f);
        self
    }

    pub fn after_start<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.inner_builder.after_start(f);
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
        let env = Arc::new(super::Env {
            on_tick: self.on_tick.take(),
            metrics_running_task_count: FUTUREPOOL_RUNNING_TASK_VEC.with_label_values(&[name]),
            metrics_handled_task_count: FUTUREPOOL_HANDLED_TASK_VEC.with_label_values(&[name]),
        });
        let pool = Arc::new(self.inner_builder.build());
        super::FuturePool {
            pool,
            env,
            max_tasks: self.max_tasks,
        }
    }
}

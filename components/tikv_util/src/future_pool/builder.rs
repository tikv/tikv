// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tokio_threadpool::Builder as TokioBuilder;

use super::metrics::*;

pub struct Builder {
    inner_builder: TokioBuilder,
    name_prefix: Option<String>,
    after_start: Option<Box<dyn Fn() + Send + Sync>>,
    before_stop: Option<Box<dyn Fn() + Send + Sync>>,
    on_tick: Option<Box<dyn Fn() + Send + Sync>>,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            inner_builder: TokioBuilder::new(),
            name_prefix: None,
            after_start: None,
            before_stop: None,
            on_tick: None,
        }
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
        self.before_stop = Some(Box::new(f));
        self
    }

    pub fn after_start<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.after_start = Some(Box::new(f));
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
            metrics_pool_schedule_duration: FUTUREPOOL_SCHEDULE_DURATION_VEC
                .with_label_values(&[name]),
        });
        let before_stop = self.before_stop.take();
        let after_start = self.after_start.take();
        self.inner_builder.before_stop(Box::new(move || {
            if let Some(f) = &before_stop {
                f();
            }
            tikv_alloc::remove_thread_memory_accessor();
        }));
        self.inner_builder.after_start(Box::new(move || {
            tikv_alloc::add_thread_memory_accessor();
            if let Some(f) = &after_start {
                f();
            }
        }));
        let pool = Arc::new(self.inner_builder.build());
        super::FuturePool { pool, env }
    }
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::*;
use crate::stats::StatsMap;
use crate::task::{ArcTask, Task};
use crate::worker::Worker;
use crate::MultilevelPool;

use crossbeam::deque::{Injector, Steal, Stealer, Worker as LocalQueue};
use once_cell::sync::Lazy;
use prometheus::{IntCounter, IntGauge};
use rand::prelude::*;
use tikv_util::time::Instant;

use std::cell::Cell;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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
    on_tick: Option<Box<dyn Fn() + Send + Sync>>,
    after_start_func: Arc<dyn Fn() + Send + Sync + 'static>,
    stack_size: usize,
    max_tasks: usize,
    pool_size: usize,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            name_prefix: None,
            on_tick: None,
            after_start_func: Arc::new(|| {}),
            max_tasks: std::usize::MAX,
            stack_size: 2_000_000,
            pool_size: num_cpus::get(),
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
        self.name_prefix = Some(name.clone());
        self
    }

    pub fn on_tick<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_tick = Some(Box::new(f));
        self
    }

    pub fn after_start<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.after_start_func = Arc::new(f);
        self
    }

    pub fn max_tasks(&mut self, val: usize) -> &mut Self {
        self.max_tasks = val;
        self
    }

    pub fn build(&mut self) -> MultilevelPool {
        let name = if let Some(name) = &self.name_prefix {
            name.as_str()
        } else {
            "multilevel_pool"
        };
        let env = Arc::new(super::Env {
            on_tick: self.on_tick.take(),
            metrics_running_task_count: FUTUREPOOL_RUNNING_TASK_VEC.with_label_values(&[name]),
            metrics_handled_task_count: FUTUREPOOL_HANDLED_TASK_VEC.with_label_values(&[name]),
        });

        // Create workers
        let mut injectors = Vec::with_capacity(self.pool_size);
        let mut workers: Vec<Worker> = Vec::with_capacity(self.pool_size);
        for _ in 0..self.pool_size {
            let injector = Injector::new();
            injectors.push(injector);
        }
        let injectors: Arc<[Injector<ArcTask>]> = Arc::from(injectors);
        let mut local_queues = Vec::new();
        for _ in 0..self.pool_size {
            local_queues.push(LocalQueue::new_fifo());
        }
        let all_stealers: Vec<_> = local_queues.iter().map(|q| q.stealer()).collect();
        for (i, local) in local_queues.into_iter().enumerate() {
            let mut stealers = Vec::new();
            for j in 0..self.pool_size {
                if i != j {
                    stealers.push(all_stealers[j].clone());
                }
            }
            let worker = Worker {
                local,
                stealers,
                injectors: injectors.clone(),
                // parker: parker.clone(),
                after_start: self.after_start_func.clone(),
            };
            workers.push(worker);
        }
        for (i, worker) in workers.into_iter().enumerate() {
            let thread_builder = thread::Builder::new()
                .name(format!("{}-{}", name, i + 1))
                .stack_size(self.stack_size);
            worker.start(thread_builder);
        }
        MultilevelPool {
            injectors,
            stats: StatsMap::new(),
            // parker,
            env,
            pool_size: self.pool_size,
            max_tasks: self.max_tasks,
        }
    }
}

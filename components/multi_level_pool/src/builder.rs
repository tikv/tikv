// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::*;
use crate::park::Parker;
use crate::scheduler::Scheduler;
use crate::stats::StatsMap;
use crate::tokio_park::DefaultPark;
use crate::worker::{self, Proportions, Worker};
use crate::Env;
use crate::{MultiLevelPool, SpawnOption};

use crossbeam::deque::Worker as LocalQueue;
use crossbeam::sync::WaitGroup;
use tokio_timer::Timer;

use std::sync::Arc;
use std::thread;

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

    pub fn build(&mut self) -> MultiLevelPool {
        let name = if let Some(name) = &self.name_prefix {
            name.as_str()
        } else {
            "multi_level_pool"
        };

        let env = Arc::new(Env {
            on_tick: self.on_tick.take(),
            metrics_running_task_count: MULTI_LEVEL_POOL_RUNNING_TASK_VEC
                .with_label_values(&[name]),
            metrics_handled_task_count: MULTI_LEVEL_POOL_HANDLED_TASK_VEC
                .with_label_values(&[name]),
        });
        let task_source_count = TaskSourceCount::new(name);
        let scheduler = Scheduler::new(name, self.pool_size);
        let proportions = Proportions::new(name);
        let mut timer = Timer::new(DefaultPark::new());
        let timer_wg = WaitGroup::new();

        // Create workers
        let mut workers: Vec<Worker> = Vec::with_capacity(self.pool_size);
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
                scheduler: scheduler.clone(),
                timer_handle: timer.handle(),
                timer_wg: Some(timer_wg.clone()),
                parker: Parker::new(),
                proportions: proportions.clone(),
                task_source_count: task_source_count.clone(),
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

        // Create background jobs
        let async_update_proportions = worker::update_proportions(scheduler.clone(), proportions);
        let stats_map = StatsMap::new();
        let async_cleanup_stats = stats_map.async_cleanup();

        // Init tokio-timer
        thread::Builder::new()
            .name("multi_level_pool_timer".to_string())
            .spawn(move || loop {
                timer.turn(None).expect("multi level pool timer fails");
            })
            .expect("unable to spawn multi level pool timer thread");

        let pool = MultiLevelPool {
            scheduler,
            stats_map,
            env,
            pool_size: self.pool_size,
            max_tasks: self.max_tasks,
        };
        timer_wg.wait();

        // Spawn background jobs
        pool.spawn(
            async_update_proportions,
            SpawnOption {
                task_id: 0,
                fixed_level: Some(0),
            },
        )
        .expect("unable to spawn async update proportions task");
        pool.spawn(
            async_cleanup_stats,
            SpawnOption {
                task_id: 0,
                fixed_level: Some(0),
            },
        )
        .expect("unable to spawn async cleanup stats task");
        pool
    }
}

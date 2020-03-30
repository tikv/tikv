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
    // for accessing pool_size config since Tokio doesn't offer such getter.
    pool_size: usize,
    max_tasks: usize,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            inner_builder: TokioBuilder::new(),
            name_prefix: None,
            on_tick: None,
            pool_size: 0,
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
        bootstrap_worker_threads(&pool, self.pool_size, name);
        super::FuturePool {
            pool,
            env,
            max_tasks: self.max_tasks,
        }
    }
}

#[cfg(target_os = "linux")]
fn get_bootstrapped_num_threads(name: &str) -> Option<usize> {
    const THREAD_NAME_MAX_LEN: usize = 15;
    let prefix = if name.len() > THREAD_NAME_MAX_LEN {
        &name[0..THREAD_NAME_MAX_LEN]
    } else {
        name
    };
    let mut n = 0;
    let pid = unsafe { libc::getpid() };
    for tid in crate::metrics::get_thread_ids(pid).unwrap() {
        if let Ok(stat) = procinfo::pid::stat_task(pid, tid) {
            if !stat.command.starts_with(prefix) {
                continue;
            }
            n += 1;
        }
    }
    Some(n)
}

#[cfg(not(target_os = "linux"))]
fn get_bootstrapped_num_threads(_prefix: &str) -> Option<usize> {
    None
}

/// Tokio Threadpool starts threads lazily. This function ensure that all worker threads are
/// started.
fn bootstrap_worker_threads(pool: &tokio_threadpool::ThreadPool, size: usize, name: &str) {
    let actual_pool_size = if size > 0 {
        size
    } else {
        // According to https://docs.rs/tokio-threadpool/0.1.18/tokio_threadpool/struct.Builder.html#method.pool_size
        // by default the pool size is number of cpu cores.
        sysinfo::get_logical_cores()
    };

    let (sx, rx) = std::sync::mpsc::channel();
    let num_tasks = actual_pool_size * 2;

    info!("Bootstrapping worker threads"; "name" => name);

    for _ in 0..num_tasks {
        let sx2 = sx.clone();
        pool.spawn(futures::lazy(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            sx2.send(()).unwrap();
            Ok(())
        }))
    }

    for _ in 0..num_tasks {
        rx.recv().unwrap();
    }

    let current_size = get_bootstrapped_num_threads(name);
    info!(
        "Bootstrap worker threads finished";
        "name" => name,
        "current_threads" => ?current_size,
        "target_threads" => size
    );
}

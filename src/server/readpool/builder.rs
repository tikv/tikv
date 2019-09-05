// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::future_pool::Builder as FuturePoolBuilder;

use super::config::Config;

pub struct Builder {
    builder_low: FuturePoolBuilder,
    builder_normal: FuturePoolBuilder,
    builder_high: FuturePoolBuilder,
}

impl Builder {
    pub fn from_config(config: &Config) -> Self {
        let mut builder_low = FuturePoolBuilder::new();
        builder_low
            .pool_size(config.low_concurrency)
            .stack_size(config.stack_size.0 as usize)
            .max_tasks(config.max_tasks_per_worker_low * config.low_concurrency);

        let mut builder_normal = FuturePoolBuilder::new();
        builder_normal
            .pool_size(config.normal_concurrency)
            .stack_size(config.stack_size.0 as usize)
            .max_tasks(config.max_tasks_per_worker_normal * config.normal_concurrency);

        let mut builder_high = FuturePoolBuilder::new();
        builder_high
            .pool_size(config.high_concurrency)
            .stack_size(config.stack_size.0 as usize)
            .max_tasks(config.max_tasks_per_worker_high * config.high_concurrency);

        Builder {
            builder_low,
            builder_normal,
            builder_high,
        }
    }

    pub fn name_prefix(&mut self, name: impl AsRef<str>) -> &mut Self {
        let name = name.as_ref();
        self.builder_low.name_prefix(format!("{}-low", name));
        self.builder_normal.name_prefix(format!("{}-normal", name));
        self.builder_high.name_prefix(format!("{}-high", name));
        self
    }

    pub fn on_tick<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Clone + Send + Sync + 'static,
    {
        self.builder_low.on_tick(f.clone());
        self.builder_normal.on_tick(f.clone());
        self.builder_high.on_tick(f);
        self
    }

    pub fn before_stop<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Clone + Send + Sync + 'static,
    {
        self.builder_low.before_stop(f.clone());
        self.builder_normal.before_stop(f.clone());
        self.builder_high.before_stop(f);
        self
    }

    pub fn after_start<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Clone + Send + Sync + 'static,
    {
        self.builder_low.after_start(f.clone());
        self.builder_normal.after_start(f.clone());
        self.builder_high.after_start(f);
        self
    }

    pub fn build(&mut self) -> super::ReadPool {
        super::ReadPool {
            pool_low: self.builder_low.build(),
            pool_normal: self.builder_normal.build(),
            pool_high: self.builder_high.build(),
        }
    }

    pub fn build_for_test() -> super::ReadPool {
        Builder::from_config(&Config::default_for_test()).build()
    }
}

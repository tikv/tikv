// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use tikv_util::future_pool::Builder as FuturePoolBuilder;

use super::config::Config;

pub struct Builder<'a> {
    config: &'a Config,
    builder_low: FuturePoolBuilder,
    builder_normal: FuturePoolBuilder,
    builder_high: FuturePoolBuilder,
}

impl<'a> Builder<'a> {
    pub fn from_config(config: &'a Config) -> Self {
        let mut builder_low = FuturePoolBuilder::new();
        builder_low.pool_size(config.low_concurrency);
        builder_low.stack_size(config.stack_size.0 as usize);

        let mut builder_normal = FuturePoolBuilder::new();
        builder_normal.pool_size(config.normal_concurrency);
        builder_normal.stack_size(config.stack_size.0 as usize);

        let mut builder_high = FuturePoolBuilder::new();
        builder_high.pool_size(config.high_concurrency);
        builder_high.stack_size(config.stack_size.0 as usize);

        Builder {
            config,
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
            max_tasks_low: self.config.max_tasks_per_worker_low * self.config.low_concurrency,
            max_tasks_normal: self.config.max_tasks_per_worker_normal
                * self.config.normal_concurrency,
            max_tasks_high: self.config.max_tasks_per_worker_high * self.config.high_concurrency,
        }
    }

    pub fn build_for_test() -> super::ReadPool {
        Builder::from_config(&Config::default_for_test()).build()
    }
}

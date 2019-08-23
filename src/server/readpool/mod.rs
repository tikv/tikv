// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod builder;
pub mod config;
mod priority;

pub use self::builder::Builder;
pub use self::config::Config;
pub use self::priority::Priority;

use futures::Future;
use tikv_util::future_pool::{Full, FuturePool};
use tokio_threadpool::SpawnHandle;

/// A priority-aware thread pool for executing futures.
///
/// It is specifically used for all sorts of read operations like KV Get,
/// KV Scan and Coprocessor Read to improve performance.
#[derive(Clone)]
pub struct ReadPool {
    pool_high: FuturePool,
    pool_normal: FuturePool,
    pool_low: FuturePool,
}

impl tikv_util::AssertSend for ReadPool {}
impl tikv_util::AssertSync for ReadPool {}

impl ReadPool {
    #[inline]
    fn get_pool_by_priority(&self, priority: Priority) -> &FuturePool {
        match priority {
            Priority::High => &self.pool_high,
            Priority::Normal => &self.pool_normal,
            Priority::Low => &self.pool_low,
        }
    }

    pub fn spawn<F, R>(&self, priority: Priority, future_fn: F) -> Result<(), Full>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Future + Send + 'static,
        R::Item: Send + 'static,
        R::Error: Send + 'static,
    {
        self.get_pool_by_priority(priority).spawn(future_fn)
    }

    #[must_use]
    pub fn spawn_handle<F, R>(
        &self,
        priority: Priority,
        future_fn: F,
    ) -> Result<SpawnHandle<R::Item, R::Error>, Full>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Future + Send + 'static,
        R::Item: Send + 'static,
        R::Error: Send + 'static,
    {
        self.get_pool_by_priority(priority).spawn_handle(future_fn)
    }
}

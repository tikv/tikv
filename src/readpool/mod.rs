// Copyright 2018 PingCAP, Inc.
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

mod config;
mod pool;

use std::fmt;
use std::time;
use std::thread;
use futures::Future;
use futures_cpupool as cpupool;

use util;

pub use self::config::Config;

struct Context {}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Context").finish()
    }
}

impl pool::Context for Context {}

pub struct ReadPool {
    pool_read_critical: pool::Pool<Context>,
    pool_read_high: pool::Pool<Context>,
    pool_read_normal: pool::Pool<Context>,
    pool_read_low: pool::Pool<Context>,
}

impl util::AssertSend for ReadPool {}
impl util::AssertSync for ReadPool {}

impl Clone for ReadPool {
    fn clone(&self) -> ReadPool {
        ReadPool {
            pool_read_critical: self.pool_read_critical.clone(),
            pool_read_high: self.pool_read_high.clone(),
            pool_read_normal: self.pool_read_normal.clone(),
            pool_read_low: self.pool_read_low.clone(),
        }
    }
}

impl ReadPool {
    pub fn new(config: &Config) -> ReadPool {
        let tick_interval = time::Duration::from_secs(1);
        let build_context_factory = || |_thread_id: thread::ThreadId| Context {};
        ReadPool {
            pool_read_critical: pool::Pool::new(
                config.read_critical_concurrency,
                config.stack_size.0 as usize,
                "readpool-critical",
                tick_interval,
                build_context_factory(),
            ),
            pool_read_high: pool::Pool::new(
                config.read_high_concurrency,
                config.stack_size.0 as usize,
                "readpool-high",
                tick_interval,
                build_context_factory(),
            ),
            pool_read_normal: pool::Pool::new(
                config.read_normal_concurrency,
                config.stack_size.0 as usize,
                "readpool-normal",
                tick_interval,
                build_context_factory(),
            ),
            pool_read_low: pool::Pool::new(
                config.read_low_concurrency,
                config.stack_size.0 as usize,
                "readpool-low",
                tick_interval,
                build_context_factory(),
            ),
        }
    }

    #[inline]
    fn get_pool_by_priority(&self, priority: Priority) -> &pool::Pool<Context> {
        match priority {
            Priority::ReadCritical => &self.pool_read_critical,
            Priority::ReadHigh => &self.pool_read_high,
            Priority::ReadNormal => &self.pool_read_normal,
            Priority::ReadLow => &self.pool_read_low,
        }
    }

    pub fn future_execute<F, R>(
        &self,
        priority: Priority,
        future_factory: F,
    ) -> cpupool::CpuFuture<R::Item, R::Error>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Future + Send + 'static,
        R::Item: Send + 'static,
        R::Error: Send + 'static,
    {
        // TODO: handle busy?
        let pool = self.get_pool_by_priority(priority);
        pool.spawn(future_factory())
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Priority {
    ReadNormal,
    ReadLow,
    ReadHigh,
    ReadCritical,
}

#[cfg(test)]
mod tests {
    use std::error;
    use std::result;
    use std::fmt;
    use futures::{future, Future};

    pub use super::*;

    type BoxError = Box<error::Error + Send + Sync>;

    pub fn expect_val<T>(v: T, x: result::Result<T, BoxError>)
    where
        T: PartialEq + fmt::Debug + 'static,
    {
        assert!(x.is_ok());
        assert_eq!(x.unwrap(), v);
    }

    pub fn expect_err<T>(desc: &str, x: result::Result<T, BoxError>) {
        assert!(x.is_err());
        match x {
            Err(e) => assert_eq!(e.description(), desc),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_future_execute() {
        let read_pool = ReadPool::new(&Config::default());

        expect_val(
            vec![1, 2, 4],
            read_pool
                .future_execute(Priority::ReadCritical, || {
                    box future::ok::<Vec<u8>, BoxError>(vec![1, 2, 4])
                })
                .wait(),
        );

        expect_err(
            "foobar",
            read_pool
                .future_execute(Priority::ReadCritical, || {
                    box future::err::<(), BoxError>(box_err!("foobar"))
                })
                .wait(),
        );
    }
}

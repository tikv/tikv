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

use futures::Future;
use futures_cpupool as cpupool;

pub use self::config::Config;

pub struct WorkerThreadContext {
    // Metrics collector to be added here
}

impl Clone for WorkerThreadContext {
    fn clone(&self) -> WorkerThreadContext {
        WorkerThreadContext {}
    }
}

pub struct ReadPool {
    pool_read_critical: cpupool::CpuPool,
    pool_read_high: cpupool::CpuPool,
    pool_read_normal: cpupool::CpuPool,
    pool_read_low: cpupool::CpuPool,
    context: WorkerThreadContext,
}

impl Clone for ReadPool {
    fn clone(&self) -> ReadPool {
        ReadPool {
            pool_read_critical: self.pool_read_critical.clone(),
            pool_read_high: self.pool_read_high.clone(),
            pool_read_normal: self.pool_read_normal.clone(),
            pool_read_low: self.pool_read_low.clone(),
            context: self.context.clone(),
        }
    }
}

impl ReadPool {
    pub fn new(config: &Config) -> ReadPool {
        ReadPool {
            pool_read_critical: cpupool::Builder::new()
                .name_prefix("readpool-c")
                .pool_size(config.read_critical_concurrency)
                .stack_size(config.stack_size.0 as usize)
                .create(),
            pool_read_high: cpupool::Builder::new()
                .name_prefix("readpool-h")
                .pool_size(config.read_high_concurrency)
                .stack_size(config.stack_size.0 as usize)
                .create(),
            pool_read_normal: cpupool::Builder::new()
                .name_prefix("readpool-n")
                .pool_size(config.read_normal_concurrency)
                .stack_size(config.stack_size.0 as usize)
                .create(),
            pool_read_low: cpupool::Builder::new()
                .name_prefix("readpool-l")
                .pool_size(config.read_low_concurrency)
                .stack_size(config.stack_size.0 as usize)
                .create(),
            context: WorkerThreadContext {},
        }
    }

    #[inline]
    fn get_pool_by_priority(&self, priority: Priority) -> &cpupool::CpuPool {
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
        feature_factory: F,
    ) -> cpupool::CpuFuture<R::Item, R::Error>
    where
        F: FnOnce(&WorkerThreadContext) -> R + Send + 'static,
        R: Future + Send + 'static,
        R::Item: Send + 'static,
        R::Error: Send + 'static,
    {
        // TODO: handle busy?
        let pool = self.get_pool_by_priority(priority);
        pool.spawn(feature_factory(&self.context))
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
    use std::boxed;
    use std::result;
    use std::fmt;
    use std::sync::mpsc::{channel, Sender};
    use futures::{future, Future};

    pub use super::*;

    type BoxError = Box<error::Error + Send + Sync>;
    type FutureBoxedFn<T> =
        Box<boxed::FnBox(result::Result<T, BoxError>) -> future::FutureResult<(), ()>>;

    pub fn expect_val<T>(done: Sender<i32>, v: T, id: i32) -> FutureBoxedFn<T>
    where
        T: PartialEq + fmt::Debug + 'static,
    {
        box move |x: result::Result<T, BoxError>| {
            assert!(x.is_ok());
            assert_eq!(x.unwrap(), v);
            done.send(id).unwrap();
            future::ok(())
        }
    }

    pub fn expect_err<T>(done: Sender<i32>, desc: &str, id: i32) -> FutureBoxedFn<T> {
        let desc = desc.to_string();
        box move |x: result::Result<T, BoxError>| {
            assert!(x.is_err());
            match x {
                Err(e) => assert_eq!(e.description(), desc),
                _ => unreachable!(),
            }
            done.send(id).unwrap();
            future::ok(())
        }
    }

    #[test]
    fn test_future_execute() {
        let read_pool = ReadPool::new(&Config::default());

        let (tx, rx) = channel();
        read_pool
            .future_execute(Priority::ReadCritical, |_ctx| {
                box future::ok::<Vec<u8>, BoxError>(vec![1, 2, 4])
            })
            .then(expect_val(tx.clone(), vec![1, 2, 4], 0))
            .wait()
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 0);

        read_pool
            .future_execute(Priority::ReadCritical, |_ctx| {
                box future::err::<(), BoxError>(box_err!("foobar"))
            })
            .then(expect_err(tx.clone(), "foobar", 1))
            .wait()
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 1);
    }
}

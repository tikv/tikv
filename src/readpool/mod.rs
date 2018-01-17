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

mod task;

use std::sync;
use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};

use storage::Engine;
use server::Config;

pub use self::task::{BoxedFuture, Error, Priority, Result, Task, Value};
pub use self::task::kvget::*;

pub struct WorkerThreadContext {
    engine: Box<Engine>,
}

impl Clone for WorkerThreadContext {
    fn clone(&self) -> WorkerThreadContext {
        WorkerThreadContext {
            engine: self.engine.clone(),
        }
    }
}

pub struct ReadPool {
    pool_read_critical: CpuPool,
    pool_read_high: CpuPool,
    pool_read_normal: CpuPool,
    pool_read_low: CpuPool,
    context: sync::Arc<WorkerThreadContext>,
}

impl Clone for ReadPool {
    fn clone(&self) -> ReadPool {
        ReadPool {
            pool_read_critical: self.pool_read_critical.clone(),
            pool_read_high: self.pool_read_high.clone(),
            pool_read_normal: self.pool_read_normal.clone(),
            pool_read_low: self.pool_read_low.clone(),
            context: sync::Arc::clone(&self.context),
        }
    }
}

impl ReadPool {
    pub fn new(config: &Config, engine: Box<Engine>) -> ReadPool {
        ReadPool {
            pool_read_critical: CpuPoolBuilder::new()
                .name_prefix("readpool-c")
                .pool_size(config.readpool_read_critical_concurrency)
                // .stack_size(config.readpool_stack_size.0 as usize)
                .create(),
            pool_read_high: CpuPoolBuilder::new()
                .name_prefix("readpool-h")
                .pool_size(config.readpool_read_high_concurrency)
                // .stack_size(config.readpool_stack_size.0 as usize)
                .create(),
            pool_read_normal: CpuPoolBuilder::new()
                .name_prefix("readpool-n")
                .pool_size(config.readpool_read_normal_concurrency)
                // .stack_size(config.readpool_stack_size.0 as usize)
                .create(),
            pool_read_low: CpuPoolBuilder::new()
                .name_prefix("readpool-l")
                .pool_size(config.readpool_read_low_concurrency)
                // .stack_size(config.readpool_stack_size.0 as usize)
                .create(),
            context: sync::Arc::new(WorkerThreadContext { engine }),
        }
    }

    #[inline]
    fn get_pool_by_priority(&self, priority: Priority) -> &CpuPool {
        match priority {
            Priority::ReadCritical => &self.pool_read_critical,
            Priority::ReadHigh => &self.pool_read_high,
            Priority::ReadNormal => &self.pool_read_normal,
            Priority::ReadLow => &self.pool_read_low,
        }
    }

    // TODO: Support pool busy

    pub fn future_execute(&self, priority: Priority, mut task: Box<Task>) -> BoxedFuture {
        let pool = self.get_pool_by_priority(priority);
        box pool.spawn(task.build(&self.context))
    }
}

#[cfg(test)]
mod tests {
    use std::error;
    use std::boxed;
    use std::sync::mpsc::{channel, Sender};
    use futures::{future, Future};

    use storage;

    pub use super::*;

    pub fn expect_get_val(
        done: Sender<i32>,
        v: Vec<u8>,
        id: i32,
    ) -> Box<boxed::FnBox(Result) -> future::FutureResult<(), ()>> {
        box move |x: Result| {
            assert!(x.is_ok());
            match x.unwrap() {
                Value::StorageValue(val) => assert_eq!(val, Some(v)),
                _ => unreachable!(),
            }
            done.send(id).unwrap();
            future::ok(())
        }
    }

    pub fn expect_get_none(
        done: Sender<i32>,
        id: i32,
    ) -> Box<boxed::FnBox(Result) -> future::FutureResult<(), ()>> {
        box move |x: Result| {
            assert!(x.is_ok());
            match x.unwrap() {
                Value::StorageValue(val) => assert!(val.is_none()),
                _ => unreachable!(),
            }
            done.send(id).unwrap();
            future::ok(())
        }
    }

    pub fn expect_err(
        done: Sender<i32>,
        desc: &str,
        id: i32,
    ) -> Box<boxed::FnBox(Result) -> future::FutureResult<(), ()>> {
        let desc = desc.to_string();
        box move |x: Result| {
            assert!(x.is_err());
            match x {
                Err(e) => assert_eq!(error::Error::description(&e), desc),
                _ => unreachable!(),
            }
            done.send(id).unwrap();
            future::ok(())
        }
    }

    /// Make a `Value::StorageValue` success result for asserting
    pub fn make_value_result(v: Vec<u8>) -> Result {
        Ok(Value::StorageValue(Some(v)))
    }

    /// Make an `Error::Other` result for asserting
    pub fn make_err_result(desc: &str) -> Result {
        Err(Error::Other(box_err!(desc)))
    }

    #[test]
    fn test_future_execute() {
        struct FooTask {
            val: Option<Result>,
        }
        impl FooTask {
            fn new(val: Result) -> FooTask {
                FooTask { val: Some(val) }
            }
            fn from_value(v: Vec<u8>) -> FooTask {
                FooTask::new(make_value_result(v))
            }
            fn from_err(desc: &str) -> FooTask {
                FooTask::new(make_err_result(desc))
            }
        }
        impl Task for FooTask {
            fn build(&mut self, _context: &WorkerThreadContext) -> BoxedFuture {
                box future::result(self.val.take().unwrap())
            }
        }

        let storage_config = storage::Config::default();
        let storage = storage::Storage::new(&storage_config).unwrap();
        let read_pool = ReadPool::new(&Config::default(), storage.get_engine());

        let (tx, rx) = channel();
        read_pool
            .future_execute(
                Priority::ReadCritical,
                box FooTask::from_value(vec![1, 2, 4]),
            )
            .then(expect_get_val(tx.clone(), vec![1, 2, 4], 0))
            .wait()
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 0);

        read_pool
            .future_execute(Priority::ReadCritical, box FooTask::from_err("foobar"))
            .then(expect_err(tx.clone(), "foobar", 1))
            .wait()
            .unwrap();
        assert_eq!(rx.recv().unwrap(), 1);
    }

    // TODO: Test priority
}

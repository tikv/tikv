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
mod errors;

use std::{io, result, sync};

use util::threadpool::{self, ThreadPool, ThreadPoolBuilder};
use util::worker::{Runnable, ScheduleError, Scheduler, Worker};
use storage::Engine;
use server::Config;
use kvproto::kvrpcpb;

pub use self::errors::Error;
pub use self::task::{Callback, Priority, Result, SubTask, Task, Value};
pub use self::task::kvget::*;
pub use self::task::kvbatchget::*;
pub use self::task::cop::*;

pub fn map_pb_command_priority(priority: kvrpcpb::CommandPri) -> Priority {
    match priority {
        kvrpcpb::CommandPri::High => Priority::ReadHigh,
        kvrpcpb::CommandPri::Normal => Priority::ReadNormal,
        kvrpcpb::CommandPri::Low => Priority::ReadLow,
    }
}

struct WorkerThreadContextFactory {
    end_point_batch_row_limit: usize,
    end_point_recursion_limit: u32,
    engine: Box<Engine>,
}

impl Clone for WorkerThreadContextFactory {
    fn clone(&self) -> WorkerThreadContextFactory {
        WorkerThreadContextFactory {
            engine: self.engine.clone(),
            ..*self
        }
    }
}

impl threadpool::ContextFactory<WorkerThreadContext> for WorkerThreadContextFactory {
    fn create(&self) -> WorkerThreadContext {
        WorkerThreadContext {
            end_point_batch_row_limit: self.end_point_batch_row_limit,
            end_point_recursion_limit: self.end_point_recursion_limit,
            engine: self.engine.clone(),
        }
    }
}

pub struct WorkerThreadContext {
    end_point_batch_row_limit: usize,
    end_point_recursion_limit: u32,
    engine: Box<Engine>,
}

impl threadpool::Context for WorkerThreadContext {}

#[inline]
fn schedule_task(scheduler: &Scheduler<Task>, t: Task) {
    match scheduler.schedule(t) {
        Err(ScheduleError::Full(t)) => {
            let task_detail = format!("{}", t);
            (t.callback)(Err(Error::SchedulerBusy(task_detail)));
        }
        Err(ScheduleError::Stopped(t)) => {
            let task_detail = format!("{}", t);
            (t.callback)(Err(Error::SchedulerStopped(task_detail)));
        }
        Ok(_) => (),
    }
}

struct Runner {
    pool_read_critical: ThreadPool<WorkerThreadContext>,
    pool_read_high: ThreadPool<WorkerThreadContext>,
    pool_read_normal: ThreadPool<WorkerThreadContext>,
    pool_read_low: ThreadPool<WorkerThreadContext>,
    max_read_tasks: usize,

    scheduler: Scheduler<Task>,
}

impl Runner {
    /// Get a reference for the thread pool by the specified priority flag.
    #[inline]
    fn get_pool_by_priority(&self, priority: Priority) -> &ThreadPool<WorkerThreadContext> {
        match priority {
            Priority::ReadCritical => &self.pool_read_critical,
            Priority::ReadHigh => &self.pool_read_high,
            Priority::ReadNormal => &self.pool_read_normal,
            Priority::ReadLow => &self.pool_read_low,
        }
    }

    /// Check whether tasks in the pool exceeds the limit.
    #[inline]
    fn is_pool_busy(&self, pool: &ThreadPool<WorkerThreadContext>) -> bool {
        pool.get_task_count() >= self.max_read_tasks
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, mut t: Task) {
        let scheduler = self.scheduler.clone();
        let pool = self.get_pool_by_priority(t.priority);
        if self.is_pool_busy(pool) {
            let task_detail = format!("{}", t);
            (t.callback)(Err(Error::PoolBusy(task_detail)));
            return;
        }

        pool.execute(move |context: &mut WorkerThreadContext| {
            let subtask = t.subtask.take().unwrap();
            subtask.async_work(
                context,
                box move |result: task::SubTaskResult| match result {
                    task::SubTaskResult::Continue(new_subtask) => {
                        t.subtask = Some(new_subtask);
                        schedule_task(&scheduler, t);
                    }
                    task::SubTaskResult::Finish(result) => {
                        (t.callback)(result);
                    }
                },
            );
        });
    }

    fn shutdown(&mut self) {
        // Thread pools are built somewhere else while their ownerships are passed to the runner.
        // So the runner is responsible for destroying the thread pools.
        if let Err(e) = self.pool_read_critical.stop() {
            warn!("Stop pool_read_critical failed with {:?}", e);
        }
        if let Err(e) = self.pool_read_high.stop() {
            warn!("Stop pool_read_high failed with {:?}", e);
        }
        if let Err(e) = self.pool_read_normal.stop() {
            warn!("Stop pool_read_normal failed with {:?}", e);
        }
        if let Err(e) = self.pool_read_low.stop() {
            warn!("Stop pool_read_low failed with {:?}", e);
        }
    }
}

pub struct ReadPool {
    read_critical_concurrency: usize,
    read_high_concurrency: usize,
    read_normal_concurrency: usize,
    read_low_concurrency: usize,
    max_read_tasks: usize,
    stack_size: usize,

    end_point_batch_row_limit: usize,
    end_point_recursion_limit: u32,
    engine: Box<Engine>,

    /// `worker` is protected via a mutex to prevent concurrent mutable access
    /// (i.e. `worker.start()` & `worker.stop()`)
    worker: sync::Arc<sync::Mutex<Worker<Task>>>,

    /// `scheduler` is extracted from the `worker` so that we don't need to lock the worker
    /// (to get the `scheduler`) when pushing items into the queue.
    scheduler: Scheduler<Task>,
}

impl ReadPool {
    pub fn new(config: &Config, engine: Box<Engine>) -> ReadPool {
        let worker = Worker::new("readpool-schd");
        let scheduler = worker.scheduler();
        ReadPool {
            // Runner configurations
            read_critical_concurrency: config.readpool_read_critical_concurrency,
            read_high_concurrency: config.readpool_read_high_concurrency,
            read_normal_concurrency: config.readpool_read_normal_concurrency,
            read_low_concurrency: config.readpool_read_low_concurrency,
            max_read_tasks: config.readpool_max_read_tasks,
            stack_size: config.readpool_stack_size.0 as usize,

            // Available in runner thread contexts
            end_point_batch_row_limit: config.end_point_batch_row_limit,
            end_point_recursion_limit: config.end_point_recursion_limit,
            engine,

            // For the scheduler
            worker: sync::Arc::new(sync::Mutex::new(worker)),
            scheduler,
        }
    }

    /// Execute a task on the specified thread pool and get the result when it is finished.
    ///
    /// The caller should ensure the matching of the sub task and its priority, for example, for
    /// tasks about reading, the priority should be ReadXxx and the behavior is undefined if a
    /// WriteXxx priority is specified instead.
    pub fn async_execute(
        &self,
        begin_subtask: Box<SubTask>,
        priority: Priority,
        callback: Callback,
    ) {
        let t = Task {
            callback,
            subtask: Some(begin_subtask),
            priority,
        };
        schedule_task(&self.scheduler, t);
    }

    pub fn start(&mut self) -> result::Result<(), io::Error> {
        let thread_context_factory = WorkerThreadContextFactory {
            end_point_recursion_limit: self.end_point_recursion_limit,
            end_point_batch_row_limit: self.end_point_batch_row_limit,
            engine: self.engine.clone(),
        };
        let mut worker = self.worker.lock().unwrap();
        let runner = Runner {
            max_read_tasks: self.max_read_tasks,
            pool_read_critical: ThreadPoolBuilder::new(
                thd_name!("readpool-c"),
                thread_context_factory.clone(),
            ).thread_count(self.read_critical_concurrency)
                .stack_size(self.stack_size)
                .build(),
            pool_read_high: ThreadPoolBuilder::new(
                thd_name!("readpool-h"),
                thread_context_factory.clone(),
            ).thread_count(self.read_high_concurrency)
                .stack_size(self.stack_size)
                .build(),
            pool_read_normal: ThreadPoolBuilder::new(
                thd_name!("readpool-n"),
                thread_context_factory.clone(),
            ).thread_count(self.read_normal_concurrency)
                .stack_size(self.stack_size)
                .build(),
            pool_read_low: ThreadPoolBuilder::new(
                thd_name!("readpool-l"),
                thread_context_factory.clone(),
            ).thread_count(self.read_low_concurrency)
                .stack_size(self.stack_size)
                .build(),
            scheduler: self.scheduler.clone(),
        };
        worker.start(runner)
    }

    pub fn shutdown(&mut self) {
        let mut worker = self.worker.lock().unwrap();
        if let Err(e) = worker.stop().unwrap().join() {
            error!("failed to stop readpool: {:?}", e);
        }
    }
}

impl Clone for ReadPool {
    fn clone(&self) -> ReadPool {
        ReadPool {
            engine: self.engine.clone(),
            worker: self.worker.clone(),
            scheduler: self.scheduler.clone(),
            ..*self
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::error;
    use std::thread;
    use std::time::Duration;
    use storage;
    use kvproto::kvrpcpb;
    use super::*;
    use super::task::*;

    fn expect_ok(done: Sender<i32>, id: i32) -> Callback {
        Box::new(move |x: Result| {
            assert!(x.is_ok());
            done.send(id).unwrap();
        })
    }

    fn expect_get_none(done: Sender<i32>, id: i32) -> Callback {
        Box::new(move |x: Result| {
            assert!(x.is_ok());
            match x.unwrap() {
                Value::StorageValue(val) => assert_eq!(val, None),
                _ => unreachable!(),
            }
            done.send(id).unwrap();
        })
    }

    fn expect_get_val(done: Sender<i32>, v: Vec<u8>, id: i32) -> Callback {
        Box::new(move |x: Result| {
            assert!(x.is_ok());
            match x.unwrap() {
                Value::StorageValue(val) => assert_eq!(val, Some(v)),
                _ => unreachable!(),
            }
            done.send(id).unwrap();
        })
    }

    fn expect_err(done: Sender<i32>, desc: &str, id: i32) -> Callback {
        let desc = desc.to_string();
        Box::new(move |x: Result| {
            assert!(x.is_err());
            match x {
                Err(e) => assert_eq!(error::Error::description(&e), desc),
                _ => unreachable!(),
            }
            done.send(id).unwrap();
        })
    }

    /// Initialize a storage and create a new read pool wraps it
    fn new_read_pool(config: Config) -> ReadPool {
        let storage_config = storage::Config::default();
        let storage = storage::Storage::new(&storage_config).unwrap();
        ReadPool::new(&config, storage.get_engine())
    }

    /// Make a `Value::StorageValue` success result for asserting
    fn make_value_result(v: Vec<u8>) -> Result {
        Ok(Value::StorageValue(Some(v)))
    }

    /// Make an `Error::Other` result for asserting
    fn make_err_result(desc: &str) -> Result {
        Err(Error::Other(box_err!(desc)))
    }

    /// A dummy subtask that immediately returns the result
    // what itself is constructed
    #[derive(Debug)]
    struct Foo {
        val: Option<Result>,
    }
    impl SubTask for Foo {
        fn async_work(
            mut self: Box<Self>,
            _context: &mut WorkerThreadContext,
            on_done: SubTaskCallback,
        ) {
            on_done(SubTaskResult::Finish(self.val.take().unwrap()));
        }
    }
    impl Foo {
        fn new_value(v: Vec<u8>) -> Box<Foo> {
            box Foo {
                val: Some(make_value_result(v)),
            }
        }
        fn new_err(desc: &str) -> Box<Foo> {
            box Foo {
                val: Some(make_err_result(desc)),
            }
        }
    }

    /// A dummy subtask that immediately returns a `StorageValue` exactly as
    // what itself is constructed
    #[derive(Debug)]
    struct FooAsync {
        rx: Option<Receiver<Result>>,
    }
    impl SubTask for FooAsync {
        fn async_work(
            mut self: Box<Self>,
            _context: &mut WorkerThreadContext,
            on_done: SubTaskCallback,
        ) {
            let rx = self.rx.take().unwrap();
            thread::spawn(move || {
                let val = rx.recv().unwrap();
                on_done(SubTaskResult::Finish(val));
            });
        }
    }
    impl FooAsync {
        fn new(rx: Receiver<Result>) -> Box<FooAsync> {
            box FooAsync { rx: Some(rx) }
        }
    }

    /// Tests whether the runner handles `SubTaskResult::Finish(Ok(...))`
    #[test]
    fn test_subtask_finish_ok() {
        let (tx, rx) = channel();
        let mut read_pool = new_read_pool(Config::default());
        read_pool.start().unwrap();
        read_pool.async_execute(
            Foo::new_value(vec![1, 5, 12]),
            Priority::ReadCritical,
            expect_get_val(tx.clone(), vec![1, 5, 12], 0),
        );
        assert_eq!(rx.recv().unwrap(), 0);

        let (task_tx, task_rx) = channel();
        read_pool.async_execute(
            FooAsync::new(task_rx),
            Priority::ReadCritical,
            expect_get_val(tx.clone(), vec![3, 14, 15], 1),
        );
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        task_tx.send(make_value_result(vec![3, 14, 15])).unwrap();
        assert_eq!(rx.recv().unwrap(), 1);

        read_pool.shutdown();
    }

    /// Tests whether the runner handles `SubTaskResult::Finish(Err(...))`
    #[test]
    fn test_subtask_finish_err() {
        let (tx, rx) = channel();
        let mut read_pool = new_read_pool(Config::default());
        read_pool.start().unwrap();
        read_pool.async_execute(
            Foo::new_err("foobar"),
            Priority::ReadCritical,
            expect_err(tx.clone(), "foobar", 0),
        );
        assert_eq!(rx.recv().unwrap(), 0);

        let (task_tx, task_rx) = channel();
        read_pool.async_execute(
            FooAsync::new(task_rx),
            Priority::ReadCritical,
            expect_err(tx.clone(), "foobar2", 1),
        );
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        task_tx.send(make_err_result("foobar2")).unwrap();
        assert_eq!(rx.recv().unwrap(), 1);

        read_pool.shutdown();
    }

    /// Tests whether the runner handles `SubTaskResult::Continue(...)`
    #[test]
    fn test_subtask_continue() {
        #[derive(Debug)]
        struct Bar {
            val: Option<Vec<u8>>,
        }
        impl SubTask for Bar {
            fn async_work(
                mut self: Box<Self>,
                _context: &mut WorkerThreadContext,
                on_done: SubTaskCallback,
            ) {
                let new_val = self.val.unwrap().iter().map(|v| v * 2).collect();
                let new_subtask = Foo::new_value(new_val);
                on_done(SubTaskResult::Continue(new_subtask));
            }
        }
        impl Bar {
            fn new(v: Vec<u8>) -> Box<Bar> {
                box Bar { val: Some(v) }
            }
        }

        let (tx, rx) = channel();
        let mut read_pool = new_read_pool(Config::default());
        read_pool.start().unwrap();
        read_pool.async_execute(
            Bar::new(vec![1, 5, 12]),
            Priority::ReadCritical,
            expect_get_val(tx.clone(), vec![2, 10, 24], 0),
        );
        assert_eq!(rx.recv().unwrap(), 0);
        read_pool.shutdown();
    }

    /// Tests whether errors are raised when the thread pools are full
    #[test]
    fn test_pool_full() {
        let (tx, rx) = channel();
        let mut read_pool = new_read_pool(Config {
            readpool_read_high_concurrency: 1,
            readpool_max_read_tasks: 2,
            ..Config::default()
        });
        read_pool.start().unwrap();

        // schedule task 1
        let (task_tx_1, task_rx_1) = channel();
        read_pool.async_execute(
            FooAsync::new(task_rx_1),
            Priority::ReadHigh,
            expect_get_val(tx.clone(), vec![3, 2, 1], 1),
        );

        // schedule task 2
        let (task_tx_2, task_rx_2) = channel();
        read_pool.async_execute(
            FooAsync::new(task_rx_2),
            Priority::ReadHigh,
            expect_get_val(tx.clone(), vec![1, 2, 4], 2),
        );

        // schedule task 3 (busy)
        let (_, task_rx_3) = channel();
        read_pool.async_execute(
            FooAsync::new(task_rx_3),
            Priority::ReadHigh,
            expect_err(tx.clone(), "worker thread pool is busy", 3),
        );

        // task 3 callback is invoked first because of busy
        assert_eq!(rx.recv().unwrap(), 3);

        // finish task 2
        task_tx_2.send(make_value_result(vec![1, 2, 4])).unwrap();
        assert_eq!(rx.recv().unwrap(), 2);

        // schedule task 4
        let (task_tx_4, task_rx_4) = channel();
        read_pool.async_execute(
            FooAsync::new(task_rx_4),
            Priority::ReadHigh,
            expect_get_val(tx.clone(), vec![5, 1, 5], 4),
        );

        // schedule task 5 (busy)
        let (_, task_rx_5) = channel();
        read_pool.async_execute(
            FooAsync::new(task_rx_5),
            Priority::ReadHigh,
            expect_err(tx.clone(), "worker thread pool is busy", 5),
        );

        // finish task 1
        task_tx_1.send(make_value_result(vec![3, 2, 1])).unwrap();
        assert_eq!(rx.recv().unwrap(), 1);

        // finish task 4
        task_tx_4.send(make_value_result(vec![5, 1, 5])).unwrap();
        assert_eq!(rx.recv().unwrap(), 4);

        read_pool.shutdown();
    }
}

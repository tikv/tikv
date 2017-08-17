// Copyright 2017 PingCAP, Inc.
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

use crossbeam::sync::MsQueue;

use std::usize;
use std::time;
use std::sync::Arc;
use std::thread::{sleep, Builder, JoinHandle};
use std::boxed::FnBox;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::fmt::Write;

const WORKER_WAIT_TIME: u64 = 20; // ms

pub trait Context: Send {
    fn on_task_started(&mut self, worker_ctx: &mut Self);
    fn on_task_finished(&mut self, worker_ctx: &mut Self);
}

pub trait ContextFactory<Ctx: Context> {
    fn create_context(&self) -> Ctx;
}

pub struct Task<C> {
    // use Box<FnBox<&mut C> + Send> instead
    // after https://github.com/rust-lang/rust/issues/25647 solved.
    task: Box<FnBox(C) -> C + Send>,
    ctx: C,
}

impl<C: Context> Task<C> {
    fn new<F>(job: F, ctx: C) -> Task<C>
    where
        F: FnOnce(C) -> C + Send + 'static,
    {
        Task {
            task: Box::new(job),
            ctx: ctx,
        }
    }
}

/// `ThreadPool` is used to execute tasks in parallel.
/// Each task would be pushed into the pool, and when a thread
/// is ready to process a task, it will get a task from the pool
/// according to the `ScheduleQueue` provided in initialization.
pub struct ThreadPool<C, Ctx> {
    stop_flag: Arc<AtomicBool>,
    task_queue: Arc<MsQueue<Task<Ctx>>>,
    threads: Vec<JoinHandle<()>>,
    task_count: Arc<AtomicUsize>,
    // ctx_factory should only be used in one thread
    ctx_factory: C,
}

impl<C, Ctx> ThreadPool<C, Ctx>
where
    Ctx: Context + 'static,
    C: ContextFactory<Ctx>,
{
    pub fn new(name: String, num_threads: usize, f: C) -> ThreadPool<C, Ctx> {
        assert!(num_threads >= 1);
        let task_queue = Arc::new(MsQueue::new());
        let mut threads = Vec::with_capacity(num_threads);
        let task_count = Arc::new(AtomicUsize::new(0));
        let stop_flag = Arc::new(AtomicBool::new(false));
        // Threadpool threads
        for _ in 0..num_threads {
            let tasks = task_queue.clone();
            let task_num = task_count.clone();
            let ctx = f.create_context();
            let stop = stop_flag.clone();
            let thread = Builder::new()
                .name(name.clone())
                .spawn(move || {
                    let mut worker = Worker::new(tasks, task_num, stop, ctx);
                    worker.run();
                })
                .unwrap();
            threads.push(thread);
        }

        ThreadPool {
            stop_flag: stop_flag,
            task_queue: task_queue,
            threads: threads,
            task_count: task_count,
            ctx_factory: f,
        }
    }

    pub fn execute<F>(&mut self, job: F)
    where
        F: FnOnce(Ctx) -> Ctx + Send + 'static,
        Ctx: Context,
    {
        assert!(!self.stop_flag.load(AtomicOrdering::SeqCst));
        let ctx = self.ctx_factory.create_context();
        let task = Task::new(job, ctx);
        self.task_queue.push(task);
        self.task_count.fetch_add(1, AtomicOrdering::SeqCst);
    }

    #[inline]
    pub fn get_task_count(&self) -> usize {
        self.task_count.load(AtomicOrdering::SeqCst)
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.stop_flag.store(true, AtomicOrdering::SeqCst);
        while !self.task_queue.is_empty() {
            let _ = self.task_queue.try_pop();
        }
        let mut err_msg = String::new();
        for t in self.threads.drain(..) {
            if let Err(e) = t.join() {
                write!(&mut err_msg, "Failed to join thread with err: {:?};", e).unwrap();
            }
        }
        if !err_msg.is_empty() {
            return Err(err_msg);
        }
        Ok(())
    }
}

// Each thread has a worker.
struct Worker<C> {
    stop_flag: Arc<AtomicBool>,
    task_queue: Arc<MsQueue<Task<C>>>,
    task_count: Arc<AtomicUsize>,
    ctx: C,
}

impl<C> Worker<C>
where
    C: Context,
{
    fn new(
        task_queue: Arc<MsQueue<Task<C>>>,
        task_count: Arc<AtomicUsize>,
        stop_flag: Arc<AtomicBool>,
        ctx: C,
    ) -> Worker<C> {
        Worker {
            stop_flag: stop_flag,
            task_queue: task_queue,
            task_count: task_count,
            ctx: ctx,
        }
    }

    fn run(&mut self) {
        while !self.stop_flag.load(AtomicOrdering::SeqCst) {
            match self.task_queue.try_pop() {
                None => {
                    sleep(time::Duration::from_millis(WORKER_WAIT_TIME));
                }
                Some(mut t) => {
                    t.ctx.on_task_started(&mut self.ctx);
                    t.ctx = (t.task)(t.ctx);
                    t.ctx.on_task_finished(&mut self.ctx);
                    self.task_count.fetch_sub(1, AtomicOrdering::SeqCst);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Context, ContextFactory, ThreadPool};
    use std::time::Duration;
    use std::sync::mpsc::{channel, Sender};
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicIsize, Ordering};

    #[derive(Clone)]
    struct DummyContext {}

    unsafe impl Send for DummyContext {}

    impl Context for DummyContext {
        fn on_task_started(&mut self, _: &mut DummyContext) {}
        fn on_task_finished(&mut self, _: &mut DummyContext) {}
    }

    struct DummyContextFactory {}

    impl ContextFactory<DummyContext> for DummyContextFactory {
        fn create_context(&self) -> DummyContext {
            DummyContext {}
        }
    }

    #[test]
    fn test_get_task_count() {
        let name = thd_name!("test_get_task_count");
        let concurrency = 1;
        let f = DummyContextFactory {};
        let mut task_pool = ThreadPool::new(name, concurrency, f);
        let (tx, rx) = channel();
        let (ftx, frx) = channel();
        let receiver = Arc::new(Mutex::new(rx));
        let timeout = Duration::from_secs(2);
        let group_num = 4;
        let mut task_num = 0;
        for gid in 0..group_num {
            let rxer = receiver.clone();
            let ftx = ftx.clone();
            task_pool.execute(move |ctx: DummyContext| -> DummyContext {
                let rx = rxer.lock().unwrap();
                let id = rx.recv_timeout(timeout).unwrap();
                assert_eq!(id, gid);
                ftx.send(true).unwrap();
                ctx
            });
            task_num += 1;
            assert_eq!(task_pool.get_task_count(), task_num);
        }

        for gid in 0..group_num {
            tx.send(gid).unwrap();
            frx.recv_timeout(timeout).unwrap();
            let left_num = task_pool.get_task_count();
            // current task may be still running.
            assert!(
                left_num == task_num || left_num == task_num - 1,
                format!("left_num {},task_num {}", left_num, task_num)
            );
            task_num -= 1;
        }
        task_pool.stop().unwrap();
    }

    #[test]
    fn test_task_context() {
        struct TestContext {
            counter: Arc<AtomicIsize>,
            tx: Sender<isize>,
        }

        unsafe impl Send for TestContext {}

        impl Context for TestContext {
            fn on_task_started(&mut self, _: &mut Self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }
            fn on_task_finished(&mut self, _: &mut Self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
                self.tx.send(self.counter.load(Ordering::SeqCst)).unwrap();
            }
        }

        struct TestContextFactory {
            counter: Arc<AtomicIsize>,
            tx: Sender<isize>,
        }

        impl ContextFactory<TestContext> for TestContextFactory {
            fn create_context(&self) -> TestContext {
                TestContext {
                    counter: self.counter.clone(),
                    tx: self.tx.clone(),
                }
            }
        }

        let (tx, rx) = channel();

        let f = TestContextFactory {
            counter: Arc::new(AtomicIsize::new(0)),
            tx: tx,
        };

        let name = thd_name!("test_tasks_with_contexts");
        let concurrency = 5;
        let mut task_pool = ThreadPool::new(name, concurrency, f);

        for _ in 0..10 {
            task_pool.execute(move |ctx: TestContext| -> TestContext { ctx });
        }
        let mut fin: isize = -1;
        let mut count = 0;
        while count != 10 {
            fin = rx.recv_timeout(Duration::from_millis(20)).unwrap();
            count += 1;
        }
        task_pool.stop().unwrap();
        assert_eq!(fin, 20);
    }
}

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

use std::usize;
use std::time;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{Builder, JoinHandle};
use std::boxed::FnBox;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::fmt::Write;

pub const DEFAULT_TASKS_PER_TICK: usize = 500;
const WORKER_WAIT_TIME: u64 = 500; // ms
const DEFAULT_QUEUE_CAPACITY: usize = 1000;
const QUEUE_MAX_CAPACITY: usize = 8 * DEFAULT_QUEUE_CAPACITY;

pub trait Context: Send {
    fn on_task_started(&mut self);
    fn on_task_finished(&mut self);
    fn on_tick(&mut self);
}

pub trait ContextFactory<Ctx: Context> {
    fn create(&self) -> Ctx;
}

pub struct Task<C> {
    task: Box<FnBox(&mut C) + Send>,
}

impl<C: Context> Task<C> {
    fn new<F>(job: F) -> Task<C>
    where
        for<'r> F: FnOnce(&'r mut C) + Send + 'static,
    {
        Task {
            task: Box::new(job),
        }
    }
}

// First in first out queue.
pub struct FifoQueue<C> {
    queue: VecDeque<Task<C>>,
}

impl<C: Context> FifoQueue<C> {
    fn new() -> FifoQueue<C> {
        FifoQueue {
            queue: VecDeque::with_capacity(DEFAULT_QUEUE_CAPACITY),
        }
    }

    fn push(&mut self, task: Task<C>) {
        self.queue.push_back(task);
    }

    fn pop(&mut self) -> Option<Task<C>> {
        let task = self.queue.pop_front();

        if self.queue.is_empty() && self.queue.capacity() > QUEUE_MAX_CAPACITY {
            self.queue = VecDeque::with_capacity(DEFAULT_QUEUE_CAPACITY);
        }

        task
    }
}

/// `ThreadPool` is used to execute tasks in parallel.
/// Each task would be pushed into the pool, and when a thread
/// is ready to process a task, it will get a task from the pool
/// according to the `ScheduleQueue` provided in initialization.
pub struct ThreadPool<Ctx> {
    stop_flag: Arc<AtomicBool>,
    task_pool: Arc<(Mutex<FifoQueue<Ctx>>, Condvar)>,
    threads: Vec<JoinHandle<()>>,
    task_count: Arc<AtomicUsize>,
}

impl<Ctx> ThreadPool<Ctx>
where
    Ctx: Context + 'static,
{
    pub fn new<C: ContextFactory<Ctx>>(
        name: String,
        num_threads: usize,
        tasks_per_tick: usize,
        f: C,
    ) -> ThreadPool<Ctx> {
        assert!(num_threads >= 1);
        let task_pool = Arc::new((Mutex::new(FifoQueue::new()), Condvar::new()));
        let mut threads = Vec::with_capacity(num_threads);
        let task_count = Arc::new(AtomicUsize::new(0));
        let stop_flag = Arc::new(AtomicBool::new(false));
        // Threadpool threads
        for _ in 0..num_threads {
            let tasks = task_pool.clone();
            let task_num = task_count.clone();
            let ctx = f.create();
            let stop = stop_flag.clone();
            let thread = Builder::new()
                .name(name.clone())
                .spawn(move || {
                    let mut worker = Worker::new(tasks, task_num, tasks_per_tick, stop, ctx);
                    worker.run();
                })
                .unwrap();
            threads.push(thread);
        }

        ThreadPool {
            task_pool: task_pool,
            stop_flag: stop_flag,
            threads: threads,
            task_count: task_count,
        }
    }

    pub fn execute<F>(&mut self, job: F)
    where
        F: FnOnce(&mut Ctx) + Send + 'static,
        Ctx: Context,
    {
        if self.stop_flag.load(AtomicOrdering::SeqCst) {
            return;
        }
        let task = Task::new(job);
        let &(ref lock, ref cvar) = &*self.task_pool;
        let mut queue = lock.lock().unwrap();
        queue.push(task);
        cvar.notify_one();
        self.task_count.fetch_add(1, AtomicOrdering::SeqCst);
    }

    #[inline]
    pub fn get_task_count(&self) -> usize {
        self.task_count.load(AtomicOrdering::SeqCst)
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.stop_flag.store(true, AtomicOrdering::SeqCst);
        let &(_, ref cvar) = &*self.task_pool;
        cvar.notify_all();
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
    task_queue: Arc<(Mutex<FifoQueue<C>>, Condvar)>,
    task_count: Arc<AtomicUsize>,
    tasks_per_tick: usize,
    task_counter: usize,
    ctx: C,
}

impl<C> Worker<C>
where
    C: Context,
{
    fn new(
        task_queue: Arc<(Mutex<FifoQueue<C>>, Condvar)>,
        task_count: Arc<AtomicUsize>,
        tasks_per_tick: usize,
        stop_flag: Arc<AtomicBool>,
        ctx: C,
    ) -> Worker<C> {
        Worker {
            stop_flag: stop_flag,
            task_queue: task_queue,
            task_count: task_count,
            tasks_per_tick: tasks_per_tick,
            task_counter: 0,
            ctx: ctx,
        }
    }

    fn get_task_timeout(&mut self, timeout: time::Duration) -> Option<Task<C>> {
        let &(ref lock, ref cvar) = &*self.task_queue;
        let mut task_queue = lock.lock().unwrap();

        if let Some(task) = task_queue.pop() {
            return Some(task);
        }
        let (mut q, _) = cvar.wait_timeout(task_queue, timeout).unwrap();
        q.pop()
    }

    fn run(&mut self) {
        while !self.stop_flag.load(AtomicOrdering::SeqCst) {
            if let Some(t) = self.get_task_timeout(time::Duration::from_millis(WORKER_WAIT_TIME)) {
                self.ctx.on_task_started();
                (t.task).call_once((&mut self.ctx,));
                self.ctx.on_task_finished();
                self.task_count.fetch_sub(1, AtomicOrdering::SeqCst);
                self.task_counter += 1;
                if self.task_counter == self.tasks_per_tick {
                    self.task_counter = 0;
                    self.ctx.on_tick();
                }
            } else {
                self.task_counter = 0;
                self.ctx.on_tick();
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Context, ContextFactory, ThreadPool, DEFAULT_TASKS_PER_TICK};
    use std::time::Duration;
    use std::sync::mpsc::{channel, Sender};
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicIsize, Ordering};

    #[derive(Clone)]
    struct DummyContext {}

    unsafe impl Send for DummyContext {}

    impl Context for DummyContext {
        fn on_task_started(&mut self) {}
        fn on_task_finished(&mut self) {}
        fn on_tick(&mut self) {}
    }

    struct DummyContextFactory {}

    impl ContextFactory<DummyContext> for DummyContextFactory {
        fn create(&self) -> DummyContext {
            DummyContext {}
        }
    }

    #[test]
    fn test_get_task_count() {
        let name = thd_name!("test_get_task_count");
        let concurrency = 1;
        let f = DummyContextFactory {};
        let mut task_pool = ThreadPool::new(name, concurrency, DEFAULT_TASKS_PER_TICK, f);
        let (tx, rx) = channel();
        let (ftx, frx) = channel();
        let receiver = Arc::new(Mutex::new(rx));
        let timeout = Duration::from_secs(2);
        let group_num = 4;
        let mut task_num = 0;
        for gid in 0..group_num {
            let rxer = receiver.clone();
            let ftx = ftx.clone();
            task_pool.execute(move |_: &mut DummyContext| {
                let rx = rxer.lock().unwrap();
                let id = rx.recv_timeout(timeout).unwrap();
                assert_eq!(id, gid);
                ftx.send(true).unwrap();
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
            fn on_task_started(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }
            fn on_task_finished(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
                self.tx.send(self.counter.load(Ordering::SeqCst)).unwrap();
            }
            fn on_tick(&mut self) {}
        }

        struct TestContextFactory {
            counter: Arc<AtomicIsize>,
            tx: Sender<isize>,
        }

        impl ContextFactory<TestContext> for TestContextFactory {
            fn create(&self) -> TestContext {
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
        let mut task_pool = ThreadPool::new(name, concurrency, DEFAULT_TASKS_PER_TICK, f);

        for _ in 0..10 {
            task_pool.execute(move |_: &mut TestContext| {});
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

    #[test]
    fn test_task_tick() {
        struct TestContext {
            counter: Arc<AtomicIsize>,
            tx: Sender<isize>,
        }

        unsafe impl Send for TestContext {}

        impl Context for TestContext {
            fn on_task_started(&mut self) {}
            fn on_task_finished(&mut self) {}
            fn on_tick(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
                self.tx.send(self.counter.load(Ordering::SeqCst)).unwrap();
            }
        }

        struct TestContextFactory {
            counter: Arc<AtomicIsize>,
            tx: Sender<isize>,
        }

        impl ContextFactory<TestContext> for TestContextFactory {
            fn create(&self) -> TestContext {
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

        let name = thd_name!("test_tasks_tick");
        let concurrency = 1;
        let mut task_pool = ThreadPool::new(name, concurrency, 1, f);

        for _ in 0..10 {
            task_pool.execute(move |_: &mut TestContext| {});
        }
        let mut fin: isize = -1;
        for _ in 0..10 {
            fin = rx.recv_timeout(Duration::from_millis(20)).unwrap();
        }
        task_pool.stop().unwrap();
        assert_eq!(fin, 10);
    }
}

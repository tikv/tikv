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
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{Builder, JoinHandle};
use std::boxed::FnBox;
use std::collections::VecDeque;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::fmt::{self, Debug, Formatter, Write};
use std::sync::mpsc::{channel, Receiver, Sender};

const DEFAULT_QUEUE_CAPACITY: usize = 1000;
const QUEUE_MAX_CAPACITY: usize = 8 * DEFAULT_QUEUE_CAPACITY;

pub struct Task<C> {
    // The task's id in the pool. Each task has a unique id,
    // and it's always bigger than preceding ones.
    id: u64,

    // use Box<FnBox<&mut C> + Send> instead
    // after https://github.com/rust-lang/rust/issues/25647 solved.
    task: Box<FnBox(C) -> C + Send>,
    ctx: C,
}

impl<C> Debug for Task<C> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "task_id:{}", self.id)
    }
}

impl<C: Context> Task<C> {
    fn new<F>(job: F, ctx: C) -> Task<C>
    where
        F: FnOnce(C) -> C + Send + 'static,
    {
        Task {
            id: 0,
            task: Box::new(job),
            ctx: ctx,
        }
    }
}

impl<C> Ord for Task<C> {
    fn cmp(&self, right: &Task<C>) -> Ordering {
        self.id.cmp(&right.id).reverse()
    }
}

impl<C> PartialEq for Task<C> {
    fn eq(&self, right: &Task<C>) -> bool {
        self.cmp(right) == Ordering::Equal
    }
}

impl<C> Eq for Task<C> {}

impl<C> PartialOrd for Task<C> {
    fn partial_cmp(&self, rhs: &Task<C>) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}

pub trait ScheduleQueue<C> {
    fn pop(&mut self) -> Option<Task<C>>;
    fn push(&mut self, task: Task<C>);
}

// First in first out queue.
#[derive(Default)]
pub struct FifoQueue<C> {
    queue: VecDeque<Task<C>>,
}

impl<C: Context> FifoQueue<C> {
    pub fn new() -> FifoQueue<C> {
        FifoQueue {
            queue: VecDeque::with_capacity(DEFAULT_QUEUE_CAPACITY),
        }
    }
}

impl<C> ScheduleQueue<C> for FifoQueue<C> {
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

struct TaskPool<Q, C> {
    next_task_id: u64,
    task_queue: Q,
    stop: bool,
    jobs: Receiver<Task<C>>,
}

impl<Q, C> TaskPool<Q, C>
where
    Q: ScheduleQueue<C>,
    C: Context,
{
    fn new(queue: Q, jobs: Receiver<Task<C>>) -> TaskPool<Q, C> {
        TaskPool {
            next_task_id: 0,
            task_queue: queue,
            stop: false,
            jobs: jobs,
        }
    }

    fn pop_task(&mut self) -> Option<Task<C>> {
        if let Some(task) = self.task_queue.pop() {
            return Some(task);
        }
        // try fill queue when queue is empty.
        self.try_fill_queue();
        self.task_queue.pop()
    }

    fn try_fill_queue(&mut self) {
        while let Ok(mut task) = self.jobs.try_recv() {
            task.id = self.next_task_id;
            self.next_task_id += 1;
            self.task_queue.push(task);
        }
    }

    #[inline]
    fn stop(&mut self) {
        self.stop = true;
    }

    #[inline]
    fn is_stopped(&self) -> bool {
        self.stop
    }
}

pub trait Context: Send {
    fn on_task_started(&mut self, worker_ctx: &mut Self);
    fn on_task_finished(&mut self, worker_ctx: &mut Self);
}

pub trait ContextFactory<Ctx: Context> {
    fn create_context(&self) -> Ctx;
}

// Make clippy happy
type TTaskPool<Q, C> = Arc<(Mutex<TaskPool<Q, C>>, Condvar)>;

/// `ThreadPool` is used to execute tasks in parallel.
/// Each task would be pushed into the pool, and when a thread
/// is ready to process a task, it will get a task from the pool
/// according to the `ScheduleQueue` provided in initialization.
pub struct ThreadPool<Q, C, Ctx> {
    task_pool: TTaskPool<Q, Ctx>,
    threads: Vec<JoinHandle<()>>,
    task_count: Arc<AtomicUsize>,
    sender: Sender<Task<Ctx>>,
    // ctx_factory should only be used in one thread
    ctx_factory: C,
}

impl<Q, C, Ctx> ThreadPool<Q, C, Ctx>
where
    Q: ScheduleQueue<Ctx> + Send + 'static,
    Ctx: Context + 'static,
    C: ContextFactory<Ctx>,
{
    pub fn new(name: String, num_threads: usize, queue: Q, f: C) -> ThreadPool<Q, C, Ctx> {
        assert!(num_threads >= 1);
        let (sender, receiver) = channel::<Task<Ctx>>();
        let task_pool = Arc::new((Mutex::new(TaskPool::new(queue, receiver)), Condvar::new()));
        let mut threads = Vec::with_capacity(num_threads);
        let task_count = Arc::new(AtomicUsize::new(0));
        // Threadpool threads
        for _ in 0..num_threads {
            let tasks = task_pool.clone();
            let task_num = task_count.clone();
            let ctx = f.create_context();
            let thread = Builder::new()
                .name(name.clone())
                .spawn(move || {
                    let mut worker = Worker::new(tasks, task_num, ctx);
                    worker.run();
                })
                .unwrap();
            threads.push(thread);
        }

        ThreadPool {
            task_pool: task_pool,
            threads: threads,
            task_count: task_count,
            sender: sender,
            ctx_factory: f,
        }
    }

    pub fn execute<F>(&mut self, job: F)
    where
        F: FnOnce(Ctx) -> Ctx + Send + 'static,
        Ctx: Context,
    {
        let ctx = self.ctx_factory.create_context();
        let task = Task::new(job, ctx);
        self.sender.send(task).unwrap();
        self.task_count.fetch_add(1, AtomicOrdering::SeqCst);
        let &(_, ref cvar) = &*self.task_pool;
        cvar.notify_one();
    }

    #[inline]
    pub fn get_task_count(&self) -> usize {
        self.task_count.load(AtomicOrdering::SeqCst)
    }

    pub fn stop(&mut self) -> Result<(), String> {
        {
            let &(ref lock, ref cvar) = &*self.task_pool;
            let mut tasks = lock.lock().unwrap();
            tasks.stop();
            cvar.notify_all();
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
struct Worker<Q, C> {
    task_pool: TTaskPool<Q, C>,
    task_count: Arc<AtomicUsize>,
    ctx: C,
}

impl<Q, C> Worker<Q, C>
where
    Q: ScheduleQueue<C>,
    C: Context,
{
    fn new(task_pool: TTaskPool<Q, C>, task_count: Arc<AtomicUsize>, ctx: C) -> Worker<Q, C> {
        Worker {
            task_pool: task_pool,
            task_count: task_count,
            ctx: ctx,
        }
    }

    fn run(&mut self) {
        let mut task = self.get_next_task(None);
        // Start the worker. Loop breaks when receive stop message.
        while let Some(mut t) = task {
            // Since tikv would be down when any panic happens,
            // we don't need to process panic case here.
            t.ctx = (t.task)(t.ctx);
            self.task_count.fetch_sub(1, AtomicOrdering::SeqCst);
            task = self.get_next_task(Some(t.ctx));
        }
    }

    // `get_next_task` return `None` when `task_pool` is stopped.
    #[inline]
    fn get_next_task(&mut self, prev_ctx: Option<C>) -> Option<Task<C>> {
        // try to receive notification.
        let &(ref lock, ref cvar) = &*self.task_pool;
        let mut task_pool = lock.lock().unwrap();
        if prev_ctx.is_some() {
            let mut ctx = prev_ctx.unwrap();
            ctx.on_task_finished(&mut self.ctx);
        }
        loop {
            if task_pool.is_stopped() {
                return None;
            }
            if let Some(mut task) = task_pool.pop_task() {
                task.ctx.on_task_started(&mut self.ctx);
                return Some(task);
            }
            task_pool = cvar.wait(task_pool).unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Context, ContextFactory, FifoQueue, ScheduleQueue, Task, ThreadPool};
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
        let mut task_pool = ThreadPool::new(name, concurrency, FifoQueue::new(), f);
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
    fn test_fifo_queue() {
        let mut queue = FifoQueue::new();
        let f = DummyContextFactory {};
        for id in 0..10 {
            let mut task = Task::new(
                move |d: DummyContext| -> DummyContext { d },
                f.create_context(),
            );
            task.id = id;
            queue.push(task);
        }
        for id in 0..10 {
            let task = queue.pop().unwrap();
            assert_eq!(id, task.id);
        }
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
        let mut task_pool = ThreadPool::new(name, concurrency, FifoQueue::new(), f);

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

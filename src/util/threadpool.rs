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
use std::sync::{Arc, Mutex, Condvar};
use std::thread::{Builder, JoinHandle};
use std::boxed::FnBox;
use std::collections::{BinaryHeap, VecDeque};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::hash::Hash;
use std::marker::PhantomData;
use std::fmt::Write;
use super::collections::HashMap;

pub struct Task<T> {
    // The task's id in the pool. Each task has a unique id,
    // and it's always bigger than preceding ones.
    id: u64,
    // which group the task belongs to.
    group_id: T,
    task: Box<FnBox() + Send>,
}

impl<T> Task<T> {
    fn new<F>(id: u64, group_id: T, job: F) -> Task<T>
        where F: FnOnce() + Send + 'static
    {
        Task {
            id: id,
            group_id: group_id,
            task: Box::new(job),
        }
    }
}

impl<T> Ord for Task<T> {
    fn cmp(&self, right: &Task<T>) -> Ordering {
        self.id.cmp(&right.id).reverse()
    }
}

impl<T> PartialEq for Task<T> {
    fn eq(&self, right: &Task<T>) -> bool {
        self.cmp(right) == Ordering::Equal
    }
}

impl<T> Eq for Task<T> {}

impl<T> PartialOrd for Task<T> {
    fn partial_cmp(&self, rhs: &Task<T>) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}

pub trait ScheduleQueue<T> {
    fn pop(&mut self) -> Option<Task<T>>;
    fn push(&mut self, task: Task<T>);
    fn on_task_finished(&mut self, group_id: &T);
    fn on_task_started(&mut self, group_id: &T);
}

pub struct PushError<T>(pub T);

struct GroupStatisticsItem {
    running_count: usize,
    queue1_count: usize,
}

impl GroupStatisticsItem {
    fn new() -> GroupStatisticsItem {
        GroupStatisticsItem {
            running_count: 0,
            queue1_count: 0,
        }
    }

    fn total(&self) -> usize {
        self.queue1_count + self.running_count
    }
}


// `BigGroupThrottledQueue` tries to throttle group's concurrency to
//  `group_concurrency_limit` when all threads are busy.
// When one worker asks a task to run, it schedules in the following way:
// 1. Find out which group has a running number that is smaller than
//    that of `group_concurrency_limit`.
// 2. If more than one group meets the first point, run the one who
//    comes first.
// If no group meets the first point, choose according to the following rules:
// 1. Select the groups with least running tasks.
// 2. If more than one group meets the first point,choose the one
//     whose task comes first(with the minimum task's ID)
pub struct BigGroupThrottledQueue<T> {
    // The Tasks in `waiting_queue1` have higher priority than
    // tasks in `waiting_queue2`.
    waiting_queue1: BinaryHeap<Task<T>>,
    // group_id => tasks array. If `group_statistics[group_id]` is bigger than
    // `group_concurrency_limit`(which means the number of on-going tasks is
    // more than `group_concurrency_limit`), the rest of the group's tasks
    // would be pushed into `waiting_queue2[group_id]`
    waiting_queue2: HashMap<T, VecDeque<Task<T>>>,
    // group_id => running_num+pending_num(in `waiting_queue1`). It means at most
    // `group_statistics[group_id]` tasks of the group may be running
    group_statistics: HashMap<T, GroupStatisticsItem>,
    // The maximum number of threads that each group can run when the pool is busy.
    // Each value in `group_statistics` shouldn't be bigger than this value.
    group_concurrency_limit: usize,
}


impl<T: Hash + Eq + Ord + Send + Clone> BigGroupThrottledQueue<T> {
    pub fn new(group_concurrency_limit: usize) -> BigGroupThrottledQueue<T> {
        BigGroupThrottledQueue {
            group_statistics: HashMap::new(),
            waiting_queue2: HashMap::new(),
            waiting_queue1: BinaryHeap::new(),
            group_concurrency_limit: group_concurrency_limit,
        }
    }

    // Try push into queue1. Return none on success,return PushError(task) on failed.
    #[inline]
    fn try_push_into_queue1(&mut self, task: Task<T>) -> Result<(), PushError<Task<T>>> {
        let statistics = self.group_statistics
            .entry(task.group_id.clone())
            .or_insert(GroupStatisticsItem::new());
        if statistics.total() >= self.group_concurrency_limit {
            return Err(PushError(task));
        }
        statistics.queue1_count += 1;
        self.waiting_queue1.push(task);
        Ok(())
    }

    #[inline]
    fn pop_from_queue2(&mut self) -> Option<Task<T>> {
        let group_id = {
            // Groups in waiting_queue2 wouldn't too much, so iterate the map
            // is quick.
            let best_group = self.waiting_queue2
                .iter()
                .map(|(group_id, waiting_queue2)| {
                    (self.group_statistics[group_id].total(), waiting_queue2[0].id, group_id)
                })
                .min();
            if let Some((_, _, group_id)) = best_group {
                group_id.clone()
            } else {
                return None;
            }
        };

        let task = self.pop_from_waiting_queue_by_group_id(&group_id);
        Some(task)
    }

    #[inline]
    fn pop_from_waiting_queue_by_group_id(&mut self, group_id: &T) -> Task<T> {
        let (empty_after_pop, task) = {
            let mut waiting_tasks = self.waiting_queue2.get_mut(group_id).unwrap();
            let task = waiting_tasks.pop_front().unwrap();
            (waiting_tasks.is_empty(), task)
        };

        if empty_after_pop {
            self.waiting_queue2.remove(group_id);
        }
        task
    }
}

impl<T: Hash + Eq + Ord + Send + Clone> ScheduleQueue<T> for BigGroupThrottledQueue<T> {
    fn push(&mut self, task: Task<T>) {
        if let Err(PushError(task)) = self.try_push_into_queue1(task) {
            self.waiting_queue2
                .entry(task.group_id.clone())
                .or_insert_with(VecDeque::new)
                .push_back(task);
        }
    }

    fn pop(&mut self) -> Option<Task<T>> {
        if let Some(task) = self.waiting_queue1.pop() {
            let statistics = self.group_statistics.get_mut(&task.group_id).unwrap();
            statistics.queue1_count -= 1;
            return Some(task);
        } else if let Some(task) = self.pop_from_queue2() {
            return Some(task);
        }
        None
    }

    fn on_task_started(&mut self, group_id: &T) {
        let statistics =
            self.group_statistics.entry(group_id.clone()).or_insert(GroupStatisticsItem::new());
        statistics.running_count += 1
    }

    fn on_task_finished(&mut self, group_id: &T) {
        let count = {
            let mut statistics = self.group_statistics.get_mut(group_id).unwrap();
            statistics.running_count -= 1;
            statistics.total()
        };
        if count == 0 {
            self.group_statistics.remove(group_id);
        } else if count >= self.group_concurrency_limit {
            // If the number of running tasks for this group is big enough.
            return;
        }

        if !self.waiting_queue2.contains_key(group_id) {
            return;
        }

        // If the value of `group_statistics[group_id]` is not big enough, pop
        // a task from `waiting_queue2[group_id]` and push it into `waiting_queue1`.
        let group_task = self.pop_from_waiting_queue_by_group_id(group_id);
        assert!(self.try_push_into_queue1(group_task).is_ok());
    }
}

struct TaskPool<Q, T> {
    next_task_id: u64,
    task_queue: Q,
    marker: PhantomData<T>,
    stop: bool,
}

impl<Q: ScheduleQueue<T>, T> TaskPool<Q, T> {
    fn new(queue: Q) -> TaskPool<Q, T> {
        TaskPool {
            next_task_id: 0,
            task_queue: queue,
            marker: PhantomData,
            stop: false,
        }
    }

    // `push_task` pushes a new task into pool.
    fn push_task<F>(&mut self, group_id: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        let task = Task::new(self.next_task_id, group_id, job);
        self.next_task_id += 1;
        self.task_queue.push(task);
    }

    // `on_task_finished` is called when the thread finished a task.
    // It will clean up the remaining information of the task.
    fn on_task_finished(&mut self, group_id: &T) {
        self.task_queue.on_task_finished(group_id);
    }

    fn on_task_started(&mut self, group_id: &T) {
        self.task_queue.on_task_started(group_id);
    }

    fn pop_task(&mut self) -> Option<Task<T>> {
        self.task_queue.pop()
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

/// `ThreadPool` is used to execute tasks in parallel.
/// Each task would be pushed into the pool, and when a thread
/// is ready to process a task, it will get a task from the pool
/// according to the `ScheduleQueue` provided in initialization.
pub struct ThreadPool<Q, T> {
    task_pool: Arc<(Mutex<TaskPool<Q, T>>, Condvar)>,
    threads: Vec<JoinHandle<()>>,
    task_count: Arc<AtomicUsize>,
}

impl<Q: ScheduleQueue<T> + Send + 'static, T: Hash + Eq + Send + Clone + 'static> ThreadPool<Q, T> {
    pub fn new(name: String, num_threads: usize, queue: Q) -> ThreadPool<Q, T> {
        assert!(num_threads >= 1);
        let task_pool = Arc::new((Mutex::new(TaskPool::new(queue)), Condvar::new()));
        let mut threads = Vec::with_capacity(num_threads);
        let task_count = Arc::new(AtomicUsize::new(0));
        // Threadpool threads
        for _ in 0..num_threads {
            let tasks = task_pool.clone();
            let task_num = task_count.clone();
            let thread = Builder::new()
                .name(name.clone())
                .spawn(move || {
                    let mut worker = Worker::new(tasks, task_num);
                    worker.run();
                })
                .unwrap();
            threads.push(thread);
        }

        ThreadPool {
            task_pool: task_pool,
            threads: threads,
            task_count: task_count,
        }
    }

    pub fn execute<F>(&mut self, group_id: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        let &(ref lock, ref cvar) = &*self.task_pool;
        let mut meta = lock.lock().unwrap();
        meta.push_task(group_id, job);
        cvar.notify_one();
        self.task_count.fetch_add(1, AtomicOrdering::SeqCst);
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
struct Worker<Q, T> {
    task_pool: Arc<(Mutex<TaskPool<Q, T>>, Condvar)>,
    task_count: Arc<AtomicUsize>,
}

impl<Q, T> Worker<Q, T>
    where Q: ScheduleQueue<T>
{
    fn new(task_pool: Arc<(Mutex<TaskPool<Q, T>>, Condvar)>,
           task_count: Arc<AtomicUsize>)
           -> Worker<Q, T> {
        Worker {
            task_pool: task_pool,
            task_count: task_count,
        }
    }

    // `get_next_task` return `None` when `task_pool` is stopped.
    #[inline]
    fn get_next_task(&self) -> Option<Task<T>> {
        // try to receive notification.
        let &(ref lock, ref cvar) = &*self.task_pool;
        let mut task_pool = lock.lock().unwrap();
        loop {
            if task_pool.is_stopped() {
                return None;
            }
            if let Some(task) = task_pool.pop_task() {
                // `on_task_started` should be here since:
                //  1. To reduce lock's time;
                //  2. For some schedula_queue,on_task_started should be
                //  in the same lock with `pop_task` for the thread safety.
                task_pool.on_task_started(&task.group_id);
                return Some(task);
            }
            // wait for new task
            task_pool = cvar.wait(task_pool).unwrap();
        }
    }

    fn on_task_finished(&self, group_id: &T) {
        let &(ref lock, _) = &*self.task_pool;
        let mut task_pool = lock.lock().unwrap();
        task_pool.on_task_finished(group_id);
    }

    fn run(&mut self) {
        // Start the worker. Loop breaks when receive stop message.
        while let Some(task) = self.get_next_task() {
            // Since tikv would be down when any panic happens,
            // we don't need to process panic case here.
            (task.task)();
            self.on_task_finished(&task.group_id);
            self.task_count.fetch_sub(1, AtomicOrdering::SeqCst);
        }
    }
}

#[cfg(test)]
mod test {
    use super::{ThreadPool, BigGroupThrottledQueue, Task, ScheduleQueue};
    use std::thread::sleep;
    use std::time::Duration;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Condvar, Mutex};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn test_for_tasks_with_different_cost() {
        let name = thd_name!("test_tasks_with_different_cost");
        let concurrency = 2;
        let mut task_pool = ThreadPool::new(name, concurrency, BigGroupThrottledQueue::new(1));
        let (jtx, jrx) = channel();
        let group_with_big_task = 1001 as u64;
        let sleep_duration = Duration::from_millis(50);
        let recv_timeout_duration = Duration::from_secs(2);

        // Push a big task into pool.
        task_pool.execute(group_with_big_task, move || {
            sleep(sleep_duration * 10);
        });

        for group_id in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(group_id, move || {
                sleep(sleep_duration);
                sender.send(group_id).unwrap();
            });
        }

        for _ in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(group_with_big_task, move || {
                sleep(sleep_duration);
                sender.send(group_with_big_task).unwrap();
            });
        }

        // Since a long task of `group_with_big_task` is running,
        // the other threads shouldn't run any task of `group_with_big_task`.
        for _ in 0..10 {
            let group_id = jrx.recv_timeout(recv_timeout_duration).unwrap();
            assert_ne!(group_id, group_with_big_task);
        }

        for _ in 0..10 {
            let second = jrx.recv_timeout(recv_timeout_duration).unwrap();
            assert_eq!(second, group_with_big_task);
        }
        task_pool.stop().unwrap();
    }

    #[test]
    fn test_for_tasks_with_group_concurrency_on_busy() {
        let name = thd_name!("test_for_tasks_with_group_concurrency_on_busy");
        let concurrency = 4;
        let mut task_pool = ThreadPool::new(name, concurrency, BigGroupThrottledQueue::new(2));
        let (tx, rx) = channel();
        let sleep_duration = Duration::from_millis(50);
        let recv_timeout_duration = Duration::from_secs(2);
        let group1 = 1001;
        let push_tasks_cond = Arc::new((Mutex::new(()), Condvar::new()));
        let push_finished = Arc::new(AtomicBool::new(false));

        // Make all threads busy until all test tasks had been pushed into the pool.
        for gid in 0..concurrency {
            let listener = push_tasks_cond.clone();
            let push_finished = push_finished.clone();
            task_pool.execute(gid, move || {
                let &(ref lock, ref cvar) = &*listener;
                // conv needn't start to wait if noitification is sended.
                if !push_finished.load(Ordering::SeqCst) {
                    cvar.wait_timeout(lock.lock().unwrap(), recv_timeout_duration).unwrap();
                }
            });
        }

        // Push 4 tasks of `group1` into pool with each task cost `sleep_duration`.
        for _ in 0..4 {
            let tx = tx.clone();
            task_pool.execute(group1, move || {
                sleep(sleep_duration);
                tx.send(group1).unwrap();
            });
        }

        // Push 2 tasks of  `group2` into the pool with each task cost `2*sleep_duration`.
        let group2 = 1002;
        for _ in 0..2 {
            let tx = tx.clone();
            task_pool.execute(group2, move || {
                sleep(sleep_duration * 2);
                tx.send(group2).unwrap();
            });
        }

        // Push 2 tasks of `group3` into the pool with each task cost `2*sleep_duration`.
        let group3 = 1003;
        for _ in 0..2 {
            let tx = tx.clone();
            task_pool.execute(group3, move || {
                sleep(sleep_duration);
                tx.send(group3).unwrap();
            });
        }

        // Notify all threads that all test tasks had been pushed into the pool.
        push_tasks_cond.1.notify_all();

        // conv needn't start to wait if noitification is sended.
        push_finished.store(true, Ordering::SeqCst);

        // The tasks in pool: {txn11, txn12, txn13, txn14, txn21, txn22, txn31, txn32}.
        // First 4 tasks running during [0,sleep_duration] should be
        // {txn11, txn12,txn21, txn22 }. Since txn1 would be finished before txn2.
        // Next 4 tasks running during [sleep_duration,2*sleep_duration] should be
        // {txn13, txn14, txn21, txn22 }. During [2*sleep_duration,3*sleep_duration],
        // the running tasks should be {txn31, txn32}
        assert_eq!(rx.recv_timeout(recv_timeout_duration).unwrap(), group1);
        assert_eq!(rx.recv_timeout(recv_timeout_duration).unwrap(), group1);
        let mut group2_num = 0;
        let mut group1_num = 0;
        for _ in 0..4 {
            let group = rx.recv_timeout(recv_timeout_duration).unwrap();
            if group == group1 {
                group1_num += 1;
                continue;
            }
            assert_eq!(group, group2);
            group2_num += 1;
        }
        assert_eq!(group1_num, 2);
        assert_eq!(group2_num, 2);
        assert_eq!(rx.recv_timeout(recv_timeout_duration).unwrap(), group3);
        assert_eq!(rx.recv_timeout(recv_timeout_duration).unwrap(), group3);
    }

    #[test]
    fn test_get_task_count() {
        let name = thd_name!("test_get_task_count");
        let concurrency = 1;
        let mut task_pool = ThreadPool::new(name, concurrency, BigGroupThrottledQueue::new(1));
        let (tx, rx) = channel();
        let sleep_duration = Duration::from_millis(50);
        let recv_timeout_duration = Duration::from_secs(2);
        let group_num = 4;
        let mut task_num = 0;
        for gid in 0..group_num {
            let txer = tx.clone();
            task_pool.execute(gid, move || {
                sleep(sleep_duration);
                txer.send(gid).unwrap();
            });
            task_num += 1;
            assert_eq!(task_pool.get_task_count(), task_num);
        }

        for gid in 0..group_num {
            assert_eq!(gid, rx.recv_timeout(recv_timeout_duration).unwrap());
            // To make sure the task is finished and count has been changed.
            sleep(sleep_duration / 2);
            task_num -= 1;
            assert_eq!(task_pool.get_task_count(), task_num);
        }
        task_pool.stop().unwrap();
    }

    #[test]
    fn test_throttled_queue() {
        let max_pending_task_each_group = 1;
        let mut queue = BigGroupThrottledQueue::new(max_pending_task_each_group);
        // Push 4 tasks of `group1` into queue
        let group1 = 1001;
        let mut id = 0;
        for _ in 0..4 {
            let task = Task::new(id, group1, move || {});
            id += 1;
            queue.push(task);
        }

        // Push 4 tasks of `group2` into queue.
        let group2 = 1002;
        for _ in 0..4 {
            let task = Task::new(id, group2, move || {});
            id += 1;
            queue.push(task);
        }

        // queue: g1, g1, g1, g1, g2, g2, g2, g2. As all groups has a running number that is
        // smaller than that of group_concurrency_limit, and g1 comes first.
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group1);
        queue.on_task_started(&group1);

        // queue: g1, g1, g1, g2, g2, g2, g2; running:g1.
        // only g2 has a running number that is smaller than that of group_concurrency_limit.
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group2);
        queue.on_task_started(&group2);

        // queue: g1, g1, g1, g2, g2, g2; running:g1,g2. Since no group has a running number
        // smaller than `group_concurrency_limit`, and each group's running number is
        // the same, choose g1 since it comes first.
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group1);
        queue.on_task_started(&group1);

        // queue:g1, g1, g2, g2, g2; running:g1,g2,g1. Since no group has a running number
        // smaller than `group_concurrency_limit`, and the running number of g2 is smaller,
        // choose g2 since it comes first.
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group2);
        queue.on_task_started(&group2);
    }
}

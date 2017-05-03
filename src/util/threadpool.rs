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
use std::fmt::{self, Write, Debug, Formatter};
use super::collections::HashMap;

pub struct Task<T> {
    // The task's id in the pool. Each task has a unique id,
    // and it's always bigger than preceding ones.
    id: u64,

    // which group the task belongs to.
    gid: T,
    task: Box<FnBox() + Send>,
}

impl<T: Debug> Debug for Task<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "task_id:{},group_id:{:?}", self.id, self.gid)
    }
}

impl<T> Task<T> {
    fn new<F>(id: u64, gid: T, job: F) -> Task<T>
        where F: FnOnce() + Send + 'static
    {
        Task {
            id: id,
            gid: gid,
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

pub trait ScheduleQueue<T: Debug> {
    fn pop(&mut self) -> Option<Task<T>>;
    fn push(&mut self, task: Task<T>);
    fn on_task_finished(&mut self, gid: &T);
    fn on_task_started(&mut self, gid: &T);
}

#[derive(Debug)]
pub struct ReachConcurrencyLimit<T: Debug>(pub T);

struct GroupStatisticsItem {
    running_count: usize,
    high_priority_queue_count: usize,
}

impl GroupStatisticsItem {
    fn new() -> GroupStatisticsItem {
        GroupStatisticsItem {
            running_count: 0,
            high_priority_queue_count: 0,
        }
    }

    fn sum(&self) -> usize {
        self.running_count + self.high_priority_queue_count
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
    // The Tasks in `high_priority_queue` have higher priority than
    // tasks in `low_priority_queue`.
    high_priority_queue: BinaryHeap<Task<T>>,

    // gid => tasks array. If `group_statistics[gid]` is bigger than
    // `group_concurrency_limit`(which means the number of on-going tasks is
    // more than `group_concurrency_limit`), the rest of the group's tasks
    // would be pushed into `low_priority_queue[gid]`
    low_priority_queue: HashMap<T, VecDeque<Task<T>>>,

    // gid => (running_count,high_priority_queue_count)
    group_statistics: HashMap<T, GroupStatisticsItem>,

    // The maximum number of threads that each group can run when the pool is busy.
    // Each value in `group_statistics` shouldn't be bigger than this value.
    group_concurrency_limit: usize,
}

impl<T: Debug + Hash + Eq + Ord + Send + Clone> BigGroupThrottledQueue<T> {
    pub fn new(group_concurrency_limit: usize) -> BigGroupThrottledQueue<T> {
        BigGroupThrottledQueue {
            high_priority_queue: BinaryHeap::new(),
            low_priority_queue: HashMap::new(),
            group_statistics: HashMap::new(),
            group_concurrency_limit: group_concurrency_limit,
        }
    }

    // Try push into high priority queue. Return none on success,return PushError(task) on failed.
    #[inline]
    fn try_push_into_high_priority_queue(&mut self,
                                         task: Task<T>)
                                         -> Result<(), ReachConcurrencyLimit<Task<T>>> {
        let mut statistics = self.group_statistics
            .entry(task.gid.clone())
            .or_insert(GroupStatisticsItem::new());
        if statistics.sum() >= self.group_concurrency_limit {
            return Err(ReachConcurrencyLimit(task));
        }
        self.high_priority_queue.push(task);
        statistics.high_priority_queue_count += 1;
        Ok(())
    }

    #[inline]
    fn pop_from_low_priority_queue(&mut self) -> Option<Task<T>> {
        let gid = {
            // Groups in low_priority_queue wouldn't too much, so iterate the map
            // is quick.
            let best_group = self.low_priority_queue
                .iter()
                .map(|(gid, low_priority_queue)| {
                    (self.group_statistics[gid].sum(), low_priority_queue[0].id, gid)
                })
                .min();
            if let Some((_, _, gid)) = best_group {
                gid.clone()
            } else {
                return None;
            }
        };

        let task = self.pop_from_low_priority_queue_by_gid(&gid);
        Some(task)
    }

    #[inline]
    fn pop_from_low_priority_queue_by_gid(&mut self, gid: &T) -> Task<T> {
        let (empty_after_pop, task) = {
            let mut waiting_tasks = self.low_priority_queue.get_mut(gid).unwrap();
            let task = waiting_tasks.pop_front().unwrap();
            (waiting_tasks.is_empty(), task)
        };

        if empty_after_pop {
            self.low_priority_queue.remove(gid);
        }
        task
    }
}

impl<T: Hash + Eq + Ord + Send + Clone + Debug> ScheduleQueue<T> for BigGroupThrottledQueue<T> {
    fn push(&mut self, task: Task<T>) {
        if let Err(ReachConcurrencyLimit(task)) = self.try_push_into_high_priority_queue(task) {
            self.low_priority_queue
                .entry(task.gid.clone())
                .or_insert_with(VecDeque::new)
                .push_back(task);
        }
    }

    fn pop(&mut self) -> Option<Task<T>> {
        if let Some(task) = self.high_priority_queue.pop() {
            let mut statistics = self.group_statistics.get_mut(&task.gid).unwrap();
            statistics.high_priority_queue_count -= 1;
            return Some(task);
        } else if let Some(task) = self.pop_from_low_priority_queue() {
            return Some(task);
        }
        None
    }

    fn on_task_started(&mut self, gid: &T) {
        let mut statistics =
            self.group_statistics.entry(gid.clone()).or_insert(GroupStatisticsItem::new());
        statistics.running_count += 1
    }

    fn on_task_finished(&mut self, gid: &T) {
        let count = {
            let mut statistics = self.group_statistics.get_mut(gid).unwrap();
            statistics.running_count -= 1;
            statistics.sum()
        };
        if count == 0 {
            self.group_statistics.remove(gid);
        } else if count >= self.group_concurrency_limit {
            // If the number of running tasks for this group is big enough.
            return;
        }

        if !self.low_priority_queue.contains_key(gid) {
            return;
        }

        // If the value of `group_statistics[gid]` is not big enough, pop
        // a task from `low_priority_queue[gid]` and push it into `high_priority_queue`.
        let group_task = self.pop_from_low_priority_queue_by_gid(gid);
        self.try_push_into_high_priority_queue(group_task).unwrap();
    }
}

struct TaskPool<Q, T> {
    next_task_id: u64,
    task_queue: Q,
    marker: PhantomData<T>,
    stop: bool,
}

impl<Q: ScheduleQueue<T>, T: Debug> TaskPool<Q, T> {
    fn new(queue: Q) -> TaskPool<Q, T> {
        TaskPool {
            next_task_id: 0,
            task_queue: queue,
            marker: PhantomData,
            stop: false,
        }
    }

    // `push_task` pushes a new task into pool.
    fn push_task<F>(&mut self, gid: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        let task = Task::new(self.next_task_id, gid, job);
        self.next_task_id += 1;
        self.task_queue.push(task);
    }

    // `on_task_finished` is called when the thread finished a task.
    // It will clean up the remaining information of the task.
    fn on_task_finished(&mut self, gid: &T) {
        self.task_queue.on_task_finished(gid);
    }

    fn on_task_started(&mut self, gid: &T) {
        self.task_queue.on_task_started(gid);
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

impl<Q, T> ThreadPool<Q, T>
    where Q: ScheduleQueue<T> + Send + 'static,
          T: Hash + Eq + Send + Clone + 'static + Debug
{
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

    pub fn execute<F>(&mut self, gid: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        let &(ref lock, ref cvar) = &*self.task_pool;
        let mut meta = lock.lock().unwrap();
        meta.push_task(gid, job);
        self.task_count.fetch_add(1, AtomicOrdering::SeqCst);
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
struct Worker<Q, T> {
    task_pool: Arc<(Mutex<TaskPool<Q, T>>, Condvar)>,
    task_count: Arc<AtomicUsize>,
}

impl<Q, T> Worker<Q, T>
    where Q: ScheduleQueue<T>,
          T: Debug
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
                task_pool.on_task_started(&task.gid);
                return Some(task);
            }
            // wait for new task
            task_pool = cvar.wait(task_pool).unwrap();
        }
    }

    fn on_task_finished(&self, gid: &T) {
        let &(ref lock, _) = &*self.task_pool;
        let mut task_pool = lock.lock().unwrap();
        task_pool.on_task_finished(gid);
    }

    fn run(&mut self) {
        // Start the worker. Loop breaks when receive stop message.
        while let Some(task) = self.get_next_task() {
            // Since tikv would be down when any panic happens,
            // we don't need to process panic case here.
            (task.task)();
            self.on_task_finished(&task.gid);
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

        for gid in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(gid, move || {
                sleep(sleep_duration);
                sender.send(gid).unwrap();
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
            let gid = jrx.recv_timeout(recv_timeout_duration).unwrap();
            assert_ne!(gid, group_with_big_task);
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
        let timeout = Duration::from_secs(2);

        // Make all threads busy until all test tasks had been pushed into the pool.
        let push_tasks_pair = Arc::new((Mutex::new(false), Condvar::new()));
        for gid in 0..concurrency {
            let pair = push_tasks_pair.clone();
            task_pool.execute(gid, move || {
                let &(ref lock, ref cvar) = &*pair;
                let mut finished = lock.lock().unwrap();
                while !*finished {
                    finished = cvar.wait_timeout(finished, timeout).unwrap().0;
                }
            });
        }

        // Push 4 tasks of `group1` into pool.
        let group1 = 1001;
        let group1_tasks_pair = Arc::new((Mutex::new(false), Condvar::new()));
        for _ in 0..2 {
            let pair = group1_tasks_pair.clone();
            let tx = tx.clone();
            task_pool.execute(group1, move || {
                let &(ref lock, ref cvar) = &*pair;
                let mut finished = lock.lock().unwrap();
                while !*finished {
                    finished = cvar.wait_timeout(finished, timeout).unwrap().0;
                }
                tx.send(group1).unwrap();
            });
        }

        let group1_tasks_pair2 = Arc::new((Mutex::new(false), Condvar::new()));
        for _ in 0..2 {
            let pair = group1_tasks_pair2.clone();
            let tx = tx.clone();
            task_pool.execute(group1, move || {
                let &(ref lock, ref cvar) = &*pair;
                let mut finished = lock.lock().unwrap();
                while !*finished {
                    finished = cvar.wait_timeout(finished, timeout).unwrap().0;
                }
                tx.send(group1).unwrap();
            });
        }

        // Push 2 tasks of  `group2` into the pool.
        let group2 = 1002;
        let group2_tasks_pair = Arc::new((Mutex::new(false), Condvar::new()));
        for _ in 0..2 {
            let pair = group2_tasks_pair.clone();
            let tx = tx.clone();
            task_pool.execute(group2, move || {
                let &(ref lock, ref cvar) = &*pair;
                let mut finished = lock.lock().unwrap();
                while !*finished {
                    finished = cvar.wait_timeout(finished, timeout).unwrap().0;
                }
                tx.send(group2).unwrap();
            });
        }

        // Push 2 tasks of `group3` into the pool.
        let group3 = 1003;
        let group3_tasks_pair = Arc::new((Mutex::new(false), Condvar::new()));
        for _ in 0..2 {
            let pair = group3_tasks_pair.clone();
            let tx = tx.clone();
            task_pool.execute(group3, move || {
                let &(ref lock, ref cvar) = &*pair;
                let mut finished = lock.lock().unwrap();
                while !*finished {
                    finished = cvar.wait_timeout(finished, timeout).unwrap().0;
                }
                tx.send(group3).unwrap();
            });
        }

        // Notify all threads that all test tasks had been pushed into the pool.
        // And then finish these tasks in sequence one by one.
        // Define task21 to be the first task of group 2.
        // The tasks in pool are
        // {task11, task12, task13, task14, task21, task22, task31, task32}.
        // Ensure tasks are scheduled as expected:
        // {task11, task12, task21, task22, task13, task14, task31, task32}.
        for (i, pair) in [push_tasks_pair,
                          group1_tasks_pair,
                          group2_tasks_pair,
                          group1_tasks_pair2,
                          group3_tasks_pair]
            .into_iter()
            .enumerate() {
            {
                let &(ref lock, ref cvar) = &**pair;
                let mut finished = lock.lock().unwrap();
                *finished = true;
                cvar.notify_all();
            }
            if i == 0 {
                continue;
            }
            let expected_group = match i {
                1 | 3 => group1,
                2 => group2,
                4 => group3,
                _ => unreachable!(),
            };
            for _ in 0..2 {
                let group = rx.recv_timeout(timeout).unwrap();
                assert_eq!(group,
                           expected_group,
                           "#{}: group {}, expected {}",
                           i,
                           group,
                           expected_group);
            }
        }
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
        let mut queue = BigGroupThrottledQueue::new(1);
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
        assert_eq!(task.gid, group1);
        queue.on_task_started(&group1);

        // queue: g1, g1, g1, g2, g2, g2, g2; running:g1.
        // only g2 has a running number that is smaller than that of group_concurrency_limit.
        let task = queue.pop().unwrap();
        assert_eq!(task.gid, group2);
        queue.on_task_started(&group2);

        // queue: g1, g1, g1, g2, g2, g2; running:g1,g2. Since no group has a running number
        // smaller than `group_concurrency_limit`, and each group's running number is
        // the same, choose g1 since it comes first.
        let task = queue.pop().unwrap();
        assert_eq!(task.gid, group1);
        queue.on_task_started(&group1);

        // queue:g1, g1, g2, g2, g2; running:g1,g2,g1. Since no group has a running number
        // smaller than `group_concurrency_limit`, and the running number of g2 is smaller,
        // choose g2 since it comes first.
        let task = queue.pop().unwrap();
        assert_eq!(task.gid, group2);
        queue.on_task_started(&group2);
    }
}

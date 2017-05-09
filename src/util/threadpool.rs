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
use std::sync::mpsc::{Sender, Receiver, channel};
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
    fn new<F>(gid: T, job: F) -> Task<T>
        where F: FnOnce() + Send + 'static
    {
        Task {
            id: 0,
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
    low_priority_queue_count: usize,
}

impl GroupStatisticsItem {
    fn new() -> GroupStatisticsItem {
        GroupStatisticsItem {
            running_count: 0,
            high_priority_queue_count: 0,
            low_priority_queue_count: 0,
        }
    }

    fn sum(&self) -> usize {
        self.running_count + self.high_priority_queue_count + self.low_priority_queue_count
    }
}

// `SpeedupSmallGroupsQueue` tries speedup groups with number of
//  tasks smaller than small_group_tasks_limit when all threads are busy.
pub struct SpeedupSmallGroupsQueue<T> {
    high_priority_queue: VecDeque<Task<T>>,
    low_priority_queue: VecDeque<Task<T>>,
    big_group_currency_on_busy: usize,
    small_group_tasks_limit: usize,
    statistics: HashMap<T, GroupStatisticsItem>,
}

impl<T: Hash + Eq + Ord + Send + Clone + Debug> SpeedupSmallGroupsQueue<T> {
    pub fn new(group_concurrency_on_busy: usize,
               small_group_tasks_limit: usize)
               -> SpeedupSmallGroupsQueue<T> {
        SpeedupSmallGroupsQueue {
            high_priority_queue: VecDeque::new(),
            low_priority_queue: VecDeque::new(),
            statistics: HashMap::new(),
            big_group_currency_on_busy: group_concurrency_on_busy,
            small_group_tasks_limit: small_group_tasks_limit,
        }
    }

    fn pop_from_high_priority_queue(&mut self) -> Option<Task<T>> {
        if self.high_priority_queue.is_empty() {
            return None;
        }
        let task = self.high_priority_queue.pop_front().unwrap();
        let mut statistics = self.statistics.get_mut(&task.gid).unwrap();
        statistics.high_priority_queue_count -= 1;
        Some(task)
    }

    fn pop_from_low_priority_queue(&mut self) -> Option<Task<T>> {
        if self.low_priority_queue.is_empty() {
            return None;
        }
        let task = self.low_priority_queue.pop_front().unwrap();
        let mut statistics = self.statistics
            .get_mut(&task.gid)
            .unwrap();
        statistics.low_priority_queue_count -= 1;
        Some(task)
    }
}

impl<T: Hash + Eq + Ord + Send + Clone + Debug> ScheduleQueue<T> for SpeedupSmallGroupsQueue<T> {
    fn push(&mut self, task: Task<T>) {
        let mut statistics = self.statistics
            .entry(task.gid.clone())
            .or_insert(GroupStatisticsItem::new());
        if statistics.low_priority_queue_count == 0 &&
           statistics.running_count + statistics.high_priority_queue_count <
           self.small_group_tasks_limit {
            self.high_priority_queue.push_back(task);
            statistics.high_priority_queue_count += 1;
        } else {
            self.low_priority_queue.push_back(task);
            statistics.low_priority_queue_count += 1;
        }
    }

    fn pop(&mut self) -> Option<Task<T>> {
        if self.low_priority_queue.is_empty() {
            return self.pop_from_high_priority_queue();
        }

        if self.high_priority_queue.is_empty() {
            return self.pop_from_low_priority_queue();
        }

        let pop_from_waiting1 = {
            let t1 = self.high_priority_queue.front().unwrap();
            let t2 = self.low_priority_queue.front().unwrap();
            t1.id < t2.id ||
            (self.statistics[&t2.gid].running_count >= self.big_group_currency_on_busy)
        };

        if !pop_from_waiting1 {
            return self.pop_from_low_priority_queue();
        }
        self.pop_from_high_priority_queue()
    }

    fn on_task_started(&mut self, gid: &T) {
        let mut statistics = self.statistics.get_mut(gid).unwrap();
        statistics.running_count += 1;
    }

    fn on_task_finished(&mut self, gid: &T) {
        let count = {
            let mut statistics = self.statistics.get_mut(gid).unwrap();
            statistics.running_count -= 1;
            statistics.sum()
        };
        if count == 0 {
            self.statistics.remove(gid);
        }
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
    jobs: Receiver<Task<T>>,
}

impl<Q, T> TaskPool<Q, T>
    where Q: ScheduleQueue<T>,
          T: Debug
{
    fn new(queue: Q, jobs: Receiver<Task<T>>) -> TaskPool<Q, T> {
        TaskPool {
            next_task_id: 0,
            task_queue: queue,
            marker: PhantomData,
            stop: false,
            jobs: jobs,
        }
    }

    fn on_task_finished(&mut self, gid: &T) {
        self.task_queue.on_task_finished(gid);
    }

    fn on_task_started(&mut self, gid: &T) {
        self.task_queue.on_task_started(gid);
    }

    fn pop_task(&mut self) -> Option<Task<T>> {
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

/// `ThreadPool` is used to execute tasks in parallel.
/// Each task would be pushed into the pool, and when a thread
/// is ready to process a task, it will get a task from the pool
/// according to the `ScheduleQueue` provided in initialization.
pub struct ThreadPool<Q, T> {
    task_pool: Arc<(Mutex<TaskPool<Q, T>>, Condvar)>,
    threads: Vec<JoinHandle<()>>,
    task_count: Arc<AtomicUsize>,
    sender: Sender<Task<T>>,
}

impl<Q, T> ThreadPool<Q, T>
    where Q: ScheduleQueue<T> + Send + 'static,
          T: Hash + Eq + Send + Clone + 'static + Debug
{
    pub fn new(name: String, num_threads: usize, queue: Q) -> ThreadPool<Q, T> {
        assert!(num_threads >= 1);
        let (sender, receiver) = channel::<Task<T>>();
        let task_pool = Arc::new((Mutex::new(TaskPool::new(queue, receiver)), Condvar::new()));
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
            sender: sender,
        }
    }

    pub fn execute<F>(&mut self, gid: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        let task = Task::new(gid, job);
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
    fn get_next_task(&self, prev_gid: Option<&T>) -> Option<Task<T>> {
        // try to receive notification.
        let &(ref lock, ref cvar) = &*self.task_pool;
        let mut task_pool = lock.lock().unwrap();
        if prev_gid.is_some() {
            task_pool.on_task_finished(prev_gid.unwrap());
        }
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
            task_pool = cvar.wait(task_pool).unwrap();
        }
    }

    fn run(&mut self) {
        let mut task = self.get_next_task(None);
        // Start the worker. Loop breaks when receive stop message.
        while let Some(t) = task {
            // Since tikv would be down when any panic happens,
            // we don't need to process panic case here.
            (t.task)();
            self.task_count.fetch_sub(1, AtomicOrdering::SeqCst);
            task = self.get_next_task(Some(&t.gid));
        }
    }
}

#[cfg(test)]
mod test {
    use super::{ThreadPool, BigGroupThrottledQueue, Task, ScheduleQueue, SpeedupSmallGroupsQueue};
    use std::time::Duration;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_for_tasks_with_different_cost() {
        let name = thd_name!("test_tasks_with_different_cost");
        let concurrency = 2;
        let mut task_pool = ThreadPool::new(name, concurrency, BigGroupThrottledQueue::new(1));
        let (jtx, jrx) = channel();
        let group_with_big_task = 1001 as u64;
        let timeout = Duration::from_secs(2);
        let (ftx, frx) = channel();
        // Push a big task into pool.
        task_pool.execute(group_with_big_task, move || {
            // Since a long task of `group_with_big_task` is running,
            // the other threads shouldn't run any task of `group_with_big_task`.
            for _ in 0..10 {
                let gid = jrx.recv_timeout(timeout).unwrap();
                assert_ne!(gid, group_with_big_task);
            }
            for _ in 0..10 {
                let gid = jrx.recv_timeout(timeout).unwrap();
                assert_eq!(gid, group_with_big_task);
            }
            ftx.send(true).unwrap();
        });

        for gid in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(gid, move || {
                sender.send(gid).unwrap();
            });
        }

        for _ in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(group_with_big_task, move || {
                sender.send(group_with_big_task).unwrap();
            });
        }
        frx.recv_timeout(timeout).unwrap();
        task_pool.stop().unwrap();
    }

    #[test]
    fn test_get_task_count() {
        let name = thd_name!("test_get_task_count");
        let concurrency = 1;
        let mut task_pool = ThreadPool::new(name, concurrency, BigGroupThrottledQueue::new(1));
        let (tx, rx) = channel();
        let (ftx, frx) = channel();
        let receiver = Arc::new(Mutex::new(rx));
        let timeout = Duration::from_secs(2);
        let group_num = 4;
        let mut task_num = 0;
        for gid in 0..group_num {
            let rxer = receiver.clone();
            let ftx = ftx.clone();
            task_pool.execute(gid, move || {
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
            assert!(left_num == task_num || left_num == task_num - 1,
                    format!("left_num {},task_num {}", left_num, task_num));
            task_num -= 1;
        }
        task_pool.stop().unwrap();
    }

    #[test]
    fn test_throttled_queue() {
        let mut queue = BigGroupThrottledQueue::new(1);
        let mut id = 1;
        // Push 4 tasks of `group1` into queue
        let group1 = 1001;
        for _ in 0..4 {
            let mut task = Task::new(group1, move || {});
            task.id = id;
            id += 1;
            queue.push(task);
        }

        // Push 4 tasks of `group2` into queue.
        let group2 = 1002;
        for _ in 0..4 {
            let mut task = Task::new(group2, move || {});
            task.id = id;
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


    #[test]
    fn test_speedup_small_groups_queue() {
        let concurrency_limit = 2;
        let small_group_tasks_limit = 2;
        let mut queue = SpeedupSmallGroupsQueue::new(concurrency_limit, small_group_tasks_limit);
        let mut id = 1;
        let mut pre_id = 0;
        // Push 2 tasks of `group1` into queue
        let group1 = 1001;
        for _ in 0..small_group_tasks_limit {
            let mut task = Task::new(group1, move || {});
            task.id = id;
            id += 1;
            queue.push(task);
        }

        // high:[g11,g12]; low:[]
        assert!(queue.low_priority_queue.is_empty());

        for _ in 0..small_group_tasks_limit {
            let mut task = Task::new(group1, move || {});
            task.id = id;
            id += 1;
            queue.push(task);
        }

        // high:[g11,g12]; low:[g13,g14]
        assert_eq!(queue.low_priority_queue.len(), small_group_tasks_limit);

        let task = queue.pop().unwrap();
        assert!(task.id > pre_id);
        pre_id = task.id;
        queue.on_task_started(&task.gid);
        // high:[g12]; low:[g13,g14]; running:[g11]
        assert_eq!(queue.high_priority_queue.len(), small_group_tasks_limit - 1);
        assert_eq!(queue.low_priority_queue.len(), small_group_tasks_limit);

        let group2 = 1002;
        for _ in 0..small_group_tasks_limit {
            let mut task = Task::new(group2, move || {});
            task.id = id;
            id += 1;
            queue.push(task);
        }
        // high:[g12,g21,g22], low:[g13,g14],running:[g11]
        let task = queue.pop().unwrap();
        assert!(task.id > pre_id);
        queue.on_task_started(&task.gid);
        // high:[g21,g22],low:[g13,g14],running:[g11,g12]
        queue.on_task_finished(&task.gid);
        // high:[g21,g22],low:[g13,g14],running:[g11]
        let task = queue.pop().unwrap();
        // since g13 comes before g21,
        // and group g1's running number is smaller than 2
        assert_eq!(task.gid, group1);
    }
}

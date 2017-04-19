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
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};
use std::hash::Hash;
use std::marker::PhantomData;

pub struct Task<T> {
    // The task's number in the pool. Each task has a unique number,
    // and it's always bigger than preceding ones.
    id: u64,
    // the task's group_id.
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

// `BigGroupThrottledQueue` tries to throttle group's concurrency to
//  `group_concurrency_on_busy` when it's busy.
// When one worker asks a task to run, it schedules in the following way:
// 1. Find out which group has a running number that is smaller than
//    that of `group_concurrency_on_busy`.
// 2. If more than one group meets the first point, run the one who
//    comes first.
// If no group meets the first point, choose according to the following rules:
// 1. Select the groups with least running tasks.
// 2. If more than one group meets the first point,choose the one
//     whose task comes first(with the minimum task's ID)
pub struct BigGroupThrottledQueue<T> {
    // tasks in pending. tasks in `pending_tasks` have higher priority than
    // tasks in waiting_queue.
    pending_tasks: BinaryHeap<Task<T>>,
    // group_id => tasks array. If `group_concurrency[group_id]` is bigger than
    // `group_concurrency_on_busy`(which means the number of on-going tasks is
    // more than `group_concurrency_on_busy`), the rest of the group's tasks
    // would be pushed into `waiting_queue[group_id]`
    waiting_queue: HashMap<T, VecDeque<Task<T>>>,
    // group_id => running_num+pending num. It means there may
    // `group_concurrency[group_id]` tasks of the group are running.
    group_concurrency: HashMap<T, usize>,
    // The max number of threads that each group can run when the pool is busy.
    // Each value in `group_concurrency` shouldn't be bigger than this value.
    group_concurrency_on_busy: usize,
}


impl<T: Hash + Eq + Send + Clone> BigGroupThrottledQueue<T> {
    pub fn new(group_concurrency_on_busy: usize) -> BigGroupThrottledQueue<T> {
        BigGroupThrottledQueue {
            group_concurrency: HashMap::new(),
            waiting_queue: HashMap::new(),
            pending_tasks: BinaryHeap::new(),
            group_concurrency_on_busy: group_concurrency_on_busy,
        }
    }

    // Try push into pending. Return none on success,return Some(task) on failed.
    #[inline]
    fn try_push_into_pending(&mut self, task: Task<T>) -> Result<(), PushError<Task<T>>> {
        let count = self.group_concurrency.entry(task.group_id.clone()).or_insert(0);
        if *count >= self.group_concurrency_on_busy {
            return Err(PushError(task));
        }
        *count += 1;
        self.pending_tasks.push(task);
        Ok(())
    }

    #[inline]
    fn pop_from_waiting_queue(&mut self) -> Option<Task<T>> {
        let group_id = self.pop_group_id_from_waiting_queue();
        if group_id.is_none() {
            return None;
        }
        let group_id = group_id.unwrap();
        let task = self.pop_from_waiting_queue_with_group_id(&group_id);
        Some(task)
    }

    #[inline]
    fn pop_from_waiting_queue_with_group_id(&mut self, group_id: &T) -> Task<T> {
        let (waiting_tasks_is_empty, task) = {
            let mut waiting_tasks = self.waiting_queue.get_mut(group_id).unwrap();
            let task = waiting_tasks.pop_front().unwrap();
            (waiting_tasks.is_empty(), task)
        };
        // If waiting tasks for current group is empty, remove it from `waiting_queue`.
        if waiting_tasks_is_empty {
            self.waiting_queue.remove(group_id);
        }
        task
    }

    // pop_group_id_from_waiting_queue returns the next task's group_id.
    // we choose group according to the following rules:
    // 1. Select the groups with the least running tasks.
    // 2. If more than one group meets the first point,
    //    choose the one whose task comes first(with the minimum task's ID)
    #[inline]
    fn pop_group_id_from_waiting_queue(&mut self) -> Option<T> {
        // (group_id,count,task_id) the best current group's info with it's group_id,
        // running tasks count, front task's id in waiting queue.
        let mut next_group = None;
        for (group_id, tasks) in &self.waiting_queue {
            let front_task_id = tasks[0].id;
            assert!(self.group_concurrency.contains_key(group_id));
            let count = self.group_concurrency[group_id];
            if next_group.is_none() {
                next_group = Some((group_id, count, front_task_id));
                continue;
            }
            let (_, pre_count, pre_task_id) = next_group.unwrap();
            if pre_count > count {
                next_group = Some((group_id, count, front_task_id));
                continue;
            }
            if pre_count == count && pre_task_id > front_task_id {
                next_group = Some((group_id, count, front_task_id));
            }
        }
        if let Some((group_id, _, _)) = next_group {
            return Some(group_id.clone());
        }
        // no task in waiting.
        None
    }
}

impl<T: Hash + Eq + Send + Clone> ScheduleQueue<T> for BigGroupThrottledQueue<T> {
    // push one task into queue.
    fn push(&mut self, task: Task<T>) {
        if let Err(PushError(task)) = self.try_push_into_pending(task) {
            self.waiting_queue
                .entry(task.group_id.clone())
                .or_insert_with(VecDeque::new)
                .push_back(task);
        }
    }

    fn pop(&mut self) -> Option<Task<T>> {
        if let Some(task) = self.pending_tasks.pop() {
            let count = self.group_concurrency.entry(task.group_id.clone()).or_insert(0);
            *count -= 1;
            return Some(task);
        } else if let Some(task) = self.pop_from_waiting_queue() {
            return Some(task);
        }
        None
    }

    fn on_task_started(&mut self, group_id: &T) {
        let count = self.group_concurrency.entry(group_id.clone()).or_insert(0);
        *count += 1;
    }

    fn on_task_finished(&mut self, group_id: &T) {
        // remove this task from group_concurrency
        let count = {
            let mut count = self.group_concurrency.get_mut(group_id).unwrap();
            *count -= 1;
            *count
        };
        if count == 0 {
            self.group_concurrency.remove(group_id);
        } else if count >= self.group_concurrency_on_busy {
            // if the number of running tasks for this group is big enough.
            return;
        }

        if !self.waiting_queue.contains_key(group_id) {
            return;
        }

        // if the number of running tasks for this group is not big enough in pending,
        // get the group's first task from waiting_queue and push it into pending.
        let group_task = self.pop_from_waiting_queue_with_group_id(group_id);
        assert!(self.try_push_into_pending(group_task).is_ok());
    }
}

struct TaskPool<Q, T> {
    next_task_id: u64,
    tasks: Q,
    marker: PhantomData<T>,
    stop: bool,
}

impl<Q: ScheduleQueue<T>, T> TaskPool<Q, T> {
    fn new(queue: Q) -> TaskPool<Q, T> {
        TaskPool {
            next_task_id: 0,
            tasks: queue,
            marker: PhantomData,
            stop: false,
        }
    }

    // push_task pushes a new task into pool.
    fn push_task<F>(&mut self, group_id: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        let task = Task::new(self.next_task_id, group_id, job);
        self.next_task_id += 1;
        self.tasks.push(task);
    }

    // on_task_finished is called when one task is on_task_finished in the
    // thread. It will clean up the remaining information of the task in the pool.
    fn on_task_finished(&mut self, group_id: &T) {
        self.tasks.on_task_finished(group_id);
    }

    fn on_task_started(&mut self, group_id: &T) {
        self.tasks.on_task_started(group_id);
    }

    fn pop_task(&mut self) -> Option<Task<T>> {
        self.tasks.pop()
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
/// is ready to process a task, it get a task from the waiting queue
/// according to the schedule queue provided in initialization.
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
            let mut builder = Builder::new();
            builder = builder.name(name.clone());
            let tasks = task_pool.clone();
            let task_num = task_count.clone();
            let thread = builder.spawn(move || {
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

    /// Executes the function `job` on a thread in the pool.
    pub fn execute<F>(&mut self, group_id: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        self.task_count.fetch_add(1, AOrdering::SeqCst);
        let &(ref lock, ref cvar) = &*self.task_pool;
        let mut meta = lock.lock().unwrap();
        meta.push_task(group_id, job);
        cvar.notify_one();
    }

    #[inline]
    pub fn get_task_count(&self) -> usize {
        self.task_count.load(AOrdering::SeqCst)
    }

    pub fn stop(&mut self) -> Result<(), String> {
        {
            let &(ref lock, ref cvar) = &*self.task_pool;
            let mut tasks = lock.lock().unwrap();
            tasks.stop();
            cvar.notify_all();
        }
        let mut err_msg = String::new();
        while let Some(t) = self.threads.pop() {
            if let Err(e) = t.join() {
                err_msg.push_str(format!("failed to join thread with err :{:?}", e).as_ref());
            }
        }

        if !err_msg.is_empty() {
            return Err(err_msg);
        }
        Ok(())
    }
}

// each thread has a worker.
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

    // get_next_task,return (None,true) when task_pool is stopped.
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
                // to reduce lock's time.
                task_pool.on_task_started(&task.group_id);
                return Some(task);
            }
            // wait new task
            task_pool = cvar.wait(task_pool).unwrap();
        }

    }

    fn on_task_finished(&self, group_id: &T) {
        let &(ref lock, _) = &*self.task_pool;
        let mut task_pool = lock.lock().unwrap();
        task_pool.on_task_finished(group_id);
    }

    fn run(&mut self) {
        // start the worker.
        // loop breaks when receive stop message.
        while let Some(task) = self.get_next_task() {
            // handle task
            // since tikv would be down when any panic happens,
            // we do't need to process panic case here.
            task.task.call_box(());
            self.on_task_finished(&task.group_id);
            self.task_count.fetch_sub(1, AOrdering::SeqCst);
        }
    }
}

#[cfg(test)]
mod test {
    use super::{ThreadPool, BigGroupThrottledQueue, Task, ScheduleQueue};
    use std::thread::sleep;
    use std::time::Duration;
    use std::sync::mpsc::channel;

    #[test]
    fn test_fair_group_for_tasks_with_different_cost() {
        let name = thd_name!("test_tasks_with_different_cost");
        let concurrency = 2;
        let mut task_pool = ThreadPool::new(name, concurrency, BigGroupThrottledQueue::new(1));
        let (jtx, jrx) = channel();
        let group_with_big_task = 1001 as u64;
        let sleep_duration = Duration::from_millis(50);
        let recv_timeout_duration = Duration::from_secs(2);

        // push big task into pool.
        task_pool.execute(group_with_big_task, move || {
            sleep(sleep_duration * 10);
        });

        // make sure the big task is running.
        sleep(sleep_duration / 4);

        // push 1 task for each group_id in [0..10) into pool.
        for group_id in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(group_id, move || {
                sleep(sleep_duration);
                sender.send(group_id).unwrap();
            });
        }
        // push 10 tasks of group_with_big_task's job into pool.
        for _ in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(group_with_big_task, move || {
                sleep(sleep_duration);
                sender.send(group_with_big_task).unwrap();
            });
        }

        // Since a long task of `group_with_big_task` is running,
        // the other threads shouldn't running group_with_big_task's task.
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
    fn test_fair_group_for_tasks_with_group_concurrency_on_busy() {
        let name = thd_name!("test_tasks_with_different_cost");
        let concurrency = 4;
        let mut task_pool = ThreadPool::new(name, concurrency, BigGroupThrottledQueue::new(2));
        let (tx, rx) = channel();
        let sleep_duration = Duration::from_millis(50);
        let recv_timeout_duration = Duration::from_secs(2);
        let group1 = 1001;

        for gid in 0..concurrency {
            task_pool.execute(gid, move || {
                sleep(sleep_duration);
            });
        }

        // push 4 txn1 into pool and each needs `sleep_duration`.
        for _ in 0..4 {
            let tx = tx.clone();
            task_pool.execute(group1, move || {
                sleep(sleep_duration);
                tx.send(group1).unwrap();
            });
        }

        // push 2 txn2 into pool and each needs 2*sleep_duration.
        let group2 = 1002;
        for _ in 0..2 {
            let tx = tx.clone();
            task_pool.execute(group2, move || {
                sleep(sleep_duration * 2);
                tx.send(group2).unwrap();
            });
        }

        // push 2 txn3 into pool and each needs `2*sleep_duration`.
        let group3 = 1003;
        for _ in 0..2 {
            let tx = tx.clone();
            task_pool.execute(group3, move || {
                sleep(sleep_duration);
                tx.send(group3).unwrap();
            });
        }

        // txn11, txn12, txn13, txn14, txn21, txn22, txn31, txn32
        // first 4 tasks during [0,sleep_duration] should be
        // {txn11, txn12, txn21, txn22 }. Since txn1 is finished before txn2,
        // 4 tasks during [sleep_duration,2*sleep_duration] should be
        // {txn13, txn14, txn21, txn22 }. During [2*sleep_duration,3*sleep_duration],
        // the running task should be {txn31, txn32}
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
            // to make sure the task was finished and count has been changed.
            sleep(sleep_duration / 2);
            task_num -= 1;
            assert_eq!(task_pool.get_task_count(), task_num);
        }
        task_pool.stop().unwrap();
    }

    #[test]
    fn test_fair_group_queue() {
        let max_pending_task_each_group = 2;
        let mut queue = BigGroupThrottledQueue::new(max_pending_task_each_group);
        // push 4 group1 into queue
        let group1 = 1001;
        let mut id = 0;
        for _ in 0..4 {
            let task = Task::new(id, group1, move || {});
            id += 1;
            queue.push(task);
        }

        // push 2 group2 into queue.
        let group2 = 1002;
        for _ in 0..2 {
            let task = Task::new(id, group2, move || {});
            id += 1;
            queue.push(task);
        }
        // push 2 group3 into queue.
        let group3 = 1003;
        for _ in 0..2 {
            let task = Task::new(id, group3, move || {});
            id += 1;
            queue.push(task);
        }
        // queue:g1, g1, g1, g1, g2, g2, g3, g3
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group1);
        queue.on_task_started(&group1);
        // queue: g1, g1, g1, g2, g2, g3, g3; running: g1
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group1);
        queue.on_task_started(&group1);
        // queue: g1, g1, g2, g2, g3, g3; running: g1, g1
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group2);
        queue.on_task_started(&group2);
        // queue: g1, g1, g2, g3, g3; running: g1, g1, g2
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group2);
        queue.on_task_started(&group2);
        // queue: g1, g1, g3, g3 ; running: g1, g1, g2, g2
        // finished one g2
        queue.on_task_finished(&group2);
        // queue: g1, g1, g3, g3; running: g1, g1, g2
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group3);
        queue.on_task_started(&group3);
        // queue: g1, g1, g3; running: g1, g1, g2, g3
        // finished g1
        queue.on_task_finished(&group1);
        // queue: g1, g1, g3; running: g1, g2, g3
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group1);
        queue.on_task_started(&group1);
        // queue: g1, g3; running: g1, g1, g2, g3
        // finished g2
        queue.on_task_finished(&group2);
        // queue: g1; running: g1, g1, g3, g3
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group3);
        queue.on_task_started(&group3);
        // finished g3
        queue.on_task_finished(&group3);
        // queue: g1; running: g1, g1, g3
        let task = queue.pop().unwrap();
        assert_eq!(task.group_id, group1);
        queue.on_task_started(&group1);
    }
}

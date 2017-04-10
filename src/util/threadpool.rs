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
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread::{Builder, JoinHandle};
use std::boxed::FnBox;
use std::collections::{BinaryHeap, HashSet, HashMap, VecDeque};
use std::cmp::Ordering;
use std::hash::Hash;

struct Task<T> {
    // the task's number in the pool.
    // each task has a unique number,
    // and it's always bigger than precedes one.
    id: u64,
    // the task's group_id.
    group_id: T,
    task: Box<FnBox() + Send>,
}

impl<T: Hash + Eq + Send + Clone + 'static> Task<T> {
    fn new<F>(id: u64, group_id: T, job: F) -> Task<T>
        where F: FnOnce() + Send + 'static
    {
        Task {
            id: id,
            group_id: group_id,
            task: Box::new(job),
        }
    }

    fn group_id(&self) -> T {
        self.group_id.clone()
    }
}

impl<T> Ord for Task<T> {
    fn cmp(&self, right: &Task<T>) -> Ordering {
        if self.id > right.id {
            return Ordering::Less;
        } else if self.id < right.id {
            return Ordering::Greater;
        }
        Ordering::Equal
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

struct WaitingHeap<T> {
    data: BinaryHeap<Task<T>>,
    group_set: HashSet<T>,
}

impl<T: Hash + Eq + Send + Clone + 'static> WaitingHeap<T> {
    fn new() -> WaitingHeap<T> {
        WaitingHeap {
            data: BinaryHeap::new(),
            group_set: HashSet::new(),
        }
    }

    fn try_push(&mut self, task: Task<T>) -> bool {
        let group_id = task.group_id();
        if self.group_set.contains(&group_id) {
            return false;
        }
        self.group_set.insert(group_id);
        self.data.push(task);
        true
    }

    fn pop(&mut self) -> Option<Task<T>> {
        if let Some(task) = self.data.pop() {
            self.group_set.remove(&task.group_id);
            return Some(task);
        }
        None
    }

    fn contains(&self, group_id: &T) -> bool {
        self.group_set.contains(group_id)
    }
}

pub struct FairGroupsTasksQueue<T> {
    // group_id => count
    running_tasks: HashMap<T, usize>,
    // group_id => tasks array
    waiting_queue: HashMap<T, VecDeque<Task<T>>>,
    waiting_heap: WaitingHeap<T>,
}

impl<T: Hash + Eq + Send + Clone + 'static> FairGroupsTasksQueue<T> {
    fn new() -> FairGroupsTasksQueue<T> {
        FairGroupsTasksQueue {
            running_tasks: HashMap::default(),
            waiting_queue: HashMap::default(),
            waiting_heap: WaitingHeap::new(),
        }
    }

    fn push_back(&mut self, task: Task<T>) {
        let group_id = task.group_id();
        if !self.running_tasks.contains_key(&group_id) && !self.waiting_heap.contains(&group_id) {
            self.waiting_heap.try_push(task);
            return;
        }
        let mut group_tasks = self.waiting_queue
            .entry(group_id)
            .or_insert_with(VecDeque::new);
        group_tasks.push_back(task);
    }

    fn pop_front(&mut self) -> Option<(T, Task<T>)> {
        let mut next_task = self.waiting_heap.pop();
        if next_task.is_none() {
            if let Some(gid) = self.pop_group_id_from_waiting_queue() {
                next_task = Some(self.pop_from_waiting_queue_with_group_id(gid));
            }
        }

        if let Some(task) = next_task {
            let group_id = task.group_id();
            // running tasks for group add 1.
            self.running_tasks.entry(group_id.clone()).or_insert(0 as usize);
            let mut running_tasks = self.running_tasks.get_mut(&group_id).unwrap();
            *running_tasks += 1;
            return Some((group_id, task));
        }
        None
    }

    fn finished(&mut self, group_id: T) {
        // if running tasks exist.
        if self.running_tasks[&group_id] > 1 {
            let mut count = self.running_tasks.get_mut(&group_id).unwrap();
            *count -= 1;
            return;
        }

        // if no running tasks left.
        self.running_tasks.remove(&group_id);
        if !self.waiting_queue.contains_key(&group_id) || self.waiting_heap.contains(&group_id) {
            return;
        }
        // if waiting tasks exist && group not in heap.
        let group_task = self.pop_from_waiting_queue_with_group_id(group_id);
        assert!(self.waiting_heap.try_push(group_task));
    }

    #[inline]
    fn pop_from_waiting_queue_with_group_id(&mut self, group_id: T) -> Task<T> {
        let (empty_group_wtasks, task) = {
            let mut waiting_tasks = self.waiting_queue.get_mut(&group_id).unwrap();
            let task_meta = waiting_tasks.pop_front().unwrap();
            (waiting_tasks.is_empty(), task_meta)
        };
        // if waiting tasks for group is empty,remove from waiting_tasks.
        if empty_group_wtasks {
            self.waiting_queue.remove(&group_id);
        }
        task
    }

    // pop_group_id_from_waiting_queue returns the next task's group_id.
    // we choose the group according to the following rules:
    // 1. Choose the groups with least running tasks.
    // 2. If more than one groups has the least running tasks,
    //    choose the one whose task comes first(with the minum task's ID)
    #[inline]
    fn pop_group_id_from_waiting_queue(&mut self) -> Option<T> {
        // (group_id,count,task_id)
        let mut next_group = None;
        for (group_id, tasks) in &self.waiting_queue {
            let front_task_id = tasks[0].id;
            assert!(self.running_tasks.contains_key(group_id));
            let count = self.running_tasks[group_id];
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
            return Some((*group_id).clone());
        }

        // no tasks in waiting.
        None
    }
}

pub enum ScheduleAlgorithm {
    FairGroups,
    FIFO,
}

enum TasksQueueAlgorithm<T> {
    FairGroups { queue: FairGroupsTasksQueue<T> },
    FIFO { queue: VecDeque<Task<T>> },
}

impl<T: Hash + Eq + Send + Clone + 'static> TasksQueueAlgorithm<T> {
    fn push(&mut self, task: Task<T>) {
        match *self {
            TasksQueueAlgorithm::FairGroups { ref mut queue } => {
                queue.push_back(task);
            }
            TasksQueueAlgorithm::FIFO { ref mut queue } => {
                queue.push_back(task);
            }
        }
    }

    fn pop(&mut self) -> Option<(T, Task<T>)> {
        match *self {
            TasksQueueAlgorithm::FairGroups { ref mut queue } => queue.pop_front(),
            TasksQueueAlgorithm::FIFO { ref mut queue } => {
                if let Some(task) = queue.pop_front() {
                    return Some((task.group_id(), task));
                }
                None
            }
        }
    }

    fn finished(&mut self, group_id: T) {
        if let TasksQueueAlgorithm::FairGroups { ref mut queue } = *self {
            queue.finished(group_id);
        }
    }
}

struct ThreadPoolMeta<T> {
    next_task_id: u64,
    total_running_tasks: usize,
    total_waiting_tasks: usize,
    tasks: TasksQueueAlgorithm<T>,
}

impl<T: Hash + Eq + Send + Clone + 'static> ThreadPoolMeta<T> {
    fn new(algorithm: ScheduleAlgorithm) -> ThreadPoolMeta<T> {
        ThreadPoolMeta {
            next_task_id: 0,
            total_running_tasks: 0,
            total_waiting_tasks: 0,
            tasks: match algorithm {
                ScheduleAlgorithm::FairGroups => {
                    TasksQueueAlgorithm::FairGroups { queue: FairGroupsTasksQueue::new() }
                }
                ScheduleAlgorithm::FIFO => TasksQueueAlgorithm::FIFO { queue: VecDeque::new() },
            },
        }
    }

    // push_task pushes a new task into pool.
    fn push_task<F>(&mut self, group_id: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        let task = Task::new(self.next_task_id, group_id, job);
        self.total_waiting_tasks += 1;
        self.next_task_id += 1;
        self.tasks.push(task);
    }

    fn get_tasks_num(&self) -> usize {
        self.total_waiting_tasks + self.total_running_tasks
    }

    // finished_task is called when one task is finished in the
    // thread. It will clean up the remaining information of the
    // task in the pool.
    fn finished_task(&mut self, group_id: T) {
        self.total_running_tasks -= 1;
        self.tasks.finished(group_id);
    }

    fn pop_next_task(&mut self) -> Option<(T, Task<T>)> {
        let next_task = self.tasks.pop();
        if next_task.is_none() {
            return None;
        }
        self.total_waiting_tasks -= 1;
        self.total_running_tasks += 1;
        next_task
    }
}

/// A group task pool used to execute tasks in parallel.
/// Spawns `concurrency` threads to process tasks.
/// each task would be pushed into the pool,and when a thread
/// is ready to process a task, it get a task from the waiting queue
/// according the following rules:
///
/// ## Choose one group
/// 1. Choose the groups with least running tasks.
/// 2. If more than one groups has the least running tasks,
///    choose the one whose task comes first(with the minum task's ID)
///
/// ## Chose a task in selected group.
/// For the tasks in the selected group, choose the first
/// one in the queue(which means comes first).
///
/// # Example
///
/// One group with 10 tasks, and 10 groups with 1 task each. Every
/// task cost the same time.
///
/// ```
/// # extern crate tikv;
/// use tikv::util::threadpool::{ThreadPool,ScheduleAlgorithm};
/// use std::thread::sleep;
/// use std::time::Duration;
/// use std::sync::mpsc::{Sender, Receiver, channel};
/// # fn main(){
/// let concurrency = 2;
/// let name = Some(String::from("sample"));
/// let mut task_pool = ThreadPool::new(name,concurrency,ScheduleAlgorithm::FairGroups{});
/// let (jtx, jrx): (Sender<u64>, Receiver<u64>) = channel();
/// let group_with_many_tasks = 1001 as u64;
/// // all tasks cost the same time.
/// let sleep_duration = Duration::from_millis(100);
///
/// // push tasks of group_with_many_tasks's job to make all thread in pool busy.
/// // in order to make sure the thread would be free in sequence,
/// // these tasks should run in sequence.
/// for _ in 0..concurrency {
///     task_pool.execute(group_with_many_tasks, move || {
///         sleep(sleep_duration);
///     });
///     // make sure the pre task is running now.
///     sleep(sleep_duration / 4);
/// }
///
/// // push 10 tasks of group_with_many_tasks's job into pool.
/// for _ in 0..10 {
///     let sender = jtx.clone();
///     task_pool.execute(group_with_many_tasks, move || {
///         sleep(sleep_duration);
///         sender.send(group_with_many_tasks).unwrap();
///     });
/// }
///
/// // push 1 task for each group_id in [0..10) into pool.
/// for group_id in 0..10 {
///     let sender = jtx.clone();
///     task_pool.execute(group_id, move || {
///         sleep(sleep_duration);
///         sender.send(group_id).unwrap();
///     });
/// }
/// // when first thread is free, since there would be another
/// // thread running group_with_many_tasks's task,and the first task which
/// // is not belong to group_with_many_tasks in the waitting queue would run first.
/// // when the second thread is free, since there is group_with_many_tasks's task
/// // is running, and the first task in the waiting queue is the
/// // group_with_many_tasks's task,so it would run. Similarly when the first is
/// // free again,it would run the task in front of the queue..
/// for id in 0..10 {
///     let first = jrx.recv().unwrap();
///     assert_eq!(first, id);
///     let second = jrx.recv().unwrap();
///     assert_eq!(second, group_with_many_tasks);
/// }
/// # }
/// ```
pub struct ThreadPool<T: Hash + Eq + Send + Clone + 'static> {
    meta: Arc<Mutex<ThreadPoolMeta<T>>>,
    job_sender: Sender<bool>,
    threads: Vec<JoinHandle<()>>,
}

impl<T: Hash + Eq + Send + Clone + 'static> ThreadPool<T> {
    pub fn new(name: Option<String>,
               num_threads: usize,
               algorithm: ScheduleAlgorithm)
               -> ThreadPool<T> {
        assert!(num_threads >= 1);
        let (tx, rx) = channel();
        let rx = Arc::new(Mutex::new(rx));
        let meta = Arc::new(Mutex::new(ThreadPoolMeta::new(algorithm)));

        let mut threads = Vec::with_capacity(num_threads);
        // Threadpool threads
        for _ in 0..num_threads {
            threads.push(new_thread(name.clone(), rx.clone(), meta.clone()));
        }

        ThreadPool {
            meta: meta.clone(),
            job_sender: tx,
            threads: threads,
        }
    }

    /// Executes the function `job` on a thread in the pool.
    pub fn execute<F>(&mut self, group_id: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        {
            let lock = self.meta.clone();
            let mut meta = lock.lock().unwrap();
            meta.push_task(group_id, job);
        }
        self.job_sender.send(true).unwrap();
    }

    pub fn get_tasks_num(&self) -> usize {
        let lock = self.meta.clone();
        let meta = lock.lock().unwrap();
        meta.get_tasks_num()
    }

    pub fn stop(&mut self) -> Result<(), String> {
        for _ in 0..self.threads.len() {
            if let Err(e) = self.job_sender.send(false) {
                return Err(format!("{:?}", e));
            }
        }
        while let Some(t) = self.threads.pop() {
            if let Err(e) = t.join() {
                return Err(format!("{:?}", e));
            }
        }
        Ok(())
    }
}


// each thread has a worker.
struct Worker<'a, T>
    where T: Hash + Eq + Send + Clone + 'static
{
    job_rever: &'a Arc<Mutex<Receiver<bool>>>,
    pool_meta: &'a Arc<Mutex<ThreadPoolMeta<T>>>,
}

impl<'a, T> Worker<'a, T>
    where T: Hash + Eq + Send + Clone + 'static
{
    fn new(receiver: &'a Arc<Mutex<Receiver<bool>>>,
           pool_meta: &'a Arc<Mutex<ThreadPoolMeta<T>>>)
           -> Worker<'a, T> {
        Worker {
            job_rever: receiver,
            pool_meta: pool_meta,
        }
    }

    // wait to receive info from job_receiver,
    // return false when get stop msg
    #[inline]
    fn wait(&self) -> bool {
        // try to receive notify
        let job_receiver = self.job_rever.lock().unwrap();
        job_receiver.recv().unwrap()
    }

    #[inline]
    fn get_next_task(&mut self) -> Option<(T, Task<T>)> {
        // try to get task
        let mut meta = self.pool_meta.lock().unwrap();
        meta.pop_next_task()
    }

    fn run(&mut self) {
        // start the worker.
        // loop break on receive stop message.
        while self.wait() {
            // handle task
            // since `tikv` would be down on any panic happens,
            // we needn't process panic case here.
            if let Some((task_key, task)) = self.get_next_task() {
                task.task.call_box(());
                let mut meta = self.pool_meta.lock().unwrap();
                meta.finished_task(task_key);
            }
        }
    }
}

fn new_thread<T>(name: Option<String>,
                 receiver: Arc<Mutex<Receiver<bool>>>,
                 tasks: Arc<Mutex<ThreadPoolMeta<T>>>)
                 -> JoinHandle<()>
    where T: Hash + Eq + Send + Clone + 'static
{
    let mut builder = Builder::new();
    if let Some(ref name) = name {
        builder = builder.name(name.clone());
    }

    builder.spawn(move || {
            let mut worker = Worker::new(&receiver, &tasks);
            worker.run();
        })
        .unwrap()
}

#[cfg(test)]
mod test {
    use super::{ThreadPool, ScheduleAlgorithm};
    use std::thread::sleep;
    use std::time::Duration;
    use std::sync::mpsc::channel;

    #[test]
    fn test_tasks_with_same_cost() {
        let name = Some(thd_name!("test_tasks_with_same_cost"));
        let concurrency = 2;
        let mut task_pool = ThreadPool::new(name, concurrency, ScheduleAlgorithm::FairGroups {});
        let (jtx, jrx) = channel();
        let group_with_many_tasks = 1001 as u64;
        // all tasks cost the same time.
        let sleep_duration = Duration::from_millis(50);
        let recv_timeout_duration = Duration::from_secs(2);

        // push tasks of group_with_many_tasks's job to make all thread in pool busy.
        // in order to make sure the thread would be free in sequence,
        // these tasks should run in sequence.
        for _ in 0..concurrency {
            task_pool.execute(group_with_many_tasks, move || {
                sleep(sleep_duration);
            });
            // make sure the pre task is running now.
            sleep(sleep_duration / 4);
        }

        // push 10 tasks of group_with_many_tasks's job into pool.
        for _ in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(group_with_many_tasks, move || {
                sleep(sleep_duration);
                sender.send(group_with_many_tasks).unwrap();
            });
        }

        // push 1 task for each group_id in [0..10) into pool.
        for group_id in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(group_id, move || {
                sleep(sleep_duration);
                sender.send(group_id).unwrap();
            });
        }
        // when first thread is free, since there would be another
        // thread running group_with_many_tasks's task,and the first task which
        // is not belong to group_with_many_tasks in the waitting queue would run first.
        // when the second thread is free, since there is group_with_many_tasks's task
        // is running, and the first task in the waiting queue is the
        // group_with_many_tasks's task,so it would run. Similarly when the first is
        // free again,it would run the task in front of the queue..
        for id in 0..10 {
            let first = jrx.recv_timeout(recv_timeout_duration).unwrap();
            assert_eq!(first, id);
            let second = jrx.recv_timeout(recv_timeout_duration).unwrap();
            assert_eq!(second, group_with_many_tasks);
        }
        task_pool.stop().unwrap();
    }


    #[test]
    fn test_tasks_with_different_cost() {
        let name = Some(thd_name!("test_tasks_with_different_cost"));
        let concurrency = 2;
        let mut task_pool = ThreadPool::new(name, concurrency, ScheduleAlgorithm::FairGroups {});
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

        // since there exist a long task of group_with_big_task is runnging,
        // the other thread would always run other group's tasks in sequence.
        for id in 0..10 {
            let group_id = jrx.recv_timeout(recv_timeout_duration).unwrap();
            assert_eq!(group_id, id);
        }

        for _ in 0..10 {
            let second = jrx.recv_timeout(recv_timeout_duration).unwrap();
            assert_eq!(second, group_with_big_task);
        }
        task_pool.stop().unwrap();
    }
}

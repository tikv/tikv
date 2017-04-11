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
use std::collections::VecDeque;
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

pub enum ScheduleAlgorithm {
    FIFO,
}

enum TasksQueueAlgorithm<T> {
    FIFO { queue: VecDeque<Task<T>> },
}

impl<T: Hash + Eq + Send + Clone + 'static> TasksQueueAlgorithm<T> {
    fn push(&mut self, task: Task<T>) {
        match *self {
            TasksQueueAlgorithm::FIFO { ref mut queue } => {
                queue.push_back(task);
            }
        }
    }

    fn pop(&mut self) -> Option<(T, Task<T>)> {
        match *self {
            TasksQueueAlgorithm::FIFO { ref mut queue } => {
                if let Some(task) = queue.pop_front() {
                    return Some((task.group_id(), task));
                }
                None
            }
        }
    }

    fn finished(&mut self, _: T) {}
}

struct ThreadPoolMeta<T> {
    next_task_id: u64,
    total_running_tasks: usize,
    total_waiting_tasks: usize,
    tasks: TasksQueueAlgorithm<T>,
    job_sender: Sender<bool>,
}

impl<T: Hash + Eq + Send + Clone + 'static> ThreadPoolMeta<T> {
    fn new(algorithm: ScheduleAlgorithm, job_sender: Sender<bool>) -> ThreadPoolMeta<T> {
        ThreadPoolMeta {
            next_task_id: 0,
            total_running_tasks: 0,
            total_waiting_tasks: 0,
            job_sender: job_sender,
            tasks: match algorithm {
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
        self.job_sender.send(true).unwrap();
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

    fn push_shutdown_tasks(&self, num: usize) -> Result<(), String> {
        for _ in 0..num {
            if let Err(e) = self.job_sender.send(false) {
                return Err(format!("{:?}", e));
            }
        }
        Ok(())
    }
}

/// A group task pool used to execute tasks in parallel.
/// Spawns `concurrency` threads to process tasks.
/// each task would be pushed into the pool,and when a thread
/// is ready to process a task, it get a task from the waiting queue
/// with one of the following algorithm
/// 1. FIFO
pub struct ThreadPool<T: Hash + Eq + Send + Clone + 'static> {
    meta: Arc<Mutex<ThreadPoolMeta<T>>>,
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
        let meta = Arc::new(Mutex::new(ThreadPoolMeta::new(algorithm, tx)));

        let mut threads = Vec::with_capacity(num_threads);
        // Threadpool threads
        for _ in 0..num_threads {
            threads.push(new_thread(name.clone(), rx.clone(), meta.clone()));
        }

        ThreadPool {
            meta: meta.clone(),
            threads: threads,
        }
    }

    /// Executes the function `job` on a thread in the pool.
    pub fn execute<F>(&mut self, group_id: T, job: F)
        where F: FnOnce() + Send + 'static
    {
        let lock = self.meta.clone();
        let mut meta = lock.lock().unwrap();
        meta.push_task(group_id, job);
    }

    pub fn get_tasks_num(&self) -> usize {
        let lock = self.meta.clone();
        let meta = lock.lock().unwrap();
        meta.get_tasks_num()
    }

    pub fn stop(&mut self) -> Result<(), String> {
        // notify all threads to stop.
        {
            let lock = self.meta.clone();
            let tasks = lock.lock().unwrap();
            tasks.push_shutdown_tasks(self.threads.len()).unwrap();
        }
        // wait until all threads stopped.
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
    fn test_fifo_tasks_with_same_cost() {
        let name = Some(thd_name!("test_tasks_with_same_cost"));
        let concurrency = 2;
        let mut task_pool = ThreadPool::new(name, concurrency, ScheduleAlgorithm::FIFO {});
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

        // push 1 task for each group_id in [0..10) into pool.
        for group_id in 0..10 {
            let sender = jtx.clone();
            task_pool.execute(group_id, move || {
                sleep(sleep_duration);
                sender.send(group_id).unwrap();
            });
        }
        for id in 0..10 {
            let first = jrx.recv_timeout(recv_timeout_duration).unwrap();
            assert_eq!(first, id);
        }
        task_pool.stop().unwrap();
    }
}

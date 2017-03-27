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
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread::Builder;
use std::result::Result;
use super::HashMap;

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

type Thunk<'a> = Box<FnBox + Send + 'a>;

struct TaskMeta<'a> {
    id: u64,
    group_id: u64,
    task: Thunk<'a>,
}

impl<'a> TaskMeta<'a> {
    fn new<F>(id: u64, group_id: u64, job: F) -> TaskMeta<'static>
        where F: FnOnce() + Send + 'static
    {
        TaskMeta {
            id: id,
            group_id: group_id,
            task: Box::new(job),
        }
    }
}

struct GroupTaskPoolMeta {
    total_tasks_num: u64,
    concurrency: usize,
    running_thread_num: usize,
    running_tasks_num: usize,
    waiting_tasks_num: usize,
    // group_id=>count
    running_tasks: HashMap<u64, usize>,
    // group_id=> tasks array
    waiting_tasks: HashMap<u64, Vec<TaskMeta<'static>>>,
    stop_finished_sender: Sender<bool>, // send stop when running_thread_num turns 0
}

impl GroupTaskPoolMeta {
    fn new(concurrency: usize, stop_finished_sender: Sender<bool>) -> GroupTaskPoolMeta {
        GroupTaskPoolMeta {
            total_tasks_num: 0,
            running_tasks_num: 0,
            running_thread_num: 0,
            waiting_tasks_num: 0,
            running_tasks: HashMap::default(),
            waiting_tasks: HashMap::default(),
            stop_finished_sender: stop_finished_sender,
            concurrency: concurrency,
        }
    }

    fn push_task<F>(&mut self, group_id: u64, job: F)
        where F: FnOnce() + Send + 'static
    {
        let task = TaskMeta::new(self.total_tasks_num, group_id, job);
        self.waiting_tasks_num += 1;
        self.total_tasks_num += 1;
        let group_id = task.group_id;
        let mut group_tasks = self.waiting_tasks
            .entry(group_id)
            .or_insert_with(Vec::new);
        group_tasks.push(task);
    }

    fn total_task_num(&self) -> usize {
        self.waiting_tasks_num + self.running_tasks_num
    }

    fn new_thread_started(&mut self) -> Result<(), &str> {
        if self.running_thread_num >= self.concurrency {
            return Err("too many thread");
        }
        self.running_thread_num += 1;
        Ok(())
    }

    fn one_thread_stopped(&mut self) {
        self.running_thread_num -= 1;
        if self.running_thread_num == 0 {
            self.stop_finished_sender.send(true).unwrap();
        }
    }


    fn finished_task(&mut self, group_id: u64) {
        self.running_tasks_num -= 1;
        // sub 1 in running tasks for group_id.
        if self.running_tasks[&group_id] <= 1 {
            self.running_tasks.remove(&group_id);
        } else {
            let mut count = self.running_tasks.get_mut(&group_id).unwrap();
            *count -= 1;
        }
    }

    fn get_next_group(&self) -> Option<u64> {
        let mut next_group = None; //(group_id,count,task_id)
        for (group_id, tasks) in &self.waiting_tasks {
            if tasks.is_empty() {
                continue;
            }
            let front_task_id = tasks[0].id;
            let mut count = 0;
            if self.running_tasks.contains_key(group_id) {
                count = self.running_tasks[group_id];
            };
            if next_group.is_none() {
                next_group = Some((group_id, count, front_task_id));
                continue;
            }
            let (_, pre_count, pre_task_id) = next_group.unwrap();
            if pre_count > count {
                next_group = Some((group_id, count, front_task_id));
                continue;
            }

            if pre_count == count && pre_task_id < front_task_id {
                next_group = Some((group_id, count, front_task_id));
            }

        }

        if let Some((group_id, _, _)) = next_group {
            return Some(*group_id);
        }

        // no tasks in waiting.
        None
    }

    fn pop_next_task(&mut self) -> Option<(TaskMeta<'static>)> {
        if let Some(group_id) = self.get_next_group() {
            // running tasks for group add 1.
            self.running_tasks.entry(group_id).or_insert(0 as usize);
            let mut running_tasks = self.running_tasks.get_mut(&group_id).unwrap();
            *running_tasks += 1;
            // get front waiting task from group.
            let (empty_group_wtasks, task) = {
                let mut waiting_tasks = self.waiting_tasks.get_mut(&group_id).unwrap();
                let task_meta = waiting_tasks.swap_remove(0);
                (waiting_tasks.is_empty(), task_meta)
            };
            // if waiting tasks for group is empty,remove from waiting_tasks.
            if empty_group_wtasks {
                self.waiting_tasks.remove(&group_id);
            }

            self.waiting_tasks_num -= 1;
            self.running_tasks_num += 1;
            return Some(task);
        }
        None
    }
}

pub struct GroupTaskPool {
    tasks: Arc<Mutex<GroupTaskPoolMeta>>,
    job_sender: Sender<bool>,
    stop_finished_receiver: Receiver<bool>,
    concurrency: usize,
}

impl GroupTaskPool {
    pub fn new(num_threads: usize) -> GroupTaskPool {
        assert!(num_threads >= 1);
        let name = Some(thd_name!("endpoint-pool"));
        let (jtx, jrx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
        let jrx = Arc::new(Mutex::new(jrx));
        let (stx, srx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
        let tasks = Arc::new(Mutex::new(GroupTaskPoolMeta::new(num_threads, stx)));

        // Threadpool threads
        for _ in 0..num_threads {
            spawn_in_pool(name.clone(), jrx.clone(), tasks.clone());
        }

        GroupTaskPool {
            tasks: tasks.clone(),
            job_sender: jtx,
            stop_finished_receiver: srx,
            concurrency: num_threads,
        }
    }

    /// Executes the function `job` on a thread in the pool.
    pub fn execute<F>(&mut self, group_id: u64, job: F)
        where F: FnOnce() + Send + 'static
    {
        {
            let lock = self.tasks.clone();
            let mut meta = lock.lock().unwrap();
            meta.push_task(group_id, job);
        }
        self.job_sender.send(true).unwrap();
    }

    fn stop(&mut self) {
        for _ in 0..self.concurrency {
            self.job_sender.send(false).unwrap();
        }
        self.stop_finished_receiver.recv().unwrap();
    }

    pub fn get_tasks_num(&self) -> usize {
        let lock = self.tasks.clone();
        let meta = lock.lock().unwrap();
        meta.total_task_num()
    }
}

impl Drop for GroupTaskPool {
    fn drop(&mut self) {
        self.stop();
    }
}

fn spawn_in_pool(name: Option<String>,
                 receiver: Arc<Mutex<Receiver<bool>>>,
                 tasks: Arc<Mutex<GroupTaskPoolMeta>>) {
    let mut builder = Builder::new();
    if let Some(ref name) = name {
        builder = builder.name(name.clone());
    }

    builder.spawn(move || {
            // start new thread.
            {
                let mut meta = tasks.lock().unwrap();
                meta.new_thread_started().unwrap();
            }
            loop {
                // try to receive notify
                {
                    let job_receiver = receiver.lock().unwrap();
                    let job = job_receiver.recv().unwrap();
                    if !job {
                        break;
                    }
                }

                // try to get task
                let task = {
                    let mut meta = tasks.lock().unwrap();
                    meta.pop_next_task()
                };

                // handle task
                if let Some(task_meta) = task {
                    task_meta.task.call_box();
                    let mut meta = tasks.lock().unwrap();
                    meta.finished_task(task_meta.group_id);
                }
            }

            // clear current thread in meta pool
            {
                let mut meta = tasks.lock().unwrap();
                meta.one_thread_stopped();
            }
        })
        .unwrap();
}

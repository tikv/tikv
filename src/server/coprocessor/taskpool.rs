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
use tipb::select::SelectRequest;
use util::HashMap;
use super::Result;
use super::metrics::*;
use super::endpoint::*;
use storage::Snapshot;

struct TaskMeta {
    txn_id: u64,
    snap: Box<Snapshot>,
    task: RequestTask,
    sel: Result<SelectRequest>,
}

impl TaskMeta {
    fn new(txn: u64,
           snap: Box<Snapshot>,
           task: RequestTask,
           sel: Result<SelectRequest>)
           -> TaskMeta {
        TaskMeta {
            txn_id: txn,
            snap: snap,
            task: task,
            sel: sel,
        }
    }
}

struct TaskPoolMeta {
    concurrency: usize,
    running_thread_num: usize,
    running_tasks_num: usize,
    waiting_tasks_num: usize,
    // txn_id=>count
    running_tasks: HashMap<u64, usize>,
    // txn_id=> tasks array
    waiting_tasks: HashMap<u64, Vec<TaskMeta>>,
    stop_finished_sender: Sender<bool>, // send stop when running_thread_num turns 0
}

impl TaskPoolMeta {
    fn new(concurrency: usize, stop_finished_sender: Sender<bool>) -> TaskPoolMeta {
        TaskPoolMeta {
            running_tasks_num: 0,
            running_thread_num: 0,
            waiting_tasks_num: 0,
            running_tasks: HashMap::default(),
            waiting_tasks: HashMap::default(),
            stop_finished_sender: stop_finished_sender,
            concurrency: concurrency,
        }
    }

    fn push_tasks(&mut self, snap: Box<Snapshot>, tasks: Vec<RequestTask>) -> usize {
        let tasks_len = tasks.len();
        self.waiting_tasks_num += tasks_len;
        for mut task in tasks {
            let sel = task.parse_sel();
            let txn = task.start_ts.unwrap_or_default();
            let mut txn_tasks = self.waiting_tasks
                .entry(txn)
                .or_insert_with(Vec::new);
            let task_meta = TaskMeta::new(txn, snap.clone(), task, sel);
            txn_tasks.push(task_meta);
        }
        COPR_PENDING_REQS.with_label_values(&["select"]).add(tasks_len as f64);
        tasks_len
    }

    fn total_task_num(&self) -> usize {
        self.waiting_tasks_num + self.running_tasks_num
    }

    fn new_thread_started(&mut self) -> Result<()> {
        if self.running_thread_num >= self.concurrency {
            return Err(box_err!("too many thread"));
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


    fn finished_task(&mut self, txn: u64) {
        self.running_tasks_num -= 1;
        // sub 1 in running tasks for txn.
        if self.running_tasks[&txn] <= 1 {
            self.running_tasks.remove(&txn);
        } else {
            let mut count = self.running_tasks.get_mut(&txn).unwrap();
            *count -= 1;
        }
        COPR_PENDING_REQS.with_label_values(&["select"]).sub(1.0);
    }

    fn get_next_txn(&self) -> Option<u64> {
        let mut next_txn = None;
        for txn in self.waiting_tasks.keys() {
            let count = match self.running_tasks.get(txn) {
                None => 0 as usize,
                Some(count) => *count,
            };
            // if no running task for txn, be the next txn.
            if count == 0 {
                return Some(*txn);
            }

            // else prefer txn with min running tasks.
            if next_txn.is_none() {
                next_txn = Some((*txn, count));
                continue;
            }
            let (_, pre_count) = next_txn.unwrap();
            if pre_count > count {
                next_txn = Some((*txn, count));
            }
        }

        if let Some((txn, _)) = next_txn {
            return Some(txn);
        }

        // no tasks in waiting.
        None
    }

    fn pop_next_task(&mut self) -> Option<(TaskMeta)> {
        if let Some(txn) = self.get_next_txn() {
            // running tasks for txn add 1.
            self.running_tasks.entry(txn).or_insert(0 as usize);
            let mut running_tasks = self.running_tasks.get_mut(&txn).unwrap();
            *running_tasks += 1;
            // pop waiting task from txn.
            let (empty_txn_waiting_tasks, task) = {
                let mut waiting_tasks = self.waiting_tasks.get_mut(&txn).unwrap();
                let task_meta = waiting_tasks.pop().unwrap();
                (waiting_tasks.is_empty(), task_meta)
            };
            // if waiting tasks for txn is empty,remove from waiting_tasks.
            if empty_txn_waiting_tasks {
                self.waiting_tasks.remove(&txn);
            }

            self.waiting_tasks_num -= 1;
            self.running_tasks_num += 1;
            return Some(task);
        }
        None
    }
}

pub struct TaskPool {
    tasks: Arc<Mutex<TaskPoolMeta>>,
    job_sender: Sender<bool>,
    stop_finished_receiver: Receiver<bool>,
    concurrency: usize,
}

impl TaskPool {
    pub fn new(num_threads: usize) -> TaskPool {
        assert!(num_threads >= 1);
        let name = Some(thd_name!("endpoint-pool"));
        let (jtx, jrx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
        let jrx = Arc::new(Mutex::new(jrx));
        let (stx, srx): (Sender<bool>, Receiver<bool>) = mpsc::channel();
        let tasks = Arc::new(Mutex::new(TaskPoolMeta::new(num_threads, stx)));

        // Threadpool threads
        for _ in 0..num_threads {
            spawn_in_pool(name.clone(), jrx.clone(), tasks.clone());
        }

        TaskPool {
            tasks: tasks.clone(),
            job_sender: jtx,
            stop_finished_receiver: srx,
            concurrency: num_threads,
        }
    }

    pub fn handle_batch(&mut self, snap: Box<Snapshot>, tasks: Vec<RequestTask>) {
        // check length
        let count = {
            let lock = self.tasks.clone();
            let mut meta = lock.lock().unwrap();
            meta.push_tasks(snap, tasks)
        };

        for _ in 0..count {
            self.job_sender.send(true).unwrap();
        }
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

impl Drop for TaskPool {
    fn drop(&mut self) {
        self.stop();
    }
}

fn spawn_in_pool(name: Option<String>,
                 receiver: Arc<Mutex<Receiver<bool>>>,
                 tasks: Arc<Mutex<TaskPoolMeta>>) {
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
                    let end_point = TiDbEndPoint::new(task_meta.snap);
                    end_point.handle_request(task_meta.task, task_meta.sel);
                    let mut meta = tasks.lock().unwrap();
                    meta.finished_task(task_meta.txn_id);
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
